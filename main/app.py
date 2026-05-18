from fastapi import FastAPI
import hashlib
import os
import json
import threading
import time
import requests
import boto3

app = FastAPI()

# ---------------- Configuration ----------------
NODE_LIST = os.getenv("NODE_LIST", "")
MY_IP = os.getenv("MY_IP", "")
SNS_TOPIC_ARN = os.getenv("SNS_TOPIC_ARN")
SQS_QUEUE_URL = os.getenv("SQS_QUEUE_URL")

NODES = sorted(NODE_LIST.split(",")) if NODE_LIST else []

REPLICATION_FACTOR = 3
R = 2  # read quorum

DATA_FILE = "/app/data/store.json"
LOG_FILE = "/app/data/log.txt"
HINT_FILE = "/app/data/hints.json"

store = {}
hints = {}

# ---------------- Metrics ----------------
metrics = {
    "writes": 0,
    "reads": 0,
    "write_latency_ms": [],
    "read_latency_ms": [],
    "replication_delay_ms": []
}

# ---------------- Utility ----------------
def hash_key(key):
    return int(hashlib.sha1(key.encode()).hexdigest(), 16)

def get_replicas(key):
    if not NODES:
        return []

    h = hash_key(key)
    idx = h % len(NODES)

    return [NODES[(idx + i) % len(NODES)] for i in range(REPLICATION_FACTOR)]

# ---------------- Persistence ----------------
def save_snapshot():
    with open(DATA_FILE, "w") as f:
        json.dump(store, f)

def load_snapshot():
    global store
    if os.path.exists(DATA_FILE):
        with open(DATA_FILE) as f:
            store = json.load(f)

def append_log(key, value, ts):
    with open(LOG_FILE, "a") as f:
        f.write(json.dumps({
            "key": key,
            "value": value,
            "ts": ts
        }) + "\n")

def load_from_log():
    global store
    if os.path.exists(LOG_FILE):
        with open(LOG_FILE) as f:
            for line in f:
                try:
                    entry = json.loads(line.strip())
                    k = entry["key"]
                    if k not in store or store[k]["ts"] < entry["ts"]:
                        store[k] = entry
                except:
                    continue

# ---------------- Hints ----------------
def save_hints():
    with open(HINT_FILE, "w") as f:
        json.dump(hints, f)

def load_hints():
    global hints
    if os.path.exists(HINT_FILE):
        with open(HINT_FILE) as f:
            hints = json.load(f)

# ---------------- AWS Clients ----------------
sns = boto3.client("sns")
sqs = boto3.client("sqs")

# ---------------- API ----------------

@app.get("/put")
def put(key: str, value: str):
    start = time.time()

    replicas = get_replicas(key)
    if not replicas:
        return {"error": "no nodes configured"}

    # Forward request to primary replica
    if MY_IP != replicas[0]:
        try:
            return requests.get(
                f"http://{replicas[0]}:3030/put",
                params={"key": key, "value": value},
                timeout=1
            ).json()
        except:
            return {"error": "primary unreachable"}

    ts = int(time.time() * 1000)

    # Store locally
    store[key] = {"value": value, "ts": ts}
    append_log(key, value, ts)
    save_snapshot()

    # Publish update to SNS
    message = {
        "key": key,
        "value": value,
        "ts": ts,
        "replicas": replicas
    }

    sns.publish(
        TopicArn=SNS_TOPIC_ARN,
        Message=json.dumps(message)
    )

    latency = (time.time() - start) * 1000
    metrics["writes"] += 1
    metrics["write_latency_ms"].append(latency)

    return {
        "status": "write_sent",
        "latency_ms": latency,
        "replicas": replicas
    }

@app.get("/get")
def get(key: str):
    start = time.time()

    replicas = get_replicas(key)
    responses = []

    # Query replicas
    for node in replicas:
        try:
            r = requests.get(
                f"http://{node}:3030/local_get",
                params={"key": key},
                timeout=0.5
            )
            if r.status_code == 200:
                data = r.json()
                if "value" in data:
                    responses.append(data)
        except:
            continue

    if not responses:
        return {"error": "not found"}

    # Select latest version
    latest = max(responses, key=lambda x: x["ts"])

    # Read repair
    for node in replicas:
        try:
            requests.post(
                f"http://{node}:3030/internal_put",
                json=latest,
                timeout=0.5
            )
        except:
            pass

    latency = (time.time() - start) * 1000
    metrics["reads"] += 1
    metrics["read_latency_ms"].append(latency)

    return {
        "value": latest["value"],
        "ts": latest["ts"],
        "latency_ms": latency
    }

@app.get("/local_get")
def local_get(key: str):
    if key in store:
        return store[key]
    return {"error": "not found"}

@app.post("/internal_put")
def internal_put(data: dict):
    key = data["key"]
    value = data["value"]
    ts = data["ts"]

    delay = int(time.time() * 1000) - ts
    metrics["replication_delay_ms"].append(delay)

    if key not in store or store[key]["ts"] < ts:
        store[key] = {"value": value, "ts": ts}
        append_log(key, value, ts)
        save_snapshot()

    return {"status": "ok"}

@app.get("/metrics")
def get_metrics():
    return {
        "writes": metrics["writes"],
        "reads": metrics["reads"],
        "avg_write_latency_ms": avg(metrics["write_latency_ms"]),
        "avg_read_latency_ms": avg(metrics["read_latency_ms"]),
        "avg_replication_delay_ms": avg(metrics["replication_delay_ms"])
    }

def avg(arr):
    return sum(arr) / len(arr) if arr else 0

@app.get("/health")
def health():
    return {"status": "ok"}

# ---------------- Background Workers ----------------
def sqs_worker():
    while True:
        try:
            resp = sqs.receive_message(
                QueueUrl=SQS_QUEUE_URL,
                MaxNumberOfMessages=10,
                WaitTimeSeconds=10
            )

            for msg in resp.get("Messages", []):
                body = json.loads(msg["Body"])
                message = json.loads(body["Message"])

                replicas = message["replicas"]

                if MY_IP in replicas:
                    internal_put(message)
                else:
                    for node in replicas:
                        if node != MY_IP:
                            hints.setdefault(node, []).append(message)
                    save_hints()

                sqs.delete_message(
                    QueueUrl=SQS_QUEUE_URL,
                    ReceiptHandle=msg["ReceiptHandle"]
                )

        except Exception:
            time.sleep(2)

def hinted_handoff_worker():
    while True:
        for node in list(hints.keys()):
            remaining = []
            for item in hints[node]:
                try:
                    requests.post(
                        f"http://{node}:3030/internal_put",
                        json=item,
                        timeout=0.5
                    )
                except:
                    remaining.append(item)

            if remaining:
                hints[node] = remaining
            else:
                del hints[node]

        save_hints()
        time.sleep(5)

def anti_entropy_worker():
    while True:
        for node in NODES:
            if node == MY_IP:
                continue
            try:
                remote = requests.get(f"http://{node}:3030/dump", timeout=1).json()
                for k, v in remote.items():
                    if k not in store or store[k]["ts"] < v["ts"]:
                        store[k] = v
                        append_log(k, v["value"], v["ts"])
                save_snapshot()
            except:
                continue

        time.sleep(10)

# ---------------- Startup ----------------
@app.on_event("startup")
def startup():
    load_snapshot()
    load_from_log()
    load_hints()

    threading.Thread(target=sqs_worker, daemon=True).start()
    threading.Thread(target=hinted_handoff_worker, daemon=True).start()
    threading.Thread(target=anti_entropy_worker, daemon=True).start()