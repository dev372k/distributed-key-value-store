import asyncio
import aiohttp
import random
import string
import time

# =========================================
# CONFIG
# =========================================

TOTAL_REQUESTS = 100_000_0 ## 1 million request

CONCURRENT_REQUESTS = 200

VALUE_SIZE = 1024  # 1 KB

# -----------------------------------------
# WORKLOAD TYPES
# A = 50/50
# B = 95/5
# C = 100/0
# W = 20/80
# -----------------------------------------

WORKLOAD = "W"

# =========================================
# LOAD NODES
# =========================================

NODES = []

with open("../public_ips.txt") as f:

    for line in f:

        ip = line.strip()

        if ip:
            NODES.append(f"http://{ip}:3030")

# =========================================
# YCSB-STYLE WORKLOADS
# =========================================

WORKLOADS = {

    "A": (50, 50),
    "B": (95, 5),
    "C": (100, 0),
    "W": (20, 80),
}

READ_PERCENT, WRITE_PERCENT = WORKLOADS[WORKLOAD]

# =========================================
# RANDOM VALUE
# =========================================

value = ''.join(
    random.choices(
        string.ascii_letters + string.digits,
        k=VALUE_SIZE
    )
)

# =========================================
# METRICS
# =========================================

success = 0
failures = 0

latencies = []

counter = 0

counter_lock = asyncio.Lock()

metrics_lock = asyncio.Lock()

# =========================================
# PUT REQUEST
# =========================================

async def put_request(session, key):

    node = random.choice(NODES)

    start = time.time()

    try:

        async with session.get(
            f"{node}/put",
            params={
                "key": key,
                "value": value
            }
        ) as resp:

            await resp.text()

            latency = (time.time() - start) * 1000

            return True, latency

    except:
        return False, 0

# =========================================
# GET REQUEST
# =========================================

async def get_request(session, key):

    node = random.choice(NODES)

    start = time.time()

    try:

        async with session.get(
            f"{node}/get",
            params={
                "key": key
            }
        ) as resp:

            await resp.text()

            latency = (time.time() - start) * 1000

            return True, latency

    except:
        return False, 0

# =========================================
# WORKER
# =========================================

async def worker(session):

    global counter
    global success
    global failures

    while True:

        async with counter_lock:

            if counter >= TOTAL_REQUESTS:
                return

            idx = counter
            counter += 1

        key = f"key_{random.randint(1, TOTAL_REQUESTS)}"

        operation = random.choices(
            ["GET", "PUT"],
            weights=[READ_PERCENT, WRITE_PERCENT]
        )[0]

        if operation == "GET":

            ok, latency = await get_request(session, key)

        else:

            ok, latency = await put_request(session, key)

        async with metrics_lock:

            if ok:

                success += 1
                latencies.append(latency)

            else:

                failures += 1

# =========================================
# PROGRESS MONITOR
# =========================================

async def progress_monitor(start_time):

    while True:

        await asyncio.sleep(10)

        elapsed = time.time() - start_time

        async with metrics_lock:

            completed = success + failures

            throughput = (
                completed / elapsed
                if elapsed > 0 else 0
            )

            print("\n========== PROGRESS ==========")
            print(f"Completed: {completed}/{TOTAL_REQUESTS}")
            print(f"Success: {success}")
            print(f"Failures: {failures}")
            print(f"Throughput: {throughput:.2f} ops/sec")

        if completed >= TOTAL_REQUESTS:
            return

# =========================================
# MAIN
# =========================================

async def main():

    start = time.time()

    connector = aiohttp.TCPConnector(
        limit=2000,
        limit_per_host=300
    )

    timeout = aiohttp.ClientTimeout(
        total=120
    )

    async with aiohttp.ClientSession(
        connector=connector,
        timeout=timeout
    ) as session:

        workers = [
            worker(session)
            for _ in range(CONCURRENT_REQUESTS)
        ]

        monitor = asyncio.create_task(
            progress_monitor(start)
        )

        await asyncio.gather(*workers)

        await monitor

    end = time.time()

    duration = end - start

    throughput = success / duration if duration > 0 else 0

    # =====================================
    # LATENCY STATS
    # =====================================

    latencies.sort()

    median_latency = (
        latencies[len(latencies)//2]
        if latencies else 0
    )

    p95_latency = (
        latencies[int(len(latencies)*0.95)]
        if latencies else 0
    )

    p99_latency = (
        latencies[int(len(latencies)*0.99)]
        if latencies else 0
    )

    min_latency = (
        min(latencies)
        if latencies else 0
    )

    max_latency = (
        max(latencies)
        if latencies else 0
    )

    # =====================================
    # FINAL RESULTS
    # =====================================

    print("\n========== RESULTS ==========")

    print(f"Workload: {WORKLOAD}")
    print(f"Read %: {READ_PERCENT}")
    print(f"Write %: {WRITE_PERCENT}")

    print(f"\nTotal Requests: {TOTAL_REQUESTS}")

    print(f"Successful: {success}")
    print(f"Failures: {failures}")

    print(f"\nDuration: {duration:.2f} sec")

    print(f"Throughput: {throughput:.2f} ops/sec")

    print("\n========== LATENCY ==========")

    print(f"Median Latency (P50): {median_latency:.2f} ms")

    print(f"P95 Latency: {p95_latency:.2f} ms")

    print(f"P99 Latency: {p99_latency:.2f} ms")

    print(f"Min Latency: {min_latency:.2f} ms")

    print(f"Max Latency: {max_latency:.2f} ms")

# =========================================
# RUN
# =========================================

asyncio.run(main())