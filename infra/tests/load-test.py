import asyncio
import aiohttp
import random
import string
import time

TOTAL_KEYS = 100_000
VALUE_SIZE = 102400   # 100 KB
CONCURRENT_REQUESTS = 150

# ---------------- LOAD IPS ----------------

NODES = []

with open("../public_ips.txt") as f:

    for line in f:

        ip = line.strip()

        if ip:
            NODES.append(f"http://{ip}:3030")

print("Loaded nodes:")
for n in NODES:
    print(n)

# ---------------- GENERATE VALUE ----------------

value = ''.join(
    random.choices(
        string.ascii_letters + string.digits,
        k=VALUE_SIZE
    )
)

counter = 0
counter_lock = asyncio.Lock()

success_count = 0
failure_count = 0

success_lock = asyncio.Lock()

# ---------------- PUT REQUEST ----------------

async def put_key(session, idx):

    global success_count
    global failure_count

    key = f"key_{idx}"

    node = random.choice(NODES)

    for attempt in range(3):

        try:

            async with session.get(
                f"{node}/put",
                params={
                    "key": key,
                    "value": value
                }
            ) as resp:

                if resp.status == 200:

                    await resp.text()

                    async with success_lock:
                        success_count += 1

                    return

        except Exception as e:

            if attempt == 2:

                async with success_lock:
                    failure_count += 1

                print(f"ERROR ({node}):", e)

            await asyncio.sleep(1)

# ---------------- WORKER ----------------

async def worker(session):

    global counter

    while True:

        async with counter_lock:

            if counter >= TOTAL_KEYS:
                return

            idx = counter
            counter += 1

        await put_key(session, idx)

# ---------------- PROGRESS ----------------

async def progress_monitor(start_time):

    while True:

        await asyncio.sleep(10)

        elapsed = time.time() - start_time

        async with success_lock:

            total_done = success_count + failure_count

            rate = total_done / elapsed if elapsed > 0 else 0

            print("\n========== PROGRESS ==========")
            print(f"Completed: {total_done}/{TOTAL_KEYS}")
            print(f"Success: {success_count}")
            print(f"Failures: {failure_count}")
            print(f"Writes/sec: {rate:.2f}")

        if total_done >= TOTAL_KEYS:
            return

# ---------------- MAIN ----------------

async def main():

    start = time.time()

    connector = aiohttp.TCPConnector(
        limit=500,
        limit_per_host=100
    )

    timeout = aiohttp.ClientTimeout(
        total=300
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

    print("\n========== FINAL RESULTS ==========")
    print(f"Total keys attempted: {TOTAL_KEYS}")
    print(f"Successful writes: {success_count}")
    print(f"Failed writes: {failure_count}")
    print(f"Value size: {VALUE_SIZE}")
    print(f"Approx data: {(TOTAL_KEYS * VALUE_SIZE)/(1024**3):.2f} GB")
    print(f"Duration: {duration:.2f} sec")
    print(f"Writes/sec: {success_count/duration:.2f}")

asyncio.run(main())