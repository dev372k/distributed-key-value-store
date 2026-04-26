import json
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

# -------- LOAD DATA --------
file_path = "results.json"

latencies = []
timestamps = []
failures = []
req_times = []

with open(file_path) as f:
    for line in f:
        try:
            obj = json.loads(line)

            if obj.get("metric") == "http_req_duration":
                latencies.append(obj["data"]["value"])
                timestamps.append(obj["data"]["time"])

            if obj.get("metric") == "http_req_failed":
                failures.append(obj["data"]["value"])

            if obj.get("metric") == "http_reqs":
                req_times.append(obj["data"]["time"])

        except:
            continue

# Convert to DataFrame
df = pd.DataFrame({
    "time": pd.to_datetime(timestamps),
    "latency": latencies
})

# -------- SCALE TIME --------
df["time_sec"] = (df["time"] - df["time"].min()).dt.total_seconds()

# -------- 1. LATENCY DISTRIBUTION --------
plt.figure()
plt.hist(df["latency"], bins=50)
plt.xlabel("Latency (ms)")
plt.ylabel("Frequency")
plt.title("Latency Distribution")
plt.savefig("visualizations/latency_distribution.png")
plt.close()

# -------- 2. LATENCY PERCENTILES --------
percentiles = [50, 90, 95, 99]
values = [np.percentile(df["latency"], p) for p in percentiles]

plt.figure()
plt.bar([str(p) for p in percentiles], values)
plt.xlabel("Percentile")
plt.ylabel("Latency (ms)")
plt.title("Latency Percentiles")
plt.savefig("visualizations/latency_percentiles.png")
plt.close()

# -------- 3. LATENCY OVER TIME (SMOOTHED) --------
df["latency_smooth"] = df["latency"].rolling(window=20).mean()

plt.figure()
plt.plot(df["time_sec"], df["latency"], alpha=0.3)
plt.plot(df["time_sec"], df["latency_smooth"])
plt.xlabel("Time (seconds)")
plt.ylabel("Latency (ms)")
plt.title("Latency Over Time (Smoothed)")
plt.savefig("visualizations/latency_over_time.png")
plt.close()

# -------- 4. FAILURE RATE --------
failure_rate = sum(failures) / len(failures) if failures else 0

plt.figure()
plt.bar(["Failure Rate"], [failure_rate * 100])
plt.ylabel("Percentage (%)")
plt.title("Request Failure Rate")
plt.savefig("visualizations/failure_rate.png")
plt.close()

# -------- 5. THROUGHPUT (REQUESTS PER SECOND) --------
req_df = pd.DataFrame({"time": pd.to_datetime(req_times)})
req_df["time_sec"] = (req_df["time"] - req_df["time"].min()).dt.total_seconds()

throughput = req_df.groupby(req_df["time_sec"].astype(int)).size()

plt.figure()
plt.plot(throughput.index, throughput.values)
plt.xlabel("Time (seconds)")
plt.ylabel("Requests per Second")
plt.title("Throughput (RPS)")
plt.savefig("visualizations/throughput.png")
plt.close()

# -------- PRINT SUMMARY --------
print("Graphs generated successfully:")
print("latency_distribution.png")
print("latency_percentiles.png")
print("latency_over_time.png")
print("failure_rate.png")
print("throughput.png")