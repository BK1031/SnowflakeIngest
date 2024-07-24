def print_results(latencies):
    if not latencies:
        print("No results found.")
        return

    avg_latency = sum(latencies) / len(latencies)
    min_latency = min(latencies)
    max_latency = max(latencies)

    p95 = sorted(latencies)[int(len(latencies) * 0.95)]
    p99 = sorted(latencies)[int(len(latencies) * 0.99)]
    
    print()
    print("+-----------------------+---------------+")
    print("| Metric                | Latency (ms)  |")
    print("+-----------------------+---------------+")
    print(f"| Average latency       | {float(avg_latency):13.2f} |")
    print(f"| Minimum latency       | {float(min_latency):13.2f} |")
    print(f"| Maximum latency       | {float(max_latency):13.2f} |")
    print(f"| 95th percentile       | {float(p95):13.2f} |")
    print(f"| 99th percentile       | {float(p99):13.2f} |")
    print("+-----------------------+---------------+")
    print()