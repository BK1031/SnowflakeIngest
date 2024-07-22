import snowflake.connector

def print_results(snow, n):
    cursor = snow.cursor()
    query = """
    SELECT 
        TIMEDIFF(millisecond, SENT_AT, CREATED_AT) as latency_ms,
        SENT_AT,
        CREATED_AT
    FROM LIFT_TICKETS
    ORDER BY CREATED_AT DESC
    LIMIT ?
    """
    cursor.execute(query, (n,))
    results = cursor.fetchall()
    cursor.close()

    if not results:
        print("No results found.")
        return

    latencies = [row[0] for row in results if row[0] is not None]

    if not latencies:
        print("No valid latencies found.")
        return

    avg_latency = sum(latencies) / len(latencies)
    min_latency = min(latencies)
    max_latency = max(latencies)

    p95 = sorted(latencies)[int(len(latencies) * 0.95)]
    p99 = sorted(latencies)[int(len(latencies) * 0.99)]
    
    print("+-----------------------+---------------+")
    print("| Metric                | Latency (ms)  |")
    print("+-----------------------+---------------+")
    print(f"| Average latency       | {float(avg_latency):13.2f} |")
    print(f"| Minimum latency       | {float(min_latency):13.2f} |")
    print(f"| Maximum latency       | {float(max_latency):13.2f} |")
    print(f"| 95th percentile       | {float(p95):13.2f} |")
    print(f"| 99th percentile       | {float(p99):13.2f} |")
    print("+-----------------------+---------------+")