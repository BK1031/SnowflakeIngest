from datetime import datetime
import json
import os
import logging
import sys
from init import connect_snow

from dotenv import load_dotenv
from results import print_results

load_dotenv()
logging.basicConfig(level=logging.WARN)

query_num = 1
num_tests = 100
latencies = []

def get_query(query_num):
    if query_num == 1:
        return """
WITH hourly_precip AS (
  SELECT 
    DATE_TRUNC('hour', StartTime) AS hour,
    Severity,
    AVG(Precipitation) AS avg_precip,
    COUNT(*) AS event_count
  FROM weather
  WHERE Type = 'Snow'
  GROUP BY 1, 2
)
SELECT 
  Severity,
  DATE_TRUNC('day', hour) AS day,
  AVG(avg_precip) AS daily_avg_precip,
  SUM(event_count) AS total_events,
  MAX(avg_precip) AS max_hourly_precip
FROM hourly_precip
GROUP BY 1, 2
HAVING total_events > 10
ORDER BY daily_avg_precip DESC, total_events DESC;
"""
    elif query_num == 2:
        return "SELECT COUNT(*) FROM LIFT_TICKETS WHERE LIFT_TICKET_ID = 1"
    else:
        return "SELECT COUNT(*) FROM LIFT_TICKETS WHERE LIFT_TICKET_ID = 1"

def run_test():
    snow = connect_snow()
    cursor = snow.cursor()
    start_time = datetime.now()
    cursor.execute(get_query(query_num))
    cursor.fetchall()
    cursor.close()
    elapsed_time = (datetime.now() - start_time).total_seconds()
    latencies.append(elapsed_time * 1000)
    snow.close()

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python query.py <query_num>")
        sys.exit(1)
    
    args = sys.argv[1:]
    query_num = int(args[0])

    for i in range(num_tests):
        print(f"\nRunning test {i+1} of {num_tests}")
        run_test()
    print_results(latencies)