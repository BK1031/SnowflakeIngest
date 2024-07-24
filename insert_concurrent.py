from datetime import datetime, time
import os, sys, logging
import json
import threading
from init import init_sql, connect_snow
import snowflake.connector
import concurrent.futures
from data_generator import generate_lift_tickets
from results import print_results

from dotenv import load_dotenv
from cryptography.hazmat.primitives import serialization

load_dotenv()
logging.basicConfig(level=logging.WARN)
snowflake.connector.paramstyle='qmark'

latencies = []

def listen_for_ticket(snow, record):
    found = False
    while not found:
        cursor = snow.cursor()
        cursor.execute("SELECT * FROM LIFT_TICKETS WHERE TXID = ?", (record['txid'],))
        result = cursor.fetchone()
        if result:
            latency = (datetime.utcnow() - datetime.fromisoformat(record['sent_at'])).total_seconds() * 1000
            latencies.append(latency)
            found = True
    cursor.close()

def save_to_snowflake(snow, records):
    start_time = datetime.now()
    cursor = snow.cursor()
    threads = []
    for i, record in enumerate(records):
        record['sent_at'] = datetime.utcnow().isoformat()
        
        thread = threading.Thread(target=listen_for_ticket, args=(snow, record))
        thread.start()
        threads.append(thread)

        def insert_record(i, record):
            cursor = snow.cursor()
            row = (record['txid'], record['rfid'], record["resort"], record["purchase_time"], record["expiration_time"], record['days'], record['name'], json.dumps(record['address']), record['phone'], record['email'], json.dumps(record['emergency_contact']), record['sent_at'])
            cursor.execute("INSERT INTO LIFT_TICKETS (\"TXID\",\"RFID\",\"RESORT\",\"PURCHASE_TIME\", \"EXPIRATION_TIME\",\"DAYS\",\"NAME\",\"ADDRESS\",\"PHONE\",\"EMAIL\",\"EMERGENCY_CONTACT\",\"SENT_AT\") SELECT ?,?,?,?,?,?,?,PARSE_JSON(?),?,?,PARSE_JSON(?),?", row)
            print(f"inserted ticket ({i}) {record['txid']}")

        with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
            executor.submit(insert_record, i, record)
    
    # Wait for all threads to complete
    for thread in threads:
        thread.join()
    
    cursor.close()
    snow.commit()
    print(f"\nInserted {len(records):,} records in {(datetime.now() - start_time).total_seconds() * 1000:.2f} ms")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python insert_concurrent.py <number_of_entries>")
        sys.exit(1)
    
    num_entries = int(sys.argv[1])
    print(f"Generating {num_entries} fake lift tickets")
    fake_data = generate_lift_tickets(num_entries)
    
    snow = connect_snow()
    init_sql(snow)
    save_to_snowflake(snow, fake_data)
    print_results(latencies)
    snow.close()