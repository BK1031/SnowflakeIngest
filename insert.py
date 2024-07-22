from datetime import datetime, time
import os, sys, logging
import json
from connect import connect_snow
from init import init_sql
import snowflake.connector
from data_generator import generate_lift_tickets
from results import print_results

from dotenv import load_dotenv
from cryptography.hazmat.primitives import serialization

load_dotenv()
logging.basicConfig(level=logging.WARN)
snowflake.connector.paramstyle='qmark'

def save_to_snowflake(snow, records):
    start_time = datetime.now()    
    cursor = snow.cursor()
    for i, record in enumerate(records):
        record['sent_at'] = datetime.utcnow().isoformat()
        row = (record['txid'], record['rfid'], record["resort"], record["purchase_time"], record["expiration_time"], record['days'], record['name'], json.dumps(record['address']), record['phone'], record['email'], json.dumps(record['emergency_contact']), record['sent_at'])
        cursor.execute("INSERT INTO LIFT_TICKETS (\"TXID\",\"RFID\",\"RESORT\",\"PURCHASE_TIME\", \"EXPIRATION_TIME\",\"DAYS\",\"NAME\",\"ADDRESS\",\"PHONE\",\"EMAIL\",\"EMERGENCY_CONTACT\",\"SENT_AT\") SELECT ?,?,?,?,?,?,?,PARSE_JSON(?),?,?,PARSE_JSON(?),?", row)
        print(f"inserted ticket ({i}) {record['txid']}")
    
    cursor.close()
    snow.commit()
    print(f"\nInserted {len(records):,} records in {(datetime.now() - start_time).total_seconds() * 1000:.2f} ms")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python insert.py <number_of_entries>")
        sys.exit(1)
    
    num_entries = int(sys.argv[1])
    print(f"Generating {num_entries} fake lift tickets")
    fake_data = generate_lift_tickets(num_entries)
    
    snow = connect_snow()
    init_sql(snow)
    save_to_snowflake(snow, fake_data)
    print_results(snow, num_entries)
    snow.close()