from datetime import datetime, time
import os, sys, logging
import json
from init import init_sql
import snowflake.connector
from data_generator import generate_lift_tickets
from results import print_results

from dotenv import load_dotenv
from cryptography.hazmat.primitives import serialization

load_dotenv()
logging.basicConfig(level=logging.WARN)
snowflake.connector.paramstyle='qmark'


def connect_snow():
    private_key = "-----BEGIN PRIVATE KEY-----\n" + os.getenv("PRIVATE_KEY") + "\n-----END PRIVATE KEY-----\n)"
    p_key = serialization.load_pem_private_key(
        bytes(private_key, 'utf-8'),
        password=None
    )
    pkb = p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption())

    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        private_key=pkb,
        role="BHARAT_INGEST",
        database="BHARAT_KAFKA",
        schema="PUBLIC",
        warehouse="BHARAT_INGEST",
        session_parameters={'QUERY_TAG': 'py-insert'}, 
    )


def save_to_snowflake(snow, records):
    start_time = datetime.now()
    from concurrent.futures import ThreadPoolExecutor, as_completed
    
    def insert_record(i, record):
        cursor = snow.cursor()
        record['sent_at'] = datetime.utcnow().isoformat()
        row = (record['txid'], record['rfid'], record["resort"], record["purchase_time"], record["expiration_time"], record['days'], record['name'], json.dumps(record['address']), record['phone'], record['email'], json.dumps(record['emergency_contact']), record['sent_at'])
        cursor.execute("INSERT INTO LIFT_TICKETS (\"TXID\",\"RFID\",\"RESORT\",\"PURCHASE_TIME\", \"EXPIRATION_TIME\",\"DAYS\",\"NAME\",\"ADDRESS\",\"PHONE\",\"EMAIL\",\"EMERGENCY_CONTACT\",\"SENT_AT\") SELECT ?,?,?,?,?,?,?,PARSE_JSON(?),?,?,PARSE_JSON(?),?", row)
        print(f"inserted ticket ({i}) {record['txid']}")
        cursor.close()

    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(insert_record, i, record) for i, record in enumerate(records)]
        as_completed(futures)

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
    print_results(snow, num_entries)
    snow.close()