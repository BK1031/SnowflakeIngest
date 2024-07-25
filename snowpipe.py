from datetime import datetime
import os, sys, logging
import json
import uuid
from data_generator import generate_lift_tickets
from init import init_sql, connect_snow
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import tempfile

from dotenv import load_dotenv
from results import print_results
from snowflake.ingest import SimpleIngestManager
from snowflake.ingest import StagedFile

load_dotenv()
from cryptography.hazmat.primitives import serialization

logging.basicConfig(level=logging.WARN)

num_entries = 1
num_tests = 1
latencies = []

def wait_for_data(snow, rows):
    start_time = datetime.now()
    sent_updates = []
    cursor = snow.cursor()
    print(f"Waiting for data to be inserted")
    while True:
        cursor.execute(f"SELECT COUNT(*) FROM BHARAT_KAFKA.PUBLIC.LIFT_TICKETS")
        count = cursor.fetchone()[0]
        elapsed_time = (datetime.now() - start_time).total_seconds()
        if count == rows:
            print(f"Found {count} rows. Elapsed time: {elapsed_time} seconds")
            latencies.append(elapsed_time * 1000)
            break
        if elapsed_time > 10 and int(elapsed_time) % 5 == 0 and int(elapsed_time) not in sent_updates:
            print(f"Found {count} rows. Elapsed time: {elapsed_time} seconds")
            sent_updates.append(int(elapsed_time))
    cursor.close()

def save_to_snowflake(snow, batch, temp_dir, ingest_manager):
    logging.debug('inserting batch to db')
    pandas_df = pd.DataFrame(batch, columns=["TXID","RFID","RESORT","PURCHASE_TIME", "EXPIRATION_TIME","DAYS","NAME","ADDRESS","PHONE","EMAIL", "EMERGENCY_CONTACT", "SENT_AT"])
    arrow_table = pa.Table.from_pandas(pandas_df)
    file_name = f"{str(uuid.uuid1())}.parquet"
    out_path =  f"{temp_dir.name}/{file_name}"
    pq.write_table(arrow_table, out_path, use_dictionary=False,compression='SNAPPY')
    snow.cursor().execute("PUT 'file://{0}' @%LIFT_TICKETS".format(out_path))
    os.unlink(out_path)
    # send the new file to snowpipe to ingest (serverless)
    start_time = datetime.now()
    resp = ingest_manager.ingest_files([StagedFile(file_name, None),])
    print(f"response from snowflake for file {file_name}: {resp['responseCode']}")
    print(f"Inserted {len(batch):,} records in {(datetime.now() - start_time).total_seconds() * 1000:.2f} ms")

def run_test(batch_size, fake_data):
    snow = connect_snow()
    init_sql(snow)
    batch = []
    for record in fake_data:
        batch.append((record['txid'],record['rfid'],record["resort"],record["purchase_time"],record["expiration_time"],record['days'],record['name'],record['address'],record['phone'],record['email'], record['emergency_contact'], record['sent_at']))
        if len(batch) == batch_size:
            save_to_snowflake(snow, batch, temp_dir, ingest_manager)
            batch = []
    if len(batch) > 0:
        save_to_snowflake(snow, batch, temp_dir, ingest_manager)
    wait_for_data(snow, num_entries)
    temp_dir.cleanup()
    snow.close()

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python snowpipe.py <number_of_entries>")
        sys.exit(1)
    
    args = sys.argv[1:]
    num_entries = int(args[0])

    temp_dir = tempfile.TemporaryDirectory()
    private_key = "-----BEGIN PRIVATE KEY-----\n" + os.getenv("PRIVATE_KEY") + "\n-----END PRIVATE KEY-----\n)"
    host = os.getenv("SNOWFLAKE_ACCOUNT") + ".snowflakecomputing.com"
    ingest_manager = SimpleIngestManager(account=os.getenv("SNOWFLAKE_ACCOUNT"),
                                         host=host,
                                         user=os.getenv("SNOWFLAKE_USER"),
                                         pipe='BHARAT_KAFKA.PUBLIC.LIFT_TICKETS_PIPE',
                                         private_key=private_key)
    
    print(f"Generating {num_entries} fake lift tickets")
    fake_data = generate_lift_tickets(num_entries)

    for i in range(num_tests):
        print(f"\nRunning test {i+1} of {num_tests}")
        run_test(num_entries, fake_data)
    print_results(latencies)