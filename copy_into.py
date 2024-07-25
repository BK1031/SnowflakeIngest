import os, sys, logging
import json
import uuid
from init import init_sql, connect_snow
from results import print_results
import snowflake.connector
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import tempfile

from data_generator import generate_lift_tickets
from datetime import datetime

from dotenv import load_dotenv
from cryptography.hazmat.primitives import serialization

load_dotenv()
logging.basicConfig(level=logging.WARN)

num_entries = 1
num_tests = 1

latencies = []

def save_to_snowflake(snow, batch, temp_dir):
    logging.debug("inserting batch to db")
    pandas_df = pd.DataFrame(
        batch,
        columns=[
            "TXID",
            "RFID",
            "RESORT",
            "PURCHASE_TIME",
            "EXPIRATION_TIME",
            "DAYS",
            "NAME",
            "ADDRESS",
            "PHONE",
            "EMAIL",
            "EMERGENCY_CONTACT",
            "SENT_AT",
        ],
    )
    arrow_table = pa.Table.from_pandas(pandas_df)
    out_path = f"{temp_dir.name}/{str(uuid.uuid1())}.parquet"
    pq.write_table(arrow_table, out_path, use_dictionary=False, compression="SNAPPY")
    start_time = datetime.now()
    snow.cursor().execute(
        "PUT 'file://{0}' @%LIFT_TICKETS".format(out_path)
    )
    os.unlink(out_path)
    snow.cursor().execute(
        "COPY INTO LIFT_TICKETS FILE_FORMAT=(TYPE='PARQUET') MATCH_BY_COLUMN_NAME=CASE_SENSITIVE PURGE=TRUE"
    )
    latencies.append((datetime.now() - start_time).total_seconds() * 1000)
    print(f"\nInserted {len(batch):,} records in {(datetime.now() - start_time).total_seconds() * 1000:.2f} ms")

def run_test(fake_data):
    snow = connect_snow()
    init_sql(snow)
    temp_dir = tempfile.TemporaryDirectory()
    
    batch = []
    for record in fake_data:
        record['sent_at'] = datetime.utcnow().isoformat()
        batch.append(
            (
                record["txid"],
                record["rfid"],
                record["resort"],
                record["purchase_time"],
                record["expiration_time"],
                record["days"],
                record["name"],
                json.dumps(record["address"]),
                record["phone"],
                record["email"],
                json.dumps(record["emergency_contact"]),
                record["sent_at"],
            )
        )
    
    save_to_snowflake(snow, batch, temp_dir)
    temp_dir.cleanup()
    snow.close()

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python copy_into.py <number_of_entries>")
        sys.exit(1)
    
    args = sys.argv[1:]
    num_entries = int(args[0])
    print(f"Generating {num_entries} fake lift tickets")
    fake_data = generate_lift_tickets(num_entries)

    for i in range(num_tests):
        print(f"\nRunning test {i+1} of {num_tests}")
        run_test(fake_data)
    print_results(latencies)