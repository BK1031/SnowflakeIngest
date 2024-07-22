import os, sys, logging
import json
import uuid
from init import init_sql
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
        session_parameters={'QUERY_TAG': 'py-copy'}, 
    )

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
    print(f"\nInserted {len(batch):,} records in {(datetime.now() - start_time).total_seconds() * 1000:.2f} ms")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python copy_into.py <number_of_entries>")
        sys.exit(1)
    
    args = sys.argv[1:]
    num_entries = int(args[0])
    snow = connect_snow()
    init_sql(snow)
    temp_dir = tempfile.TemporaryDirectory()
    
    print(f"Generating {num_entries} fake lift tickets")
    fake_data = generate_lift_tickets(num_entries)
    
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