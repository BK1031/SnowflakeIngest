import os
import snowflake.connector

from dotenv import load_dotenv
from cryptography.hazmat.primitives import serialization

load_dotenv()

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

def init_sql(snow):
    cursor = snow.cursor()
    cursor.execute("""
    CREATE OR REPLACE TABLE LIFT_TICKETS (
        TXID varchar(255),
        RFID varchar(255),
        RESORT varchar(255),
        PURCHASE_TIME datetime,
        EXPIRATION_TIME date,
        DAYS number,
        NAME varchar(255),
        ADDRESS variant,
        PHONE varchar(255),
        EMAIL varchar(255),
        EMERGENCY_CONTACT variant,
        SENT_AT timestamp(6),
        CREATED_AT timestamp(6) DEFAULT CURRENT_TIMESTAMP(6)
    );
    """)
    cursor.close()
    snow.commit()
    print("Table LIFT_TICKETS created or replaced successfully.")