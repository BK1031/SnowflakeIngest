from datetime import datetime, time
import os, sys, logging
import json
from init import init_sql
import snowflake.connector
from data_generator import generate_lift_tickets
from results import print_results

from dotenv import load_dotenv
from cryptography.hazmat.primitives import serialization

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