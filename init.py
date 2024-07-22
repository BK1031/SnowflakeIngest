import snowflake.connector

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