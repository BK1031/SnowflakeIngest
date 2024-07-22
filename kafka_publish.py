from datetime import datetime
import json
import os
import logging
import sys
import confluent_kafka
from data_generator import generate_lift_tickets
from init import connect_snow
from kafka.admin import KafkaAdminClient, NewTopic

from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.WARN)

kafka_brokers = os.getenv("REDPANDA_BROKERS")

def create_topic():
    admin_client = KafkaAdminClient(bootstrap_servers=kafka_brokers, client_id='publish_data')
    topic_metadata = admin_client.list_topics()
    if topic_name not in topic_metadata:
        topic = NewTopic(name=topic_name, num_partitions=10, replication_factor=1)
        admin_client.create_topics(new_topics=[topic], validate_only=False)


def get_kafka_producer():
    print(f"Connecting to kafka")
    config = {'bootstrap.servers': kafka_brokers}
    return confluent_kafka.Producer(**config)

def reset_tables(snow):
    cursor = snow.cursor()
    cursor.execute(f"DELETE FROM {topic_name}")
    cursor.close()
    snow.commit()
    print(f"Deleted all rows from {topic_name}")

def wait_for_data(snow, rows):
    start_time = datetime.now()
    sent_updates = []
    cursor = snow.cursor()
    print(f"Waiting for data to be inserted into {topic_name}")
    while True:
        cursor.execute(f"SELECT COUNT(*) FROM {topic_name}")
        count = cursor.fetchone()[0]
        if count == rows:
            break
        elapsed_time = (datetime.now() - start_time).total_seconds()
        if elapsed_time > 10 and int(elapsed_time) % 5 == 0 and int(elapsed_time) not in sent_updates:
            print(f"Found {count} rows. Elapsed time: {elapsed_time} seconds")
            sent_updates.append(int(elapsed_time))

    cursor.close()
    print(f"Took {datetime.now() - start_time} seconds to insert {rows} rows into {topic_name}")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python kaf.py <topic_suffix> <number_of_entries>")
        sys.exit(1)

    args = sys.argv[1:]
    topic_name = f"LIFT_TICKETS_KAFKA_{args[0]}"
    batch_size = int(args[1])
    print(f"Generating {batch_size} fake lift tickets")
    fake_data = generate_lift_tickets(batch_size)

    snow = connect_snow()
    reset_tables(snow)

    producer = get_kafka_producer()
    create_topic()
    for message in fake_data:
        if message != '\n':
            message['sent_at'] = datetime.utcnow().isoformat()
            failed = True
            while failed:
                try:
                    d = json.dumps(message)
                    producer.produce(topic_name, value=bytes(d, encoding='utf8'))
                    failed = False
                except BufferError as e:
                    producer.flush()
        else:
            break
    producer.flush()
    wait_for_data(snow, batch_size)
    snow.close()