import os
import certifi

# 1. Fix the Miniconda SSL bug before initializing anything
os.environ["SSL_CERT_FILE"] = certifi.where()

import time
import random
import uuid
from datetime import datetime
from datetime import timezone
from dotenv import load_dotenv
from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

load_dotenv()

topic = 'ecommerce_transactions_avro' # Let's use a fresh topic for Avro data

# 1. Define the Data Contract (Avro Schema)
schema_str = """
{
  "namespace": "streamshop.ecommerce",
  "type": "record",
  "name": "Checkout",
  "fields": [
    {"name": "event_id", "type": "string"},
    {"name": "event_timestamp", "type": "string"},
    {"name": "platform", "type": "string"},
    {"name": "user_id", "type": "string"},
    {"name": "checkout_amount", "type": "double"}
  ]
}
"""

# 2. Configure Schema Registry Client
sr_client = SchemaRegistryClient({
    'url': os.getenv("SCHEMA_REGISTRY_URL"),
    'basic.auth.user.info': f"{os.getenv('SCHEMA_REGISTRY_KEY')}:{os.getenv('SCHEMA_REGISTRY_SECRET')}"
})

# 3. Initialize the Avro Serializer
avro_serializer = AvroSerializer(schema_registry_client=sr_client, schema_str=schema_str)

# 4. Configure the Serializing Producer
producer_conf = {
    'bootstrap.servers': 'pkc-oz2po.ap-southeast-3.aws.confluent.cloud:9092',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': os.getenv("CONFLUENT_API_KEY"),
    'sasl.password': os.getenv("CONFLUENT_API_SECRET"),
    'key.serializer': StringSerializer('utf_8'),
    'value.serializer': avro_serializer
}

producer = SerializingProducer(producer_conf)

print(f"Starting Avro stream to {topic}...")

# 5. Generate and Stream
try:
    while True:
        # A standard Python Dictionary
        event = {
            "event_id": str(uuid.uuid4()),
            "event_timestamp": datetime.now(timezone.utc).isoformat(),
            "platform": random.choice(["Web", "iOS", "Android"]),
            "user_id": f"user_{random.randint(1000, 9999)}",
            "checkout_amount": round(random.uniform(15.0, 250.0), 2)
        }
        
        # The producer automatically translates the dictionary to Avro binary using the schema
        producer.produce(topic=topic, key=event["platform"], value=event)
        producer.poll(0)
        
        print(f"Produced Avro record: {event['event_id']}")
        time.sleep(1)

except KeyboardInterrupt:
    print("Stopping stream...")
finally:
    producer.flush()