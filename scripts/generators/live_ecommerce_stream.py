import json
import time
import random
import uuid
from datetime import datetime
from confluent_kafka import Producer
# Read from variable from .env
import os
from dotenv import load_dotenv

# Load the secrets from the .env file into the system
load_dotenv()

# Safely fetch them
api_key = os.getenv("CONFLUENT_API_KEY")
api_secret = os.getenv("CONFLUENT_API_SECRET")

# 1. Configure the Producer
conf = {
    'bootstrap.servers': 'pkc-oz2po.ap-southeast-3.aws.confluent.cloud:9092',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': api_key,
    'sasl.password': api_secret
}

producer = Producer(conf)
topic = 'ecommerce_transactions_live'

# 2. Define a delivery callback
def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Delivered to topic {msg.topic()} partition [{msg.partition()}]")

print(f"Starting live stream to {topic}... (Press Ctrl+C to stop)")

# 3. Generate and stream live events
try:
    while True:
        event = {
            "event_id": str(uuid.uuid4()),
            "event_timestamp": datetime.utcnow().isoformat(),
            "platform": random.choice(["Web", "iOS", "Android"]),
            "user_id": f"user_{random.randint(1000, 9999)}",
            "checkout_amount": round(random.uniform(15.0, 250.0), 2)
        }
        
        # Serialize JSON and send to Confluent Cloud
        producer.produce(
            topic, 
            key=event["platform"], 
            value=json.dumps(event).encode('utf-8'), 
            callback=delivery_report
        )
        
        # Trigger any available delivery report callbacks
        producer.poll(0)
        time.sleep(1) # Fire 1 event per second

except KeyboardInterrupt:
    print("Stopping stream...")
finally:
    # Wait for any outstanding messages to be delivered
    producer.flush()