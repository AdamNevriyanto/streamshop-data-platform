import json
import time
import random
from datetime import datetime
import uuid
import os  # <-- Add this import

def generate_event():
    event_types = ['page_view', 'add_to_cart', 'checkout', 'purchase']
    # Simulate realistic user IDs and platforms
    user_id = random.randint(1000, 9999)
    event_type = random.choices(event_types, weights=[60, 25, 10, 5])[0]
    
    # Only attach an amount if they actually checked out or purchased
    amount = round(random.uniform(15.0, 899.0), 2) if event_type in ['checkout', 'purchase'] else 0.0
    
    return {
        "event_id": str(uuid.uuid4()),
        "user_id": user_id,
        "event_type": event_type,
        "amount": amount,
        "platform": random.choice(["iOS", "Android", "Web"]),
        "event_time": datetime.utcnow().isoformat() + "Z"
    }

if __name__ == "__main__":
    print("Starting StreamShop data generator... Press Ctrl+C to stop.")
    
    # 1. Get the absolute path of the directory containing this script
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    # 2. Build the path to data/raw by going up two levels from the script's location
    target_dir = os.path.abspath(os.path.join(script_dir, "../../data/raw"))
    
    # 3. Create the directory safely if it somehow doesn't exist
    os.makedirs(target_dir, exist_ok=True)
    
    # 4. Define the final output file
    output_file = os.path.join(target_dir, "dummy_stream.json")
    
    try:
        with open(output_file, "a") as f:
            while True:
                event = generate_event()
                f.write(json.dumps(event) + "\n")
                f.flush()
                print(f"Generated: {event['event_type']} | User: {event['user_id']} | Amount: ${event['amount']}")
                # Sleep for a random fraction of a second to simulate real traffic
                time.sleep(random.uniform(0.1, 1.5))
    except KeyboardInterrupt:
        print("\nData generation stopped.")