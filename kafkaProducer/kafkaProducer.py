from kafka import KafkaProducer
import json
import time

# Configuration
TOPIC = "reviews"
BOOTSTRAP_SERVERS = ["localhost:9092"]
INPUT_FILE = "allData.json"  # Make sure this is newline-delimited JSON

# Initialize producer
producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Read and send messages
with open(INPUT_FILE, "r", encoding='utf-8') as f:
    for line in f:
        try:
            data = json.loads(line.strip())
            producer.send(TOPIC, value=data)
            print(f"✅ Sent: {data}")
            time.sleep(0.1)  # Simulate streaming delay (optional)
        except json.JSONDecodeError as e:
            print(f"❌ Skipping invalid JSON line: {e}")

producer.flush()
producer.close()
print("🚀 Finished streaming all data.")
