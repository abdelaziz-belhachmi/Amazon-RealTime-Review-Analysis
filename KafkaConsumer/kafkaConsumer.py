from kafka import KafkaConsumer
import json
import os

# Parameters
TOPIC = "reviews"
BOOTSTRAP_SERVERS = ["localhost:9092"]  # Adjust if your Kafka broker is on another host/port
OUTPUT_FILE = "../Data/Data.json"

# Ensure output directory exists
os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

# Create Kafka consumer
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='json-writer-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print(f"📡 Listening to topic '{TOPIC}'... Writing to {OUTPUT_FILE}")

# Open file in append mode and write incoming data
with open(OUTPUT_FILE, "a", encoding='utf-8') as f:
    for message in consumer:
        try:
            json.dump(message.value, f, ensure_ascii=False)
            f.write("\n")
            print(f"✅ Received & saved: {message.value}")
        except Exception as e:
            print(f"❌ Error writing message: {e}")
