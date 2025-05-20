from kafka import KafkaProducer
import json
import time
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
TOPIC = "reviews"
BOOTSTRAP_SERVERS = ["kafka:9092"]
INPUT_FILE = "/app/Data/test2_data.json/part-00001-ae099f13-4824-4816-b1c3-98b57181a50a-c000.json"

# Initialize producer with callback functions
def on_success(record_metadata):
    logger.info("✅ SUCCESS: Message sent to Kafka!")
    logger.info(f"    Topic: {record_metadata.topic}")
    logger.info(f"    Partition: {record_metadata.partition}")
    logger.info(f"    Offset: {record_metadata.offset}")
    logger.info("------------------------")

def on_error(exc):
    logger.error("❌ ERROR: Failed to send message!")
    logger.error(f"    Error: {exc}")
    logger.error("------------------------")

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Read and send messages
message_count = 0
logger.info(f"Starting to stream data from {INPUT_FILE} to topic {TOPIC}")

with open(INPUT_FILE, "r", encoding='utf-8') as f:
    for line in f:
        try:
            data = json.loads(line.strip())
            future = producer.send(TOPIC, value=data)
            future.add_callback(on_success).add_errback(on_error)
            message_count += 1
            
            if message_count % 20 == 0:  # Log every 100 messages
                logger.info(f"📊 Progress: Sent {message_count} messages so far")
                logger.info("------------------------")
            
            time.sleep(5)  # Simulate streaming delay
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON line: {e}")

producer.flush()
logger.info(f"Finished streaming {message_count} messages")
producer.close()