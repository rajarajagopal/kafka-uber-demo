# cab-location-consumer.py
import json
import redis
from kafka import KafkaConsumer

# Redis setup
redis_client = redis.Redis(host='localhost', port=6379, db=0)

# Kafka Consumer
consumer = KafkaConsumer(
    "cab-locations",
    bootstrap_servers=["localhost:9094"],
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    auto_offset_reset="latest",
    group_id="cab-consumer-group"
)

print("🚕 Listening for driver location updates...")

for msg in consumer:
    driver = msg.value
    driver_id = driver['driver_id']
    redis_client.set(driver_id, json.dumps(driver))
    print(f"📍 Updated location for driver {driver['name']} in {driver['location']}")

