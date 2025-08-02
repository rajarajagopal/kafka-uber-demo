import json
from kafka import KafkaConsumer

print("🛰️  UBER-CLI Booking Consumer")

try:
    consumer = KafkaConsumer(
        'cab_bookings',
        bootstrap_servers='localhost:9092',
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='uber-cli-group'
    )
except Exception as e:
    print("❌ Failed to connect to Kafka:", e)
    exit(1)

print("✅ Connected! Waiting for bookings...\n")

for message in consumer:
    booking = message.value
    print(f"📦 New Booking: {booking['passenger']} from {booking['pickup']} to {booking['drop']}")

