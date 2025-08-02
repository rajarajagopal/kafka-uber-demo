import json
from kafka import KafkaProducer

print("🚕 UBER-CLI Booking Producer")

try:
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
except Exception as e:
    print("❌ Failed to connect to Kafka:", e)
    exit(1)

while True:
    name = input("\n👤 Enter passenger name (or 'exit'): ")
    if name.lower() == 'exit':
        break
    pickup = input("📍 Pickup location: ")
    drop = input("🎯 Drop location: ")

    booking = {
        "passenger": name,
        "pickup": pickup,
        "drop": drop
    }

    producer.send('cab_bookings', booking)
    print("✅ Booking sent:", booking)

producer.close()

