import json
from kafka import KafkaConsumer

KAFKA_BOOTSTRAP_SERVERS = "localhost:9094"
MATCHED_TOPIC = "matched-rides"

consumer = KafkaConsumer(
    MATCHED_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    group_id="passenger-consumer",
    auto_offset_reset="latest",
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
)

print("🧾 Passenger consumer listening on 'matched-rides'...")

for message in consumer:
    ride = message.value
    passenger = ride.get("passenger", {})
    driver = ride.get("driver", {})

    print("\n🎉 Matched Ride Info:")
    print(f"🧍 Passenger: {passenger.get('name')} | From: {passenger.get('from')} → To: {passenger.get('to')}")
    print(f"🚗 Driver: {driver.get('name')}")
    print(f"💬 Reason: {driver.get('reason')}")

