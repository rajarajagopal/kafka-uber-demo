from kafka import KafkaConsumer
import json

# Listen to both topics
consumer = KafkaConsumer(
    "booking-topic", "payment-topic",
    bootstrap_servers="localhost:9094",
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="uber-consumer-group"
)

print("🚦 Listening to booking and payment events...")

for message in consumer:
    topic = message.topic
    data = message.value

    if topic == "booking-topic":
        print("\n📦 [Booking Event]")
        print("--------------------")
        print(f"🆔 Ride ID: {data.get('ride_id', 'N/A')}")
        print(f"👤 Passenger: {data.get('customer', 'Unknown')}")
        print(f"📍 From: {data.get('pickup', '')} -> To: {data.get('drop', '')}")
        driver = data.get('driver_assigned', {})
        print(f"🚗 Driver: {driver.get('name', 'N/A')} | Cab: {driver.get('car_number', 'N/A')}")
        print(f"📏 Distance: {data.get('distance_km', 'N/A')} km | ETA: {data.get('eta_min', 'N/A')} min")

    elif topic == "payment-topic":
        print("\n💰 [Payment Event]")
        print("--------------------")
        print(f"🆔 Ride ID: {data.get('ride_id', 'N/A')}")
        print(f"👤 Passenger: {data.get('customer', 'Unknown')}")
        print(f"📍 From: {data.get('pickup', '')} -> To: {data.get('drop', '')}")
        driver = data.get('driver_assigned', {})
        print(f"🚗 Driver: {driver.get('name', 'N/A')} | Cab: {driver.get('car_number', 'N/A')}")
        print(f"📏 Distance: {data.get('distance_km', 'N/A')} km | ETA: {data.get('eta_min', 'N/A')} min")
        print(f"💵 Amount: ₹{data.get('fare', 'N/A')}")

    print("-" * 60)

