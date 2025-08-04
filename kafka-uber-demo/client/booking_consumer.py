from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    "booking-topic",
    bootstrap_servers="localhost:9094",
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    auto_offset_reset="earliest",  # to see old messages too
    enable_auto_commit=True,
    group_id="uber-consumer-group"
)

print("🚦 Listening for Uber bookings...")

for message in consumer:
    booking = message.value
    print("🚖 New Booking:")
    print("---------------")
    print(f"👤 Passenger: {booking.get('customer', 'Unknown')}")
    print(f"📍 Pickup: {booking.get('pickup', '')} -> 🎯 Drop: {booking.get('drop', '')}")
    
    driver = booking.get('driver_assigned', {})
    print(f"🚗 Driver: {driver.get('name', 'N/A')} | Cab: {driver.get('car_number', 'N/A')}")
    
    print(f"📏 Distance: {booking.get('distance_km', 'N/A')} km | ETA: {booking.get('eta_min', 'N/A')} min")
    print("-" * 50)

