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
    print(f"👤 Passenger: {booking['passenger']}")
    print(f"📍 Pickup: {booking['pickup']} -> 🎯 Drop: {booking['drop']}")
    print(f"🚗 Driver: {booking['driver']} | Cab: {booking['cab']}")
    print(f"📏 Distance: {booking.get('distance_km', 'N/A')} km | ETA: {booking.get('eta_min', 'N/A')} min")
    print("-" * 50)

