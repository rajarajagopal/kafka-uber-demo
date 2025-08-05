import json
import uuid
from kafka import KafkaConsumer, KafkaProducer

KAFKA_SERVER = "localhost:9094"
BOOKING_TOPIC = "booking-topic"
PAYMENT_TOPIC = "payment-topic"
CONFIRM_TOPIC = "confirmation-topic"  # optional second topic

# Create Kafka consumer to read new messages only
consumer = KafkaConsumer(
    BOOKING_TOPIC,
    bootstrap_servers=KAFKA_SERVER,
    auto_offset_reset="latest",             # Only new messages
    enable_auto_commit=True,
    group_id="payment-processor-group",     # Keeps track of offset
    value_deserializer=lambda m: json.loads(m.decode("utf-8"))
)

# Create producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

print("[Payment Producer] 🔄 Waiting for new booking...")

# Keep running and processing new bookings
for message in consumer:
    booking = message.value
    ride_id = booking.get("ride_id") or str(uuid.uuid4())
    customer = booking.get("customer", "Unknown")
    distance = booking.get("distance_km", 0)

    # Payment calculation logic
    base_fare = 50
    per_km_rate = 15
    amount = round(base_fare + distance * per_km_rate, 2)

    payment_data = {
    	"ride_id": booking.get("ride_id"),
    	"customer": booking.get("customer"),
    	"pickup": booking.get("pickup"),
    	"drop": booking.get("drop"),
   	"driver_assigned": booking.get("driver_assigned"),
    	"distance_km": booking.get("distance_km"),
    	"eta_min": booking.get("eta_min"),
    	"fare": amount
    }

    # Publish to both payment-topic and confirmation-topic
    producer.send(PAYMENT_TOPIC, value=payment_data)
    producer.send(CONFIRM_TOPIC, value=payment_data)
    producer.flush()

    print(f"[Payment Producer] ✅ Payment calculated for Ride ID {ride_id}: ₹{amount}")

