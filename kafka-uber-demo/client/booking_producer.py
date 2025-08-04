import os
import json
import time
import random
import datetime
import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError

# === CONFIGURATION ===
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
KAFKA_TOPIC = "uber-booking"
ORS_API_KEY = os.getenv("ORS_API_KEY", "eyJvcmciOiI1YjNjZTM1OTc4NTExMTAwMDFjZjYyNDgiLCJpZCI6IjZjY2NlMGY1NWE4ODRlZDM4M2FmYTcwNjdiMzI1MjRkIiwiaCI6Im11cm11cjY0In0=")  # Replace with your key

# === DRIVERS DB (Simulated Nearby Driver Pool) ===
DRIVERS = [
    {"name": "Arun", "lat": 13.098, "lon": 77.610},
    {"name": "Divya", "lat": 13.092, "lon": 77.615},
    {"name": "Ravi", "lat": 13.087, "lon": 77.605},
    {"name": "Sneha", "lat": 13.101, "lon": 77.620},
    {"name": "Vikram", "lat": 13.095, "lon": 77.612}
]

# === FUNCTIONS ===
def geocode_location(location: str):
    try:
        response = requests.get(
            "https://api.openrouteservice.org/geocode/search",
            params={"api_key": ORS_API_KEY, "text": f"{location}, Bangalore", "size": 1},
            timeout=5
        )
        response.raise_for_status()
        coords = response.json()["features"][0]["geometry"]["coordinates"]
        return coords[1], coords[0]  # (lat, lon)
    except Exception as e:
        print(f"❌ Geocoding failed for '{location}': {e}")
        return None, None

def find_nearest_driver(pickup_lat, pickup_lon):
    def distance(d):
        return ((d["lat"] - pickup_lat) ** 2 + (d["lon"] - pickup_lon) ** 2) ** 0.5
    return min(DRIVERS, key=distance)

def create_producer():
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            retries=3
        )
        print(f"✅ Connected to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
        return producer
    except KafkaError as e:
        print(f"❌ Kafka Connection Error: {e}")
        exit(1)

# === MAIN ===
if __name__ == "__main__":
    producer = create_producer()

    while True:
        name = input("👤 Enter passenger name (or 'exit'): ").strip()
        if name.lower() == "exit":
            break

        pickup_text = input("📍 Pickup location: ").strip()
        drop_text = input("🎯 Drop location: ").strip()

        pickup_lat, pickup_lon = geocode_location(pickup_text)
        drop_lat, drop_lon = geocode_location(drop_text)

        if not all([pickup_lat, pickup_lon, drop_lat, drop_lon]):
            print("⚠️ Booking skipped due to geocoding failure.\n")
            continue

        driver = find_nearest_driver(pickup_lat, pickup_lon)
        booking_id = f"BK{random.randint(1000, 9999)}"

        payload = {
            "booking_id": booking_id,
            "passenger_name": name,
            "pickup_location": {"text": pickup_text, "lat": pickup_lat, "lon": pickup_lon},
            "drop_location": {"text": drop_text, "lat": drop_lat, "lon": drop_lon},
            "driver": {"name": driver["name"], "lat": driver["lat"], "lon": driver["lon"]},
            "cab_number": f"KA-{random.randint(10, 99)}-{random.randint(1000, 9999)}",
            "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat()
        }

        producer.send(KAFKA_TOPIC, payload)
        print(f"✅ Booking Confirmed!\n🚖 Driver: {driver['name']} | Cab: {payload['cab_number']}\n📦 Sent to Kafka\n")

    print("👋 Exiting.")

