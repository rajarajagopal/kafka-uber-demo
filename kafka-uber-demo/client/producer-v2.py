import json
import random
import requests
from kafka import KafkaProducer

ORS_API_KEY = "eyJvcmciOiI1YjNjZTM1OTc4NTExMTAwMDFjZjYyNDgiLCJpZCI6IjZjY2NlMGY1NWE4ODRlZDM4M2FmYTcwNjdiMzI1MjRkIiwiaCI6Im11cm11cjY0In0="
KAFKA_TOPIC = "uber-booking"
KAFKA_BROKER = "localhost:9092"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

drivers = [
    {"name": "Sharan", "car": "KA01AB1234"},
    {"name": "Vishnu", "car": "KA02CD5678"},
    {"name": "Raj", "car": "KA03EF9012"},
    {"name": "Kashaf", "car": "KA03MN9823"},
    {"name": "Hari", "car": "KA05QR3456"},
]

def geocode_location(location):
    try:
        response = requests.get(
            "https://api.openrouteservice.org/geocode/search",
            params={"api_key": ORS_API_KEY, "text": location, "boundary.country": "IN"}
        )
        response.raise_for_status()
        coords = response.json()["features"][0]["geometry"]["coordinates"]
        return coords[1], coords[0]  # (lat, lon)
    except:
        print(f"❌ Could not geocode: {location}")
        return None

def calculate_distance(pickup_coords, drop_coords):
    try:
        response = requests.post(
            "https://api.openrouteservice.org/v2/directions/driving-car",
            headers={"Authorization": ORS_API_KEY},
            json={"coordinates": [[pickup_coords[1], pickup_coords[0]], [drop_coords[1], drop_coords[0]]]}
        )
        response.raise_for_status()
        route = response.json()["routes"][0]["segments"][0]
        return round(route["distance"] / 1000, 2), round(route["duration"] / 60, 1)
    except:
        print("❌ Error fetching route info")
        return None, None

print("🚕 UBER-CLI Booking Producer")

while True:
    passenger = input("👤 Enter passenger name (or 'exit'): ").strip()
    if passenger.lower() == "exit":
        break

    pickup = input("📍 Pickup location: ").strip()
    drop = input("🎯 Drop location: ").strip()

    pickup_coords = geocode_location(f"{pickup}, Bangalore")
    drop_coords = geocode_location(f"{drop}, Bangalore")

    if not pickup_coords or not drop_coords:
        continue

    distance_km, eta_min = calculate_distance(pickup_coords, drop_coords)

    driver = random.choice(drivers)

    booking = {
        "passenger": passenger,
        "pickup": pickup,
        "drop": drop,
        "driver": driver["name"],
        "cab": driver["car"],
        "distance_km": distance_km,
        "eta_min": eta_min
    }

    producer.send(KAFKA_TOPIC, booking)

    print("✅ Booking Confirmed!")
    print(f"🚖 Driver: {driver['name']} | Cab: {driver['car']}")
    print(f"📏 Distance: {distance_km} km | ⏱️ ETA: {eta_min} min")

