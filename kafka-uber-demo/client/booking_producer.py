import json
import random
from kafka import KafkaProducer
import openrouteservice
from openrouteservice.exceptions import ApiError

# Replace with your actual OpenRouteService API key
ORS_API_KEY = "eyJvcmciOiI1YjNjZTM1OTc4NTExMTAwMDFjZjYyNDgiLCJpZCI6IjZjY2NlMGY1NWE4ODRlZDM4M2FmYTcwNjdiMzI1MjRkIiwiaCI6Im11cm11cjY0In0="

# Kafka producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9094',  # Or 9092, depending on your Kafka setup
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Simulated drivers list
drivers = [
    {"name": "Vishnu", "car_number": "KA01AB1234"},
    {"name": "Kashaf", "car_number": "KA02CD5678"},
    {"name": "Hari", "car_number": "KA03EF9012"},
    {"name": "Raj", "car_number": "KA01KA7200"}
]

# Input from user
print("🚖 Booking App\n--------------")
passenger = input("👤 Passenger name: ")
pickup = input("📍 Pickup location: ")
drop = input("🎯 Drop location: ")

client = openrouteservice.Client(key=ORS_API_KEY)

def get_coordinates(place):
    try:
        result = client.pelias_search(text=place)
        coords = result['features'][0]['geometry']['coordinates']  # [lon, lat]
        return coords[::-1]  # [lat, lon]
    except (IndexError, ApiError, KeyError, TypeError) as e:
        print(f"❌ Error fetching location for {place}: {e}")
        return None

# Fetch coordinates
pickup_coords = get_coordinates(pickup)
drop_coords = get_coordinates(drop)

if not pickup_coords or not drop_coords:
    print("❌ Invalid pickup or drop location. Exiting.")
    exit(1)

# Try calculating route (for distance/ETA)
try:
    route = client.directions(
        coordinates=[pickup_coords[::-1], drop_coords[::-1]],  # input as [lon, lat]
        profile='driving-car',
        format='geojson'
    )
    distance_km = round(route['features'][0]['properties']['segments'][0]['distance'] / 1000, 2)
    duration_min = round(route['features'][0]['properties']['segments'][0]['duration'] / 60, 2)
except Exception as e:
    print(f"⚠️ Failed to calculate route: {e}")
    distance_km = None
    duration_min = None

# Assign driver
assigned_driver = random.choice(drivers)

# Compose message
booking_message = {
    "customer": passenger,
    "pickup": pickup,
    "drop": drop,
    "driver_assigned": assigned_driver,
    "distance_km": distance_km,
    "eta_min": duration_min
}

# Send message to Kafka
producer.send("booking-topic", booking_message)
producer.flush()

print("✅ Booking message sent to Kafka:")
print(json.dumps(booking_message, indent=2))

