import json
import uuid
import random
import requests
from kafka import KafkaProducer
import openrouteservice
from openrouteservice.exceptions import ApiError

# OpenRouteService API key
ORS_API_KEY = "eyJvcmciOiI1YjNjZTM1OTc4NTExMTAwMDFjZjYyNDgiLCJpZCI6IjZjY2NlMGY1NWE4ODRlZDM4M2FmYTcwNjdiMzI1MjRkIiwiaCI6Im11cm11cjY0In0="  

# Kafka producer setup
producer = KafkaProducer(
    bootstrap_servers='localhost:9094',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Dummy drivers
drivers = [
    {"name": "Vishnu", "car_number": "KA01AB1234"},
    {"name": "Kashaf", "car_number": "KA02CD5678"},
    {"name": "Hari", "car_number": "KA03EF9012"},
    {"name": "Raj", "car_number": "KA01KA7200"}
]

# Geocoding function using bounding box for Bangalore
def get_coordinates(place):
    try:
        url = "https://api.openrouteservice.org/geocode/search"
        params = {
            'api_key': ORS_API_KEY,
            'text': place,
            'boundary.rect.min_lon': 77.4,
            'boundary.rect.min_lat': 12.8,
            'boundary.rect.max_lon': 77.8,
            'boundary.rect.max_lat': 13.2,
            'size': 1
        }
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        coords = data['features'][0]['geometry']['coordinates']  # [lon, lat]
        print(f"📍 Resolved {place} to coordinates: {coords[::-1]}")
        return coords  # [lon, lat] format
    except (IndexError, KeyError, requests.RequestException) as e:
        print(f"❌ Error fetching location for {place}: {e}")
        return None

# Collect user input
print("🚖 Booking App\n--------------")
passenger = input("👤 Passenger name: ")
pickup = input("📍 Pickup location: ")
drop = input("🎯 Drop location: ")

pickup_coords = get_coordinates(pickup)
drop_coords = get_coordinates(drop)

if not pickup_coords or not drop_coords:
    print("❌ Invalid pickup or drop location. Exiting.")
    exit(1)

# OpenRouteService client
client = openrouteservice.Client(key=ORS_API_KEY)

# Calculate route
try:
    route = client.directions(
        coordinates=[pickup_coords, drop_coords],  # both are [lon, lat]
        profile='driving-car',
        format='geojson'
    )
    distance_km = round(route['features'][0]['properties']['segments'][0]['distance'] / 1000, 2)
    duration_min = round(route['features'][0]['properties']['segments'][0]['duration'] / 60, 2)
except Exception as e:
    print(f"⚠️ Failed to calculate route: {e}")
    distance_km = None
    duration_min = None

# Randomly assign a driver
assigned_driver = random.choice(drivers)

# Compose a message
booking_message = {
    "ride_id": str(uuid.uuid4()),
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

# Output
print("\n✅ Booking message sent to Kafka:")
print(json.dumps(booking_message, indent=2))

