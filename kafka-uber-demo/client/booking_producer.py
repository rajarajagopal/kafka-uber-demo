import json
import random
from kafka import KafkaProducer
import openrouteservice
from openrouteservice.exceptions import ApiError

# Replace with your actual OpenRouteService API key
ORS_API_KEY = "eyJvcmciOiI1YjNjZTM1OTc4NTExMTAwMDFjZjYyNDgiLCJpZCI6IjZjY2NlMGY1NWE4ODRlZDM4M2FmYTcwNjdiMzI1MjRkIiwiaCI6Im11cm11cjY0In0="

producer = KafkaProducer(
    bootstrap_servers='localhost:9094',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Simulated drivers list
drivers = [
    {"id": "DRV101", "name": "Vishnu", "car": "KA01AB1234"},
    {"id": "DRV102", "name": "Kashaf", "car": "KA02CD5678"},
    {"id": "DRV103", "name": "Hari", "car": "KA03EF9012"}
]

# Get input
name = input("Passenger Name: ")
pickup_location = input("Enter pickup location: ")
drop_location = input("Enter drop location: ")

# Geocode using OpenRouteService
client = openrouteservice.Client(key=ORS_API_KEY)

def get_coordinates(place_name):
    try:
        result = client.pelias_search(text=place_name)
        coords = result['features'][0]['geometry']['coordinates']
        return coords[::-1]  # return as (lat, lon)
    except (IndexError, ApiError):
        print(f"❌ Failed to find location: {place_name}")
        return None

pickup_coords = get_coordinates(pickup_location)
drop_coords = get_coordinates(drop_location)

if not pickup_coords or not drop_coords:
    print("❌ Invalid pickup or drop location. Exiting.")
    exit(1)

# Randomly assign a driver
assigned_driver = random.choice(drivers)

booking_event = {
    "passenger": name,
    "pickup_location": pickup_location,
    "pickup_coordinates": pickup_coords,
    "drop_location": drop_location,
    "drop_coordinates": drop_coords,
    "driver": assigned_driver
}

# Send message
producer.send('booking-topic', booking_event)
producer.flush()
print("✅ Booking request sent to Kafka:")
print(json.dumps(booking_event, indent=2))

