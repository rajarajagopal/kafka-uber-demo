# passenger_producer.py
import json, time, random
from kafka import KafkaProducer
from faker import Faker

fake = Faker("en_IN")
producer = KafkaProducer(
    bootstrap_servers=['localhost:9094'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

locations = ["BTM", "HSR", "Whitefield", "Hebbal", "Varthur", "Devanahalli"]

def generate_passenger():
    src = random.choice(locations)
    dst = random.choice([l for l in locations if l != src])
    return {
        "passenger_id": fake.uuid4(),
        "name": fake.first_name(),
        "from": src,
        "to": dst
    }

while True:
    passenger = generate_passenger()
    print(f"Sending: {passenger}")
    producer.send("ride-requests", passenger)
    time.sleep(10)  # every 10 sec

