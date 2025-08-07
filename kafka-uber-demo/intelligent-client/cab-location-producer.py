import json, time, random
from kafka import KafkaProducer
from faker import Faker

fake = Faker("en_IN")
producer = KafkaProducer(
    bootstrap_servers=['localhost:9094'],  # adjust if your Kafka is on a different port
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

locations = ["BTM", "HSR", "Whitefield", "Hebbal", "Varthur", "Devanahalli"]

def generate_driver():
    return {
        "driver_id": fake.uuid4(),
        "name": fake.first_name_male(),
        "vehicle_number": f"KA-{random.randint(10, 99)}-{random.randint(1000, 9999)}",
        "location": random.choice(locations)
    }

while True:
    driver = generate_driver()
    print(f"Sending: {driver}")
    producer.send("cab-locations", driver)
    time.sleep(10)

