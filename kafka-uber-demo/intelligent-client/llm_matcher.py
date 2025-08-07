import json
import redis
import threading
import requests
from kafka import KafkaConsumer, KafkaProducer

# Redis setup
redis_client = redis.Redis(host="localhost", port=6379, db=0)

# Kafka setup
KAFKA_BOOTSTRAP_SERVERS = "localhost:9094"
DRIVER_TOPIC = "cab-locations"
PASSENGER_TOPIC = "ride-requests"
MATCHED_TOPIC = "matched-rides"

# Kafka producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda x: json.dumps(x).encode("utf-8"),
)

def store_driver_in_redis(driver):
    driver_id = driver.get("driver_id")
    if driver_id:
        redis_key = f"driver:{driver_id}"
        redis_client.set(redis_key, json.dumps(driver), ex=60)  # 60 sec TTL

def get_all_drivers_from_redis():
    driver_keys = redis_client.keys("driver:*")
    drivers = []
    for key in driver_keys:
        try:
            driver_data = redis_client.get(key)
            if driver_data:
                drivers.append(json.loads(driver_data))
        except Exception as e:
            print(f"⚠️ Error reading driver data for {key}: {e}")
    return drivers

def call_llm_for_best_match(passenger, drivers):
    prompt = f"""
You are a smart cab matcher. Find the best driver for the following passenger based on location proximity and shared language.
Passenger: {json.dumps(passenger)}
Available Drivers: {json.dumps(drivers)}
Respond ONLY with a JSON like this:
{{ "name": "<driver name>", "reason": "<short reason>" }}
"""
    try:
        response = requests.post(
            "http://localhost:11434/api/generate",
            json={
                "model": "llama3",
                "prompt": prompt.strip(),
                "stream": False
            },
            timeout=30
        )
        response.raise_for_status()
        content = response.json().get("response", "")
        return json.loads(content.strip())
    except Exception as e:
        print(f"❌ LLM error or JSON parsing failed: {e}")
        return None

def handle_driver_messages():
    consumer = KafkaConsumer(
        DRIVER_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="llm-matcher-drivers",
        auto_offset_reset="latest",
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    )
    print(f"📡 Listening for drivers on '{DRIVER_TOPIC}'...")
    for message in consumer:
        driver = message.value
        print(f"🚗 Received driver: {driver}")
        store_driver_in_redis(driver)

def handle_passenger_messages():
    consumer = KafkaConsumer(
        PASSENGER_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="llm-matcher-passengers",
        auto_offset_reset="latest",
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    )
    print(f"🧍 Listening for passengers on '{PASSENGER_TOPIC}'...")
    for message in consumer:
        passenger = message.value
        print(f"📥 Received passenger: {passenger}")

        drivers = get_all_drivers_from_redis()
        if not drivers:
            print("⚠️ No available drivers. Skipping match.")
            continue

        match = call_llm_for_best_match(passenger, drivers)
        if match:
            print("✅ Best match:")
            print(json.dumps(match, indent=2))

            result = {
                "passenger": passenger,
                "driver": match
            }
            producer.send(MATCHED_TOPIC, value=result)
            producer.flush()
            print(f"📤 Published matched ride to '{MATCHED_TOPIC}'")
        else:
            print("⚠️ No suitable match found.")

def main():
    threading.Thread(target=handle_driver_messages, daemon=True).start()
    threading.Thread(target=handle_passenger_messages).start()

if __name__ == "__main__":
    main()

