from kafka import KafkaProducer
import json
import time
import random

# Creating the Kafka producer
producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

print("Starting a continuouse Kafka producer on localhost...")
print("Press control+C to stop")


counter=0
try:
    while True:
        event = {
            "timestamp": int(time.time()*1000),
            "sensor": random.choice(["THERM", "BAR", "VELO", "MOIST"]),
            "value": round(random.uniform(20.0, 100.0), 2),
            "count": counter+1
        }

        print("Sending event to Kafka...")
        producer.send(
            topic="readings",
            value=event
            )
        
        counter += 1
        print(f"{counter} | Event sent | {event}")
        time.sleep(0.05)
            


except KeyboardInterrupt:
    print("\nStopping producer...")
finally:
    producer.close()