from kafka import KafkaProducer
import json
import time
import random

# Setup producer on localhost:9092 w/ a lambda json serializer to utf-8
kafka = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda x: json.dumps(x).encode("utf-8")
)
counter = 0

# Run while loop with a smooth KeyboardInterrupt handling
try:
    while True:
        # Create the dictionary
        readings = {
            "sensor_id": random.choice(["therm", "velo", "mois", "press"]),
            "area": random.choice(["line_1", "line_2"]),
            "value": float(random.uniform(12.0, 95.0)),
            "timestamp": int(time.time()*1000)
        }

        # Send it to a kafka topic "readings"
        kafka.send(
            topic="readings",
            value=readings
        )
        print(f"{counter+1} --> {readings}")
        
        # Sleep for a bit & count
        counter += 1
        time.sleep(0.02)

# Close the connections
except KeyboardInterrupt:
    print("Kafka producer was stopped...")
finally:
    kafka.close()



