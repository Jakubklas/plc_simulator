from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.window import TumblingProcessingTimeWindows
from pyflink.common import Types, Time
import json

# Set up the env and the kafka consumer
env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)

# Add Kafka connection JAR
env.add_jars("file:///Users/jakubklas/factory_readings/kafka/flink-sql-connector-kafka-3.1.0-1.18.jar")

# Create Kafka consumer
consumer_props = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "flink-consumer"
}

kafka_consumer = FlinkKafkaConsumer(
    topics="readings",
    deserialization_schema=SimpleStringSchema(),
    properties=consumer_props
)

# Begin reading messagees fromt he earlies available one
kafka_consumer.set_start_from_earliest()

# Initialize the stream
stream = env.add_source(kafka_consumer)

# Parser function
def parse_json(msg):
    msg = json.loads(msg)
    return (msg["timestamp"], msg["sensor"], msg["value"], 1)

stream = stream \
    .map(lambda x: parse_json(x)) \
    .filter(lambda x: x is not None) \
    .key_by(lambda x: x[1]) \
    .window(TumblingProcessingTimeWindows.of(Time.seconds(2))) \
    .reduce(lambda a, b: (
        max(a[0], b[0]),        # Timestamp
        a[1],                   # Sensor
        a[2] + b[2],            # Sum of values
        a[3] + b[3]             # Counter
    )) \
    .map(lambda x: (
        x[0],                   # Timestamp
        x[1],                   # Sensor
        x[2] / x[3],            # Average value
        x[3]                    # Counter
    )) \
    .map(lambda x: f"{x[0]} | Sensor: {x[1]} | Avg Value: {x[2]:.2f} | Count: {x[3]}") \

stream.print()
try:
    env.execute()
except KeyboardInterrupt:
    print("\nConsumer stopped.\n")
finally:
    env.close()