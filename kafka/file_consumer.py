from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
from pyflink.common.serialization import SimpleStringSchema, Encoder
from pyflink.datastream.window import TumblingProcessingTimeWindows, TumblingEventTimeWindows
from pyflink.common.watermark_strategy import TimestampAssigner, WatermarkStrategy
from pyflink.datastream.connectors.file_system import FileSink, OutputFileConfig, RollingPolicy
from pyflink.common import Types, Time
import json

# Set up StreamExecutionEnvironment & set paralllelism to 1
env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)
env.enable_checkpointing(1000 * 10)     # Check-pooint every 10 seconds

# Add Kafka JAR file location (starting with "file://") -> loads java classes
env.add_jars(
    "file:///Users/jakubklas/factory_readings/flink-sql-connector-kafka-3.1.0-1.18.jar",     # Kafka JAR
    
)

# Create Kafka props dict & the KafkaConsumer class w/ SimpleStringSchema()
consumer_props = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "flink-consumer"
}

consumer = FlinkKafkaConsumer(
    topics="readings",
    deserialization_schema=SimpleStringSchema(),
    properties=consumer_props
)

# Set the start from earliest record w/ consumer.set_start_from_earliest()
consumer.set_start_from_latest()

# Creat the stream by adding kafka as a source to env.add_source(consumer)
stream = env.add_source(consumer)

# Write a JSON parsing function to return e.g. TUPLES
def parse_json(msg):
    msg = json.loads(msg)
    return (msg["sensor_id"], msg["area"], msg["value"], msg["timestamp"])

# Create a ts assigner class
class TsAssigner(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp: int) -> int:
        return value[3]

# Create a wm strategy
wm = WatermarkStrategy \
    .for_monotonous_timestamps()\
    .with_timestamp_assigner(TsAssigner())

# Start streaming and handle closures gracefully

stream = stream \
    .map(lambda x: parse_json(x)) \
    .assign_timestamps_and_watermarks(wm) \
    .map(lambda x: (x[0], x[1], x[2], x[3], 1)) \
    .filter(lambda x: x is not None) \
    .key_by(lambda x: x[1]) \
    .window(TumblingEventTimeWindows.of(size=Time.seconds(3))) \
    .reduce(lambda a, b:
            (a[0], a[1], a[2], max(a[3],b[3]), a[4]+b[4])
            ) \
    .map(lambda x: (x[0], x[1], x[2]/x[4], x[3], x[4])) \
    .map(lambda x: f"{x[1]} | {x[0]} | Avg. Value: {round(x[2], 2)} | Count: {x[4]} | Ts: {x[3]}",
            output_type=Types.STRING())

file_sink = FileSink \
    .for_row_format(
        base_path="/Users/jakubklas/factory_readings/xxx_content",
        encoder=Encoder.simple_string_encoder("UTF-8")
        ) \
    .with_output_file_config(
        OutputFileConfig.builder() \
            .with_part_suffix(".json") \
            .build()
    ) \
    .build()

# Stream to sink
if stream.sink_to(file_sink):
    print("="*60)
    print("STREAM RUNNING. SINKNG DATA INTO A TEXT FILE")
    print("="*60)

try:
    stream.print()
    env.execute()
except KeyboardInterrupt:
    print("Strem closed...")
finally:
    env.close()


    