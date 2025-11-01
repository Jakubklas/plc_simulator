from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
from pyflink.common.serialization import SimpleStringSchema, Encoder
from pyflink.datastream.window import TumblingProcessingTimeWindows, TumblingEventTimeWindows
from pyflink.common.watermark_strategy import TimestampAssigner, WatermarkStrategy
from pyflink.datastream.connectors.file_system import FileSink, OutputFileConfig, RollingPolicy
from pyflink.datastream.connectors.jdbc import JdbcSink, JdbcConnectionOptions, JdbcExecutionOptions

from pyflink.common import Types, Time, Row
import json
from db import setup_postgres

# Set up StreamExecutionEnvironment & set paralllelism to 1
env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)
env.enable_checkpointing(10000)

# Add Kafka JAR file location (starting with "file://") -> loads java classes
env.add_jars(
    "file:///Users/jakubklas/factory_readings/flink-sql-connector-kafka-3.1.0-1.18.jar",     # Kafka JAR
    "file:///Users/jakubklas/factory_readings/postgresql-42.7.1.jar",                        # PostGres JAR
    "file:///Users/jakubklas/factory_readings/flink-connector-jdbc-3.1.2-1.18.jar"           # JDBC Conector JAR
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

# Create a DataBase sink config

jdbc_sink = JdbcSink.sink(
    "INSERT INTO sensor_data (area, sensor_id, avg_value, count, timestamp) VALUES (?, ?, ?, ?, ?)",
    Types.ROW([Types.STRING(), Types.STRING(), Types.DOUBLE(), Types.INT(), Types.LONG()]),
    JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
        .with_url("jdbc:postgresql://localhost:5432/readings")
        .with_driver_name("org.postgresql.Driver")
        .with_user_name("postgres")
        .with_password("12345")
        .build(),

    # Execution Options are 'optional', BUT if not present Flink defaults to batch size 5,000
    # meaning data sits in memory until 5K reocrds arrive - may be too long
    JdbcExecutionOptions.builder()
        .with_batch_size(100)
        .with_batch_interval_ms(1000)
        .with_max_retries(3)
        .build()
)

# Create DB and DB Table if they don;t exist
setup_postgres()

# Streaming data as 'Row' (actual metadata) instead of Tuple (python native)
stream = stream \
    .map(lambda x: parse_json(x)) \
    .map(lambda x: Row(x[1], x[0], float(x[2]), 1, int(x[3])),
         output_type=Types.ROW([Types.STRING(), Types.STRING(), Types.DOUBLE(), Types.INT(), Types.LONG()]))

# Add JDBC sink 
stream.add_sink(jdbc_sink)
print("="*60)
print("STREAM RUNNING. SINKNG DATA INTO A DATABASE")
print("="*60)

try:
    env.execute()
except KeyboardInterrupt:
    print("Strem closed...")
finally:
    env.close()


    