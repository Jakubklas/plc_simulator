from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.window import TumblingEventTimeWindows, SlidingProcessingTimeWindows, TumblingProcessingTimeWindows
from pyflink.common import Types, Time, WatermarkStrategy
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream.functions import SourceFunction
import random
import time
import math
import threading


# Execution environment
env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)

# Custom source to continuously read overwritten file
class FilePollingSource:
    def __init__(self, file_path):
        self.file_path = file_path
        self.running = True
        
    def poll_file(self):
        seen_lines = set()
        while self.running:
            try:
                with open(self.file_path, 'r') as f:
                    lines = f.readlines()
                    for line in lines:
                        line = line.strip()
                        if line and line not in seen_lines:
                            seen_lines.add(line)
                            yield line
                time.sleep(0.05)  # Poll every 50ms to match generation rate
            except FileNotFoundError:
                time.sleep(0.1)
                continue
    
    def stop(self):
        self.running = False

# Create continuous streaming source
import threading
import queue

class ContinuousFileStreamSource:
    def __init__(self, file_path):
        self.file_path = file_path
        self.running = True
        self.data_queue = queue.Queue()
        
    def start_streaming(self):
        def stream_worker():
            seen_lines = set()
            while self.running:
                try:
                    with open(self.file_path, 'r') as f:
                        lines = f.readlines()
                        # Get last 10 lines to keep it manageable
                        recent_lines = lines[-10:] if len(lines) > 10 else lines
                        
                        for line in recent_lines:
                            line = line.strip()
                            if line and line not in seen_lines:
                                seen_lines.add(line)
                                self.data_queue.put(line)
                                
                    time.sleep(0.1)  # Poll every 100ms
                except FileNotFoundError:
                    time.sleep(0.5)
                    continue
        
        # Start background streaming thread
        self.stream_thread = threading.Thread(target=stream_worker)
        self.stream_thread.daemon = False
        self.stream_thread.start()
        
    def get_streaming_data(self):
        # Collect data as it arrives
        collected = []
        timeout_count = 0
        
        while self.running and timeout_count < 50:  # Max 5 seconds timeout
            try:
                line = self.data_queue.get(timeout=0.1)
                collected.append(line)
                if len(collected) >= 20:  # Collect 20 lines for processing
                    break
            except queue.Empty:
                timeout_count += 1
                continue
                
        return collected
        
    def stop(self):
        self.running = False
        if hasattr(self, 'stream_thread'):
            self.stream_thread.join(timeout=2)

# Create and start continuous streaming
print("Starting continuous file streaming...")
file_streamer = ContinuousFileStreamSource("content.txt")
file_streamer.start_streaming()

# Collect initial batch of data
print("Collecting streaming data...")
streaming_data = file_streamer.get_streaming_data()
print(f"Collected {len(streaming_data)} lines for processing")

if streaming_data:
    print(f"Sample line: {streaming_data[0]}")

# Create stream from streaming data
stream = env.from_collection(streaming_data, type_info=Types.STRING())

# Timestamp watermarking strategy
class TSAssigner(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp: int) -> int:
        return value[1]

wms = WatermarkStrategy.for_monotonous_timestamps().with_timestamp_assigner(TSAssigner())

# Parsing the logs
def parse_logs(line):
    objs = [l.strip() for l in line.split("|")]
    if len(objs) == 3:
        lvl = objs[0]
        ts = int(objs[1])
        msg = objs[2]
        return (lvl, ts, msg, 1)
    else:
        return None

# Stream and count sliding windows
stream = stream \
    .map(lambda x: parse_logs(x)) \
    .filter(lambda x: x != None) \
    .assign_timestamps_and_watermarks(wms) \
    .key_by(lambda x: x[0]) \
    .window(TumblingProcessingTimeWindows.of(Time.seconds(2))) \
    .reduce(lambda a, b: (
        a[0],
        max(a[1],b[1]),
        a[2],
        a[3]+b[3])
        ) \
    .map(lambda x: f"{x[0]} | Count: {x[3]} | TS: {x[1]}")

stream.print()

# Execute Flink job in background thread
print("Starting Flink job...")
flink_thread = threading.Thread(target=env.execute)
flink_thread.start()

# Keep main thread alive until user interruption
try:
    print("Streaming continuously... Press Ctrl+C to stop")
    while file_streamer.running:
        time.sleep(1)
        # Optional: Print status
        if not flink_thread.is_alive():
            print("Flink job completed")
            break
            
except KeyboardInterrupt:
    print("\nUser interrupted - stopping stream...")
finally:
    # Clean shutdown
    file_streamer.stop()
    print("Stream stopped")
    
    # Wait for threads to finish
    if flink_thread.is_alive():
        flink_thread.join(timeout=5)
    
    print("Cleanup completed")
