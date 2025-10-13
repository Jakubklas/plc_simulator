from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.window import TumblingProcessingTimeWindows, SlidingProcessingTimeWindows
from pyflink.datastream.window import TumblingEventTimeWindows, SlidingEventTimeWindows
from pyflink.datastream.functions import ProcessWindowFunction, ProcessFunction
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.table import StreamTableEnvironment
from pyflink.table.expressions import col
from pyflink.common import Types, Time, WatermarkStrategy
import random
import time

env = StreamExecutionEnvironment.get_execution_environment()
env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
env.set_parallelism(1)

terminal_text = env.read_text_file("terminal.txt", charset_name="UTF-8")

# Parsing function for terminal.txt and add time-stamps
def parse_with_timestamps(line):
    words = line.lower().split()
    current_ts = int(time.time() * 1000)        # Current time in miliseconds
    for word in words:
        word.strip()
        if len(word) > 2:
            yield (word, 1, current_ts)

# Apply the funcion as a .flat_map (one to many)
words_with_ts = terminal_text \
    .flat_map(
        parse_with_timestamps,
        output_type=Types.TUPLE([Types.STRING(), Types.INT(), Types.LONG()])
    )

tumbling_word_count = words_with_ts \
    .key_by(lambda x: x[0]) \
    .window(TumblingProcessingTimeWindows.of(Time.seconds(2))) \
    .reduce(lambda a, b: (a[0], a[1]+b[1], max(a[2], b[2])))


print("\n=== TUMBLING WINDOW (2 seconds) ===")
tumbling_word_count.print()

sliding_word_count = words_with_ts \
    .key_by(lambda x: x[0]) \
    .window(SlidingProcessingTimeWindows.of(Time.seconds(2), Time.seconds(5))) \
    .reduce(lambda a, b: (a[0], a[1] + b[1], max(a[2], b[2])))

print("\n=== SLIDING WINDOW (5 sec slide each 2 sec) ===")
sliding_word_count.print()


print("\n=== TUMBLING EVENT WINDOW (20 miliseconds) ===")
tumbling_word_count.print()


class EventTimeStamps(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp):
        return value[2]

watermark_strategy = WatermarkStrategy \
    .for_monotonous_timestamps() \
    .with_timestamp_assigner(EventTimeStamps())

words_with_event_time = words_with_ts.assign_timestamps_and_watermarks(watermark_strategy)

tumbling_word_count = words_with_event_time \
    .key_by(lambda x: x[0]) \
    .window(TumblingEventTimeWindows.of(Time.milliseconds(20))) \
    .reduce(lambda a, b: (a[0], a[1] + b[1], max(a[2], b[2])))

print("\n=== TUMBLING EVENT TIME WINDOW (5 sec window, 2 sec slide) ===")
tumbling_word_count.print()

sliding_word_count = words_with_event_time \
    .key_by(lambda x: x[0]) \
    .window(SlidingEventTimeWindows.of(Time.milliseconds(100), Time.milliseconds(20))) \
    .reduce(lambda a, b: (a[0], a[1] + b[1], max(a[2], b[2])))

print("\n=== SLIDING EVENT TIME WINDOW (5 sec window, 2 sec slide) ===")
sliding_word_count.print()