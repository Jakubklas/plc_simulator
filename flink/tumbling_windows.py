from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.window import TumblingProcessingTimeWindows, SlidingProcessingTimeWindows
from pyflink.datastream.window import TumblingEventTimeWindows, SlidingEventTimeWindows
from pyflink.datastream.functions import ProcessWindowFunction
from pyflink.common import Types, Time, WatermarkStrategy
from pyflink.common.watermark_strategy import TimestampAssigner
import time
from datetime import datetime

env = StreamExecutionEnvironment.get_execution_environment()
terminal = env.read_text_file("terminal.txt")
data = [
    ("apple", 1, 1),    # happened at second 1
    ("banana", 1, 2),   # happened at second 2
    ("apple", 1, 3),    # happened at second 3
    ("apple", 1, 5),    # happened at second 5
    ("banana", 1, 7),   # happened at second 7
    ("orange", 1, 8),   # happened at second 7
    ("banana", 1, 9),   # happened at second 7
    ("apple", 1, 9)     # happened at second 7
]

# Creteating env from collection and using 'tup_info' to indicate the types of data
stream = env.from_collection(
    data,
    type_info=Types.TUPLE([Types.STRING(), Types.INT(), Types.LONG()])
)

# Creating a custom class called 'MyTimeStampassigner' inherinting from pyFlink's 'Timestampassigner'
# with the predefined method extract_timestamp(self, value, record_timestamp) returning the timestamp 
# part of my tuple
class MyTimeStampAssigner(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp):
        return value[2]

# Creating the Watermark strategy class with params .for_monotonous_timestamps() and 
# .with_timestamp_assigner('my_ts_class')
wm_strategy = WatermarkStrategy \
    .for_monotonous_timestamps() \
    .with_timestamp_assigner(MyTimeStampAssigner())

# Now assign timestamps to the stream with .assign_timestamps_and_watermarks(my_watermark_strategy)
stream = stream.assign_timestamps_and_watermarks(wm_strategy)


# Creating a stream where we sum the counts each 3 seconds of our timestamps
# we key by the fruit and window by time with .window(TumblingEventTimeWindows.of(Time.seconds(3)))
# then we sum each the counts within each 3 second window

# 3-second tumbling window
windowed = stream \
    .key_by(lambda x: x[0]) \
    .window(TumblingEventTimeWindows.of(Time.seconds(3))) \
    .reduce(lambda a, b: (a[0], a[1]+b[1], max(a[2], b[2])))


print("=== WINDOWED DATA ===")
windowed.print()

env.execute()