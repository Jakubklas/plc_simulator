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

stream = terminal \
    .map(lambda x: x.lower()) \
    .map(lambda x: (
            "DOCKER" if "docker" in x else
            "NAVIGATION" if "ls" in x else
            "NAVIGATION" if "cd" in x else
            "OTHER",
            1,
            int(time.time()*1000)
            ),
        output_type= Types.TUPLE([Types.STRING(), Types.INT(), Types.LONG()])
        )

class CustomAssigner(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp):
        return value[2]
    
watermark_str = WatermarkStrategy \
    .for_monotonous_timestamps() \
    .with_timestamp_assigner(CustomAssigner())

stream = stream.assign_timestamps_and_watermarks(watermark_str)

tumbl_count = stream \
    .key_by(lambda x: x[0]) \
    .window(TumblingEventTimeWindows.of(Time.milliseconds(2))) \
    .reduce(lambda a, b: (a[0], a[1]+b[1], max(a[2], b[2])))

tumbl_count.print()

sliding_count = stream \
    .key_by(lambda x: x[0]) \
    .window(SlidingEventTimeWindows.of(Time.milliseconds(2), Time.milliseconds(10))) \
    .reduce(lambda a, b: (a[0], a[1]+b[1], max(a[2], b[2])))

sliding_count.print()

env.execute()