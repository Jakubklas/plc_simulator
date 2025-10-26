from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.window import TumblingEventTimeWindows, SlidingProcessingTimeWindows, TumblingProcessingTimeWindows
from pyflink.common import Types, Time, WatermarkStrategy
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream.functions import SourceFunction
import random
import time
import math
import threading

def generator(limit):
    print("="*60)
    print("GENERATOR IS RUNNING")
    print("="*60)
    levels = ["INFO", "WARN", "ERROR"]
    counter = 0
    result = []

    with open("content.txt", "r+") as f:
        f.seek(0)
        f.truncate()
    
    sign_a = "|-O-|"
    sign_b = "O-|-O"

    while True:
        with open("content.txt", "a") as f:
            sin_gap = int(math.sin(float(counter)/10)*25 + 30)
            cos_gap = int(math.cos(float(counter)/10)*25 + 30)
            
            if sin_gap > cos_gap:
                gap_a = cos_gap*" "
                gap_b = (sin_gap-cos_gap)*" "
                f.write(f"{gap_a}{sign_a}{gap_b}{sign_b}\n")
            elif sin_gap == cos_gap:
                f.write(f"{gap_a}X\n")
            else:
                gap_a = sin_gap*" "
                gap_b = (cos_gap-sin_gap)*" "
                f.write(f"{gap_a}{sign_a}{gap_b}{sign_b}\n")

        
        if counter > limit:
            with open("content.txt", "r+") as f:
                lines = f.readlines()
                f.seek(0)
                f.truncate()
                f.writelines(lines[1:])

        time.sleep(0.05)
        counter += 1


generator(60)

# env = StreamExecutionEnvironment.get_execution_environment()
# # env.set_runtime_mode(RuntimeExecutionMode.BATCH)
# # env.set_parallelism(1)



# logs = generator(500)
# stream = env.from_collection(logs,
#                              type_info= Types.TUPLE([Types.LONG(), Types.STRING(), Types.STRING()])
#                              )

# class TSAssigner(TimestampAssigner):
#     def extract_timestamp(self, value, record_timestamp: int) -> int:
#         return value[0]

# wms = WatermarkStrategy.for_monotonous_timestamps().with_timestamp_assigner(TSAssigner())


# stream = stream \
#     .assign_timestamps_and_watermarks(wms) \
#     .map(lambda x: (x[0], x[1], x[2], 1)) \
#     .key_by(lambda x: x[1]) \
#     .window(TumblingEventTimeWindows.of(Time.seconds(3))) \
#     .reduce(lambda a, b: (max(a[0],b[0]), a[1], a[2], a[3]+b[3])) \
#     .map(lambda x: f"{x[1]} | {x[3]} | {x[0]}")

# stream.print()
# env.execute()
