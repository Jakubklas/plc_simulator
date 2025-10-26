from py4j.java_gateway import JavaObject
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.window import TumblingProcessingTimeWindows, SlidingProcessingTimeWindows
from pyflink.datastream.window import TumblingEventTimeWindows, SlidingEventTimeWindows
from pyflink.datastream.functions import ProcessWindowFunction
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common import Types, Time, WatermarkStrategy
from pyflink.common.watermark_strategy import TimestampAssigner
import time
import random 
from datetime import datetime
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

    while True:
        with open("content.txt", "a") as f:
            ts = int(time.time()*1000)
            level = random.choice(levels)
            message = f"Message no. {counter+1}"
            f.write(f"{level} | {ts} | {message}\n")
        
        if counter > limit:
            with open("content.txt", "r+") as f:
                lines = f.readlines()
                f.seek(0)
                f.truncate()
                f.writelines(lines[1:])

        time.sleep(0.05)
        counter += 1

generator_thread = threading.Thread(target=generator, args=(200, ))
generator_thread.daemon = False
generator_thread.start()

for i in range(1, 10):
    print(f"Sleeping {i} seconds...")
    time.sleep(1)
print("Looping..")