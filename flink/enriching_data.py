from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.window import TumblingProcessingTimeWindows, SlidingProcessingTimeWindows
from pyflink.datastream.functions import ProcessWindowFunction, ProcessFunction
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


tumbling_word_count.print()
env.execute()