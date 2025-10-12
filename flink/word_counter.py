from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.table import StreamTableEnvironment
from pyflink.table.expressions import col
from pyflink.common.typeinfo import Types
import random

env = StreamExecutionEnvironment.get_execution_environment()
env.set_runtime_mode(RuntimeExecutionMode.BATCH)
env.set_parallelism(1)
table_env = StreamTableEnvironment.create(env)

terminal_text = env.read_text_file("terminal.txt", charset_name="UTF-8")

# Build a leaderboard of word occurences
word_count = terminal_text \
    .flat_map(lambda line: line.lower().split()) \
    .map(lambda x: x.strip()) \
    .filter(lambda x: len(x)>2) \
    .filter(lambda x: x not in ["jakubklas@jakubs-macbook-pro"]) \
    .map(lambda x: (x, 1), output_type=Types.TUPLE([Types.STRING(), Types.INT()])) \
    .key_by(lambda x: x[0]) \
    .reduce(lambda a, b: (a[0], a[1] + b[1]))

table = table_env.from_data_stream(word_count)
table_env.create_temporary_view("word_table", table)

top_10 = table_env.sql_query(
    """
    SELECT 
        f0
        , f1
    FROM word_table
    ORDER BY f1 DESC
    LIMIT 25;
    """
)

print("Table schema:")
print(table.get_schema())

top_10.execute().print()

