from pyflink.datastream import StreamExecutionEnvironment

env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)

# Stream of numbers
transactions = env.from_collection([
    {"user": "Alice", "amount": 100},
    {"user": "Bob", "amount": 50},
    {"user": "Alice", "amount": 200},
    {"user": "Charlie", "amount": 30},
    {"user": "Bob", "amount": 150},
    {"user": "Alice", "amount": 50}
])

# Double each number
operation_1 = transactions \
    .filter(lambda x: x["amount"] > 40) \
    .map(lambda x: (x["user"], x["amount"])) \
    .key_by(lambda x: x[0]) \
    .reduce(lambda a, b: (a[0], a[1]+b[1]))

operation_2 = operation_1 \
    .map(lambda x: ("Rita", x[1]) if x[0]=="Alice" else (x[0], x[1]))

operation_2.print()
env.execute("Double Numbers")



# flat_map(lambda x: x, y z) --> turns every x into x, y and z. Turns one element into one or more elements