from pyflink.datastream import StreamExecutionEnvironment
import random

env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)

text_stream = env.read_text_file("terminal.txt", charset_name="UTF-8")

stream = text_stream \
    .map(lambda x: {
        "line_len": len(x),             # Characters
        "words": len(x.split()),        # Words
        "counter": 1                    # Line count
    }) \
    .map(lambda x: ("global", x)) \
    .key_by(lambda x:x[0]) \
    .reduce(lambda a, b: (
        "global",
        {
            "line_len": a[1]["line_len"] + b[1]["line_len"],
            "words": a[1]["words"] + b[1]["words"],    
            "counter": a[1]["counter"] + b[1]["counter"]
        }
    )) \
    .map(lambda x: x[1]) \
    .map(lambda x: {
        "avg_line_len": x["line_len"]/x["counter"],
        "avg_words": x["words"]/x["counter"],
        "lines": x["counter"]
    })

with open("terminal.txt", "r") as f:
    print(len(f.read()))

stream.print()
env.execute("Doubling amounts.")

# def create_txt_file():
#     with open("text_file.txt", "w") as f:
#         while True:
#             f.write(str(random.randint(-50, 50)))
#             f.write(",")

# if __name__ == "__main__":
#     # create_txt_file()
