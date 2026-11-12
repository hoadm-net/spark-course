"""Chapter 2 solutions.  Run: python -m ch02_setup.solutions [2.1 2.2]"""
from common.data import data_path
from common.session import run_cli


def ex_2_1(spark):
    print("Spark version:", spark.version)
    print("Default parallelism:", spark.sparkContext.defaultParallelism)


def ex_2_2(spark):
    # Line counts include the header line.
    for name in ["customers", "orders", "order_items"]:
        print(name, spark.read.text(data_path(f"{name}.csv")).count())


if __name__ == "__main__":
    run_cli({"2.1": ex_2_1, "2.2": ex_2_2}, "ch02-solutions")
