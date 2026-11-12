"""Chapter 3 solutions.  Run: python -m ch03_execution_model.solutions [3.2 3.3]"""
from pyspark.sql import functions as F

from common.data import data_path
from common.session import run_cli


def ex_3_2(spark):
    # Reading a CSV header already triggers a tiny job (Spark reads the first line).
    orders = spark.read.csv(data_path("orders.csv"), header=True)

    # (a) transformations only: no query appears in the Spark UI
    completed = orders.filter(F.col("status") == "COMPLETED").select("order_id", "status")

    # (b) an action
    print(completed.count())

    # (c) an action that needs a shuffle
    orders.groupBy("status").count().show()


def ex_3_3(spark):
    orders = spark.read.csv(data_path("orders.csv"), header=True)
    print("Initial partitions:", orders.rdd.getNumPartitions())

    repartitioned = orders.repartition(8)
    repartitioned.groupBy(F.spark_partition_id().alias("pid")).count().orderBy("pid").show()

    coalesced = repartitioned.coalesce(2)
    coalesced.groupBy(F.spark_partition_id().alias("pid")).count().orderBy("pid").show()


if __name__ == "__main__":
    run_cli({"3.2": ex_3_2, "3.3": ex_3_3}, "ch03-solutions")
