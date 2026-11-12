"""Chapter 3 - partitions, lazy evaluation, jobs and stages.

Run: python -m ch03_execution_model.examples
"""
from pyspark.sql import functions as F

from common.session import run_cli


def partitions(spark):
    df = spark.range(0, 1_000_000)
    print("Default partitions:", df.rdd.getNumPartitions())
    print("After repartition(8):", df.repartition(8).rdd.getNumPartitions())


def lazy_evaluation(spark):
    df = spark.range(0, 1_000_000)
    a = df.withColumn("k", F.col("id") % 10)    # transformation: nothing runs
    b = a.filter(F.col("id") > 100)             # still nothing
    c = b.groupBy("k").count()                  # still nothing
    c.show()                                    # action: the whole chain runs now
    c.explain()                                 # look for Exchange = shuffle boundary


if __name__ == "__main__":
    run_cli({"3.2": partitions, "3.3": lazy_evaluation}, "ch03-examples")
