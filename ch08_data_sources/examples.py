"""Chapter 8 - reading and writing data.  Run: python -m ch08_data_sources.examples"""
from pyspark.sql import functions as F

from common.data import load_orders, out_path
from common.session import run_cli


def _orders_with_partitions(spark):
    return (load_orders(spark)
            .withColumn("year", F.year("order_date"))
            .withColumn("month", F.month("order_date")))


def partitioned_write(spark):
    (_orders_with_partitions(spark).write
        .mode("overwrite")
        .partitionBy("year", "month")
        .parquet(out_path("orders_parquet")))
    print("Written to", out_path("orders_parquet"))


def partition_pruning(spark):
    partitioned_write(spark)
    march = spark.read.parquet(out_path("orders_parquet")).filter(
        (F.col("year") == 2025) & (F.col("month") == 3)
    )
    print(march.count())
    march.explain()      # look for PartitionFilters in the scan node


def tables(spark):
    _orders_with_partitions(spark).write.mode("overwrite").saveAsTable("orders_tbl")
    spark.sql("SELECT status, COUNT(*) AS n FROM orders_tbl GROUP BY status").show()


if __name__ == "__main__":
    run_cli({"8.2": partitioned_write, "8.3": partition_pruning, "8.4": tables}, "ch08-examples")
