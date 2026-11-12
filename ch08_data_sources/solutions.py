"""Chapter 8 solutions.  Run: python -m ch08_data_sources.solutions [8.1 ...]"""
import os
import time

from pyspark.sql import functions as F

from common.data import load_items_with_total, load_orders, out_path
from common.session import run_cli


def write_items_partitioned(spark) -> str:
    path = out_path("items_parquet")
    items_dated = (
        load_items_with_total(spark)
        .join(load_orders(spark).select("order_id", "order_date"), "order_id")
        .withColumn("year", F.year("order_date"))
        .withColumn("month", F.month("order_date"))
    )
    # repartition by the partition columns -> few files per partition directory
    (items_dated.repartition("year", "month")
                .write.mode("overwrite")
                .partitionBy("year", "month")
                .parquet(path))
    return path


def ex_8_1(spark):
    path = write_items_partitioned(spark)
    print(spark.read.parquet(path).select("year", "month").distinct().count())   # 18


def _size_mb(path: str) -> float:
    total = sum(os.path.getsize(os.path.join(d, f)) for d, _, files in os.walk(path) for f in files)
    return round(total / 1024 / 1024, 2)


def ex_8_2(spark):
    items = load_items_with_total(spark)
    csv_path, parquet_path = out_path("items_csv"), out_path("items_pq")
    items.write.mode("overwrite").csv(csv_path, header=True)
    items.write.mode("overwrite").parquet(parquet_path)
    print("CSV:", _size_mb(csv_path), "MB | Parquet:", _size_mb(parquet_path), "MB")

    start = time.time()
    spark.read.csv(csv_path, header=True, schema=items.schema).agg(F.sum("quantity")).show()
    print(f"CSV: {time.time() - start:.2f}s")

    start = time.time()
    spark.read.parquet(parquet_path).agg(F.sum("quantity")).show()
    print(f"Parquet: {time.time() - start:.2f}s")


def ex_8_3(spark):
    path = write_items_partitioned(spark)
    q4 = spark.read.parquet(path).filter((F.col("year") == 2025) & (F.col("month") >= 10))
    q4.agg(F.sum("quantity").alias("q4_quantity")).show()
    q4.explain()      # PartitionFilters: [(year = 2025), (month >= 10)]


def ex_8_4(spark):
    small = spark.createDataFrame([(1, "a"), (2, "b"), (3, "c")], "id INT, v STRING")
    path = out_path("save_modes")

    small.write.mode("overwrite").parquet(path)
    print("overwrite:", spark.read.parquet(path).count())       # 3
    small.write.mode("append").parquet(path)
    print("append:", spark.read.parquet(path).count())          # 6
    small.write.mode("ignore").parquet(path)
    print("ignore:", spark.read.parquet(path).count())          # 6
    try:
        small.write.mode("errorifexists").parquet(path)
    except Exception as exc:                                     # AnalysisException
        print("errorifexists ->", type(exc).__name__)


if __name__ == "__main__":
    run_cli({"8.1": ex_8_1, "8.2": ex_8_2, "8.3": ex_8_3, "8.4": ex_8_4}, "ch08-solutions")
