"""Chapter 12 (exercise 12.1) - a standalone batch job.

spark-submit --master "local[*]" ch12_deployment/jobs/monthly_revenue.py \
    --input data --output output/gold

Note: the job never sets the master; that is decided at submit time.
"""
import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

ORDERS_SCHEMA = "order_id INT, customer_id INT, order_date TIMESTAMP, status STRING, payment_method STRING"
ORDER_ITEMS_SCHEMA = "order_id INT, product_id INT, quantity INT, unit_price DOUBLE, discount DOUBLE"


def add_line_total(items: DataFrame) -> DataFrame:
    return items.withColumn(
        "line_total",
        F.round(F.col("quantity") * F.col("unit_price") * (1 - F.col("discount"))),
    )


def monthly_revenue(orders: DataFrame, items: DataFrame) -> DataFrame:
    """Pure DataFrame -> DataFrame logic: easy to unit test."""
    return (
        orders.filter(F.col("status") == "COMPLETED")
        .join(add_line_total(items), "order_id")
        .groupBy(F.date_format("order_date", "yyyy-MM").alias("month"))
        .agg(F.countDistinct("order_id").alias("num_orders"), F.sum("line_total").alias("revenue"))
    )


def main(input_dir: str, output_dir: str) -> None:
    spark = SparkSession.builder.appName("monthly-revenue").getOrCreate()
    orders = spark.read.csv(f"{input_dir}/orders.csv", header=True, schema=ORDERS_SCHEMA)
    items = spark.read.csv(f"{input_dir}/order_items.csv", header=True, schema=ORDER_ITEMS_SCHEMA)
    (monthly_revenue(orders, items)
        .coalesce(1)                      # the result has only 18 rows
        .write.mode("overwrite").parquet(f"{output_dir}/monthly_revenue"))
    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    main(args.input, args.output)
