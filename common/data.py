"""Schemas, loaders and reusable transformations for the VietMart dataset."""
import os
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

DATA_DIR = Path(os.environ.get("VIETMART_DATA", "data"))
OUTPUT_DIR = Path(os.environ.get("VIETMART_OUTPUT", "output"))

CUSTOMERS_SCHEMA = "customer_id INT, full_name STRING, city STRING, signup_date DATE, segment STRING"
PRODUCTS_SCHEMA = "product_id INT, product_name STRING, category STRING, price DOUBLE"
ORDERS_SCHEMA = "order_id INT, customer_id INT, order_date TIMESTAMP, status STRING, payment_method STRING"
ORDER_ITEMS_SCHEMA = "order_id INT, product_id INT, quantity INT, unit_price DOUBLE, discount DOUBLE"
EVENTS_SCHEMA = "event_time TIMESTAMP, user_id INT, event_type STRING, product_id INT"


def data_path(name: str) -> str:
    return str(DATA_DIR / name)


def out_path(name: str) -> str:
    return str(OUTPUT_DIR / name)


def load_customers_raw(spark: SparkSession) -> DataFrame:
    return spark.read.csv(data_path("customers.csv"), header=True, schema=CUSTOMERS_SCHEMA)


def load_products(spark: SparkSession) -> DataFrame:
    return spark.read.schema(PRODUCTS_SCHEMA).json(data_path("products.json"))


def load_orders(spark: SparkSession) -> DataFrame:
    return spark.read.csv(data_path("orders.csv"), header=True, schema=ORDERS_SCHEMA)


def load_order_items(spark: SparkSession) -> DataFrame:
    return spark.read.csv(data_path("order_items.csv"), header=True, schema=ORDER_ITEMS_SCHEMA)


def clean_customers(customers_raw: DataFrame) -> DataFrame:
    """Deduplicate, normalize city names and fill missing cities (exercise 5.2)."""
    return (
        customers_raw
        .dropDuplicates(["customer_id"])
        .withColumn("city", F.initcap(F.trim(F.col("city"))))
        .fillna({"city": "Unknown"})
    )


def with_line_total(items: DataFrame) -> DataFrame:
    """Add line_total = quantity * unit_price * (1 - discount) (exercise 5.3)."""
    return items.withColumn(
        "line_total",
        F.round(F.col("quantity") * F.col("unit_price") * (1 - F.col("discount"))),
    )


def order_totals(items_with_total: DataFrame) -> DataFrame:
    """One row per order: number of lines, total quantity, order value."""
    return items_with_total.groupBy("order_id").agg(
        F.count("*").alias("num_lines"),
        F.sum("quantity").alias("total_quantity"),
        F.sum("line_total").alias("order_value"),
    )


def load_customers(spark: SparkSession) -> DataFrame:
    return clean_customers(load_customers_raw(spark))


def load_items_with_total(spark: SparkSession) -> DataFrame:
    return with_line_total(load_order_items(spark))


def completed_orders(orders: DataFrame) -> DataFrame:
    return orders.filter(F.col("status") == "COMPLETED")


def monthly_revenue(orders: DataFrame, totals: DataFrame) -> DataFrame:
    """Revenue of COMPLETED orders per month (exercise 5.4)."""
    return (
        completed_orders(orders)
        .join(totals, "order_id")
        .groupBy(F.date_format("order_date", "yyyy-MM").alias("month"))
        .agg(F.count("*").alias("num_orders"), F.sum("order_value").alias("revenue"))
        .orderBy("month")
    )
