"""Chapter 5 - DataFrame API.  Run: python -m ch05_dataframe.examples"""
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType, TimestampType

from common.data import data_path, load_order_items, load_orders, load_products
from common.session import run_cli


def reading_with_schema(spark):
    # Option 1: DDL string
    items = spark.read.csv(
        data_path("order_items.csv"), header=True,
        schema="order_id INT, product_id INT, quantity INT, unit_price DOUBLE, discount DOUBLE",
    )
    # Option 2: StructType
    orders_schema = StructType([
        StructField("order_id", IntegerType(), False),
        StructField("customer_id", IntegerType()),
        StructField("order_date", TimestampType()),
        StructField("status", StringType()),
        StructField("payment_method", StringType()),
    ])
    orders = spark.read.csv(data_path("orders.csv"), header=True, schema=orders_schema)
    # JSON Lines: Spark can infer the schema
    products = spark.read.json(data_path("products.json"))

    orders.printSchema()
    orders.show(5)
    items.show(3)
    products.show(3, truncate=False)


def select_filter_with_column(spark):
    orders, items = load_orders(spark), load_order_items(spark)
    (orders
        .select("order_id", "customer_id", "status")
        .filter((F.col("status") == "COMPLETED") & (F.col("customer_id") != 1))
        .show(3))

    items = items.withColumn(
        "line_total", F.round(F.col("quantity") * F.col("unit_price") * (1 - F.col("discount")))
    ).withColumn(
        "has_discount", F.when(F.col("discount") > 0, "yes").otherwise("no")
    )
    items.show(3)


def aggregation(spark):
    orders = load_orders(spark)
    (orders
        .groupBy("payment_method")
        .agg(
            F.count("*").alias("num_orders"),
            F.sum(F.when(F.col("status") == "CANCELLED", 1).otherwise(0)).alias("num_cancelled"),
        )
        .withColumn("cancel_rate", F.round(F.col("num_cancelled") / F.col("num_orders"), 3))
        .orderBy(F.desc("num_orders"))
        .show())


def joins(spark):
    orders, items, products = load_orders(spark), load_order_items(spark), load_products(spark)
    order_details = (
        orders.join(items, on="order_id", how="inner")
              .join(products.select("product_id", "product_name", "category"),
                    on="product_id", how="left")
    )
    order_details.select("order_id", "product_name", "category", "quantity").show(3, truncate=False)


def cleaning(spark):
    dirty = spark.createDataFrame(
        [(1, "  HUẾ "), (2, None), (2, None), (3, "Huế")],
        "id INT, city STRING",
    )
    clean = (
        dirty.dropDuplicates(["id"])
             .withColumn("city", F.initcap(F.trim(F.col("city"))))
             .fillna({"city": "Unknown"})
    )
    clean.show()


if __name__ == "__main__":
    run_cli({"5.2": reading_with_schema, "5.3": select_filter_with_column,
             "5.4": aggregation, "5.5": joins, "5.6": cleaning}, "ch05-examples")
