"""Chapter 13 - capstone: a bronze / silver / gold pipeline for VietMart.

Run from the repository root:  python -m ch13_capstone.pipeline
Every step writes with mode("overwrite"), so the pipeline is idempotent.
"""
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from common.data import (CUSTOMERS_SCHEMA, ORDER_ITEMS_SCHEMA, ORDERS_SCHEMA, PRODUCTS_SCHEMA,
                         data_path)
from common.session import get_spark

LAKE = "lake"
SOURCES = {
    "customers":   ("csv", CUSTOMERS_SCHEMA),
    "products":    ("json", PRODUCTS_SCHEMA),
    "orders":      ("csv", ORDERS_SCHEMA),
    "order_items": ("csv", ORDER_ITEMS_SCHEMA),
}


def read_layer(spark: SparkSession, layer: str, table: str) -> DataFrame:
    return spark.read.parquet(f"{LAKE}/{layer}/{table}")


def write_layer(df: DataFrame, layer: str, table: str, partition_by: str | None = None) -> None:
    writer = df.write.mode("overwrite")
    if partition_by:
        writer = writer.partitionBy(partition_by)
    writer.parquet(f"{LAKE}/{layer}/{table}")


# ---------- BRONZE: raw data, only converted to Parquet ----------
def bronze(spark: SparkSession) -> None:
    for table, (fmt, schema) in SOURCES.items():
        reader = spark.read.schema(schema)
        df = (reader.csv(data_path(f"{table}.csv"), header=True) if fmt == "csv"
              else reader.json(data_path(f"{table}.json")))
        write_layer(df.withColumn("_ingested_at", F.current_timestamp()), "bronze", table)


# ---------- SILVER: cleaned, typed, deduplicated ----------
def check_quality(customers: DataFrame, orders: DataFrame, items: DataFrame) -> None:
    def is_unique(df: DataFrame, key: str) -> bool:
        return df.count() == df.select(key).distinct().count()

    errors = []
    if not is_unique(customers, "customer_id"):
        errors.append("duplicate customer_id")
    if not is_unique(orders, "order_id"):
        errors.append("duplicate order_id")
    if not items.join(orders, "order_id", "left_anti").isEmpty():
        errors.append("order items without a parent order")
    if not items.filter(F.col("quantity") <= 0).isEmpty():
        errors.append("non-positive quantity")
    if errors:
        raise ValueError("Silver quality checks failed: " + "; ".join(errors))


def silver(spark: SparkSession) -> None:
    customers = (
        read_layer(spark, "bronze", "customers").drop("_ingested_at")
        .dropDuplicates(["customer_id"])
        .withColumn("city", F.initcap(F.trim("city")))
        .fillna({"city": "Unknown"})
    )
    orders = (
        read_layer(spark, "bronze", "orders").drop("_ingested_at")
        .withColumn("order_month", F.date_format("order_date", "yyyy-MM"))
    )
    items = (
        read_layer(spark, "bronze", "order_items").drop("_ingested_at")
        .withColumn("line_total",
                    F.round(F.col("quantity") * F.col("unit_price") * (1 - F.col("discount"))))
    )
    check_quality(customers, orders, items)

    write_layer(customers, "silver", "customers")
    write_layer(read_layer(spark, "bronze", "products").drop("_ingested_at"), "silver", "products")
    write_layer(orders.repartition("order_month"), "silver", "orders", partition_by="order_month")
    write_layer(items, "silver", "order_items")


# ---------- GOLD: business-level aggregates ----------
def gold(spark: SparkSession) -> None:
    customers = read_layer(spark, "silver", "customers")
    products = read_layer(spark, "silver", "products")
    orders = read_layer(spark, "silver", "orders")
    items = read_layer(spark, "silver", "order_items")

    # Completed order lines feed all three gold tables -> cache.
    # products has 100 rows -> broadcast, so the large side is not shuffled.
    details = (
        orders.filter(F.col("status") == "COMPLETED")
        .join(items, "order_id")
        .join(F.broadcast(products.select("product_id", "product_name", "category")), "product_id")
        .cache()
    )

    # 1. Daily revenue per category
    daily_revenue = (
        details.groupBy(F.to_date("order_date").alias("date"), "category")
        .agg(F.countDistinct("order_id").alias("num_orders"),
             F.sum("quantity").alias("units_sold"),
             F.sum("line_total").alias("revenue"))
    )
    write_layer(daily_revenue.coalesce(1), "gold", "daily_revenue")

    # 2. Customer 360
    order_values = items.groupBy("order_id").agg(F.sum("line_total").alias("order_value"))
    stats = (
        orders.join(order_values, "order_id")           # aggregate first, join later
        .groupBy("customer_id")
        .agg(F.count("*").alias("num_orders"),
             F.sum(F.when(F.col("status") == "COMPLETED", F.col("order_value")).otherwise(0))
              .alias("total_spent"),
             F.max("order_date").alias("last_order_date"),
             F.round(F.avg((F.col("status") == "CANCELLED").cast("int")), 3).alias("cancel_rate"))
    )
    w_customer = Window.partitionBy("customer_id").orderBy(F.desc("category_revenue"), "category")
    favorite_category = (
        details.groupBy("customer_id", "category").agg(F.sum("line_total").alias("category_revenue"))
        .withColumn("rank", F.row_number().over(w_customer))
        .filter(F.col("rank") == 1)
        .select("customer_id", F.col("category").alias("favorite_category"))
    )
    customer_360 = (
        customers.select("customer_id", "full_name", "city", "segment")
        .join(stats, "customer_id", "left")
        .join(favorite_category, "customer_id", "left")
        .fillna({"num_orders": 0, "total_spent": 0})
    )
    write_layer(customer_360.coalesce(1), "gold", "customer_360")

    # 3. Top 5 products per month
    w_month = Window.partitionBy("order_month").orderBy(F.desc("revenue"))
    top_products_monthly = (
        details.groupBy("order_month", "product_id", "product_name")
        .agg(F.sum("line_total").alias("revenue"))
        .withColumn("rank", F.row_number().over(w_month))
        .filter(F.col("rank") <= 5)
    )
    write_layer(top_products_monthly.coalesce(1), "gold", "top_products_monthly")

    details.unpersist()


def main() -> None:
    spark = get_spark("vietmart-pipeline")
    bronze(spark)
    silver(spark)
    gold(spark)

    # Quick reconciliation
    revenue = read_layer(spark, "gold", "daily_revenue").agg(F.sum("revenue")).first()[0]
    print("Total revenue (gold):", int(revenue))
    print("Rows in customer_360:", read_layer(spark, "gold", "customer_360").count())
    print("Rows in top_products_monthly:", read_layer(spark, "gold", "top_products_monthly").count())


if __name__ == "__main__":
    main()
