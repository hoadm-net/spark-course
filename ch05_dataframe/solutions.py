"""Chapter 5 solutions.  Run: python -m ch05_dataframe.solutions [5.1 ...]

The transformations written here are reused by later chapters through
common/data.py (clean_customers, with_line_total, order_totals, monthly_revenue).
"""
from pyspark.sql import functions as F

from common.data import (CUSTOMERS_SCHEMA, completed_orders, data_path, load_order_items,
                         load_orders, load_products)
from common.session import run_cli


def ex_5_1(spark):
    customers_raw = spark.read.csv(data_path("customers.csv"), header=True, schema=CUSTOMERS_SCHEMA)
    print("Rows:", customers_raw.count())
    print("Distinct customer_id:", customers_raw.select("customer_id").distinct().count())
    print("Missing city:", customers_raw.filter(F.col("city").isNull()).count())
    customers_raw.groupBy("city").count().orderBy("city").show(30, truncate=False)


def ex_5_2(spark):
    customers_raw = spark.read.csv(data_path("customers.csv"), header=True, schema=CUSTOMERS_SCHEMA)
    customers = (
        customers_raw
        .dropDuplicates(["customer_id"])
        .withColumn("city", F.initcap(F.trim(F.col("city"))))
        .fillna({"city": "Unknown"})
    )
    print(customers.count(), customers.select("city").distinct().count())    # 5000 9
    return customers


def _items_with_total(spark):
    return load_order_items(spark).withColumn(
        "line_total",
        F.round(F.col("quantity") * F.col("unit_price") * (1 - F.col("discount"))),
    )


def _order_totals(items):
    return items.groupBy("order_id").agg(
        F.count("*").alias("num_lines"),
        F.sum("quantity").alias("total_quantity"),
        F.sum("line_total").alias("order_value"),
    )


def ex_5_3(spark):
    totals = _order_totals(_items_with_total(spark))
    totals.orderBy(F.desc("order_value")).show(1)


def ex_5_4(spark):
    totals = _order_totals(_items_with_total(spark))
    monthly = (
        completed_orders(load_orders(spark))
        .join(totals, "order_id")
        .groupBy(F.date_format("order_date", "yyyy-MM").alias("month"))
        .agg(F.count("*").alias("num_orders"), F.sum("order_value").alias("revenue"))
        .orderBy("month")
    )
    monthly.show(20)
    monthly.orderBy(F.desc("revenue")).show(1)


def ex_5_5(spark):
    # Aggregate first (100 rows), then join with products: the join stays tiny.
    top10 = (
        completed_orders(load_orders(spark))
        .join(_items_with_total(spark), "order_id")
        .groupBy("product_id")
        .agg(F.sum("quantity").alias("units_sold"), F.sum("line_total").alias("revenue"))
        .join(load_products(spark), "product_id")
        .select("product_name", "category", "units_sold", "revenue")
        .orderBy(F.desc("revenue"))
        .limit(10)
    )
    top10.show(truncate=False)


def ex_5_6(spark):
    customers = ex_5_2(spark)
    no_orders = customers.join(load_orders(spark), "customer_id", "left_anti")
    print(no_orders.count())
    no_orders.groupBy("segment").count().show()


if __name__ == "__main__":
    run_cli({"5.1": ex_5_1, "5.2": ex_5_2, "5.3": ex_5_3, "5.4": ex_5_4,
             "5.5": ex_5_5, "5.6": ex_5_6}, "ch05-solutions")
