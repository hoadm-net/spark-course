"""Chapter 7 - UDFs and the Catalyst optimizer.  Run: python -m ch07_udf_catalyst.examples"""
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import DoubleType, StringType

from common.data import load_order_items, load_orders, load_products
from common.session import run_cli


@F.udf(returnType=StringType())
def price_band(price):
    if price is None:
        return None
    return "high" if price >= 1_000_000 else ("medium" if price >= 300_000 else "low")


@pandas_udf(DoubleType())
def price_with_vat(price: pd.Series) -> pd.Series:
    return price * 1.08          # works on a whole batch (Series), not row by row


def python_udf(spark):
    products = load_products(spark)
    products.withColumn("price_band", price_band("price")).groupBy("price_band").count().show()


def pandas_udf_example(spark):
    products = load_products(spark)
    products.select("product_name", price_with_vat("price").alias("price_with_vat")).show(3)


def reading_a_plan(spark):
    orders, items = load_orders(spark), load_order_items(spark)
    query = (
        orders.filter(F.col("status") == "COMPLETED")
              .join(items, "order_id")
              .groupBy("payment_method")
              .agg(F.sum("quantity").alias("total_quantity"))
    )
    query.explain("formatted")


if __name__ == "__main__":
    run_cli({"7.1": python_udf, "7.2": pandas_udf_example, "7.4": reading_a_plan}, "ch07-examples")
