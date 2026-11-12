"""Chapter 9 - performance tuning.  Run: python -m ch09_performance.examples"""
import time

from pyspark.sql import functions as F

from common.data import load_order_items, load_orders, load_products
from common.session import run_cli


def join_strategies(spark):
    items, products = load_order_items(spark), load_products(spark)

    # Disable automatic broadcast to see a SortMergeJoin
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
    items.join(products, "product_id").explain()

    # Explicit hint: broadcast the small table to every executor
    items.join(F.broadcast(products), "product_id").explain()

    spark.conf.unset("spark.sql.autoBroadcastJoinThreshold")


def caching(spark):
    orders, items = load_orders(spark), load_order_items(spark)
    order_values = (
        items.groupBy("order_id")
             .agg(F.sum(F.col("quantity") * F.col("unit_price") * (1 - F.col("discount"))).alias("order_value"))
             .join(orders, "order_id")
    )
    order_values.cache()            # lazy: nothing is stored yet
    for attempt in (1, 2):
        start = time.time()
        order_values.count()
        print(f"count #{attempt}: {time.time() - start:.2f}s")
    order_values.unpersist()


if __name__ == "__main__":
    run_cli({"9.3": join_strategies, "9.4": caching}, "ch09-examples")
