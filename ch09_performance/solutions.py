"""Chapter 9 solutions.  Run: python -m ch09_performance.solutions [9.1 ...]"""
import glob
import time

from pyspark.sql import functions as F

from common.data import (load_customers, load_items_with_total, load_orders, load_products, out_path)
from common.session import run_cli


def ex_9_1(spark):
    items, products = load_items_with_total(spark), load_products(spark)
    print("(a) default")
    items.join(products, "product_id").explain()

    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
    print("(b) auto broadcast disabled")
    items.join(products, "product_id").explain()
    print("(c) auto broadcast disabled + broadcast hint")
    items.join(F.broadcast(products), "product_id").explain()
    spark.conf.unset("spark.sql.autoBroadcastJoinThreshold")


def ex_9_2(spark):
    full = (load_orders(spark)
            .join(load_items_with_total(spark), "order_id")
            .join(load_products(spark), "product_id"))

    def three_queries(df):
        df.groupBy("category").agg(F.sum("line_total")).collect()
        df.groupBy("payment_method").agg(F.sum("line_total")).collect()
        df.groupBy(F.date_format("order_date", "yyyy-MM")).agg(F.sum("line_total")).collect()

    start = time.time()
    three_queries(full)
    print(f"No cache: {time.time() - start:.2f}s")

    full.cache()
    full.count()                     # action that materializes the cache
    start = time.time()
    three_queries(full)
    print(f"With cache: {time.time() - start:.2f}s")
    full.unpersist()


def ex_9_3(spark):
    orders = load_orders(spark)

    def run(name):
        path = out_path(name)
        start = time.time()
        orders.groupBy("customer_id").count().write.mode("overwrite").parquet(path)
        num_files = len(glob.glob(f"{path}/part-*"))
        print(f"{name}: {time.time() - start:.2f}s, {num_files} files")

    spark.conf.set("spark.sql.adaptive.enabled", False)
    spark.conf.set("spark.sql.shuffle.partitions", 200)
    run("counts_200")
    spark.conf.set("spark.sql.shuffle.partitions", 8)
    run("counts_8")

    spark.conf.set("spark.sql.adaptive.enabled", True)
    spark.conf.set("spark.sql.shuffle.partitions", 200)
    run("counts_aqe")
    spark.conf.unset("spark.sql.shuffle.partitions")


def ex_9_4(spark):
    orders, customers = load_orders(spark), load_customers(spark)

    # 1. Show the skew
    (orders.repartition(8, "customer_id")
           .groupBy(F.spark_partition_id().alias("pid")).count()
           .orderBy(F.desc("count")).show())

    # 2. Salting: spread the hot key over NUM_SALTS sub-keys
    NUM_SALTS = 16
    orders_salted = orders.withColumn("salt", (F.rand(seed=7) * NUM_SALTS).cast("int"))
    customers_salted = customers.crossJoin(
        spark.range(NUM_SALTS).select(F.col("id").cast("int").alias("salt"))
    )
    salted_join = orders_salted.join(customers_salted, ["customer_id", "salt"]).drop("salt")
    plain_join = orders.join(customers, "customer_id")
    print(salted_join.count(), plain_join.count())      # both 50000

    # 3. Distribution after salting
    (orders_salted.repartition(8, "customer_id", "salt")
                  .groupBy(F.spark_partition_id().alias("pid")).count()
                  .orderBy(F.desc("count")).show())


if __name__ == "__main__":
    run_cli({"9.1": ex_9_1, "9.2": ex_9_2, "9.3": ex_9_3, "9.4": ex_9_4}, "ch09-solutions")
