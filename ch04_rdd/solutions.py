"""Chapter 4 solutions (RDD API only).  Run: python -m ch04_rdd.solutions [4.1 ...]"""
import json

from common.data import data_path
from common.session import run_cli


def _item_rows(sc):
    """order_items.csv as lists: order_id, product_id, quantity, unit_price, discount."""
    lines = sc.textFile(data_path("order_items.csv"))
    header = lines.first()
    # split(",") is only safe because no value contains a comma
    return lines.filter(lambda line: line != header).map(lambda line: line.split(","))


def ex_4_1(spark):
    sc = spark.sparkContext
    names = sc.textFile(data_path("products.json")).map(lambda line: json.loads(line)["product_name"])
    word_counts = (
        names.flatMap(lambda name: name.lower().split())
             .map(lambda word: (word, 1))
             .reduceByKey(lambda a, b: a + b)
    )
    print(word_counts.takeOrdered(10, key=lambda kv: -kv[1]))


def ex_4_2(spark):
    rows = _item_rows(spark.sparkContext)
    revenue = (
        rows.map(lambda r: (int(r[1]), int(r[2]) * float(r[3]) * (1 - float(r[4]))))
            .reduceByKey(lambda a, b: a + b)
    )
    for product_id, value in revenue.takeOrdered(5, key=lambda kv: -kv[1]):
        print(product_id, round(value))


def ex_4_3(spark):
    sc = spark.sparkContext
    lines = sc.textFile(data_path("orders.csv"))
    header = lines.first()
    top_customers = (
        lines.filter(lambda line: line != header)
             .map(lambda line: (int(line.split(",")[1]), 1))
             .reduceByKey(lambda a, b: a + b)
             .takeOrdered(5, key=lambda kv: -kv[1])
    )
    print(top_customers)


def ex_4_4(spark):
    pairs = _item_rows(spark.sparkContext).map(lambda r: (int(r[1]), int(r[2])))  # (product_id, quantity)

    # (a) groupByKey ships every value through the shuffle
    avg_a = pairs.groupByKey().mapValues(lambda values: sum(values) / len(values))

    # (b) aggregateByKey combines (sum, count) inside each partition first
    avg_b = (
        pairs.aggregateByKey(
                (0, 0),
                lambda acc, value: (acc[0] + value, acc[1] + 1),      # within a partition
                lambda x, y: (x[0] + y[0], x[1] + y[1]))              # across partitions
             .mapValues(lambda t: t[0] / t[1])
    )
    print(sorted(avg_a.collect())[:3])
    print(sorted(avg_b.collect())[:3])


if __name__ == "__main__":
    run_cli({"4.1": ex_4_1, "4.2": ex_4_2, "4.3": ex_4_3, "4.4": ex_4_4}, "ch04-solutions")
