"""Chapter 6 solutions.  Run: python -m ch06_sql_window.solutions [6.1 6.2 6.3]"""
from pyspark.sql import Window
from pyspark.sql import functions as F

from common.data import (completed_orders, load_items_with_total, load_orders, load_products,
                         monthly_revenue, order_totals)
from common.session import run_cli


def ex_6_1(spark):
    load_orders(spark).createOrReplaceTempView("orders")
    load_items_with_total(spark).createOrReplaceTempView("order_items")
    load_products(spark).createOrReplaceTempView("products")
    # COUNT(DISTINCT ...) because one order has several item lines.
    spark.sql("""
        SELECT p.category,
               COUNT(DISTINCT o.order_id)                             AS num_orders,
               SUM(i.line_total)                                      AS revenue,
               ROUND(SUM(i.line_total) / COUNT(DISTINCT o.order_id))  AS avg_per_order
        FROM orders o
        JOIN order_items i ON o.order_id = i.order_id
        JOIN products p    ON i.product_id = p.product_id
        WHERE o.status = 'COMPLETED'
        GROUP BY p.category
        ORDER BY revenue DESC
    """).show()


def top3_per_category(spark):
    product_revenue = (
        completed_orders(load_orders(spark))
        .join(load_items_with_total(spark), "order_id")
        .join(load_products(spark), "product_id")
        .groupBy("category", "product_name")
        .agg(F.sum("line_total").alias("revenue"))
    )
    w = Window.partitionBy("category").orderBy(F.desc("revenue"))
    return (
        product_revenue.withColumn("rank", F.row_number().over(w))
                       .filter(F.col("rank") <= 3)
                       .orderBy("category", "rank")
    )


def ex_6_2(spark):
    top3_per_category(spark).show(15, truncate=False)


def ex_6_3(spark):
    monthly = monthly_revenue(load_orders(spark), order_totals(load_items_with_total(spark)))
    # No partitionBy: fine here because `monthly` has only 18 rows.
    w_month = Window.orderBy("month")
    w_year = (Window.partitionBy(F.substring("month", 1, 4)).orderBy("month")
                    .rowsBetween(Window.unboundedPreceding, Window.currentRow))
    growth = (
        monthly
        .withColumn("prev_revenue", F.lag("revenue").over(w_month))
        .withColumn("growth_pct",
                    F.round((F.col("revenue") - F.col("prev_revenue")) / F.col("prev_revenue") * 100, 1))
        .withColumn("ytd_revenue", F.sum("revenue").over(w_year))
    )
    growth.show(20)


if __name__ == "__main__":
    run_cli({"6.1": ex_6_1, "6.2": ex_6_2, "6.3": ex_6_3}, "ch06-solutions")
