"""Chapter 6 - Spark SQL and window functions.  Run: python -m ch06_sql_window.examples"""
from pyspark.sql import Window
from pyspark.sql import functions as F

from common.data import load_orders
from common.session import run_cli


def sql_on_dataframes(spark):
    load_orders(spark).createOrReplaceTempView("orders")
    result = spark.sql("""
        SELECT payment_method,
               COUNT(*) AS num_orders,
               ROUND(AVG(CASE WHEN status = 'CANCELLED' THEN 1 ELSE 0 END), 3) AS cancel_rate
        FROM orders
        GROUP BY payment_method
        ORDER BY num_orders DESC
    """)
    result.show()


def window_functions(spark):
    orders = load_orders(spark)
    w = Window.partitionBy("customer_id").orderBy("order_date")
    history = (
        orders.filter(F.col("customer_id") != 1)
              .withColumn("order_seq", F.row_number().over(w))
              .withColumn("prev_order_date", F.lag("order_date").over(w))
              .withColumn("days_since_prev", F.datediff("order_date", "prev_order_date"))
    )
    history.select("customer_id", "order_date", "order_seq", "days_since_prev").show(6)

    # The same ranking in SQL
    orders.createOrReplaceTempView("orders")
    spark.sql("""
        SELECT customer_id, order_date,
               ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date) AS order_seq
        FROM orders
        WHERE customer_id <> 1
    """).show(3)


if __name__ == "__main__":
    run_cli({"6.1": sql_on_dataframes, "6.2": window_functions}, "ch06-examples")
