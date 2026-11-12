"""Chapter 7 solutions.  Run: python -m ch07_udf_catalyst.solutions [7.1 7.2 7.3]"""
import time

import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import StringType

from ch06_sql_window.solutions import top3_per_category
from common.data import load_items_with_total, order_totals
from common.session import run_cli


@F.udf(returnType=StringType())
def order_size_udf(value):
    if value is None:
        return None
    return "small" if value < 500_000 else ("medium" if value < 2_000_000 else "large")


def order_size_builtin():
    return (
        F.when(F.col("order_value") < 500_000, "small")
         .when(F.col("order_value") < 2_000_000, "medium")
         .otherwise("large")
    )


@pandas_udf(StringType())
def order_size_pandas(value: pd.Series) -> pd.Series:
    return pd.cut(value, bins=[-float("inf"), 500_000, 2_000_000, float("inf")],
                  labels=["small", "medium", "large"], right=False).astype(str)


def _timed(label, df):
    start = time.time()
    df.groupBy("size").count().orderBy("size").show()
    print(f"{label}: {time.time() - start:.2f}s")


def ex_7_1(spark):
    totals = order_totals(load_items_with_total(spark))
    _timed("Python UDF", totals.withColumn("size", order_size_udf("order_value")))
    _timed("Built-in when()", totals.withColumn("size", order_size_builtin()))
    # The UDF plan contains BatchEvalPython: rows travel to a Python worker and back.
    totals.withColumn("size", order_size_udf("order_value")).explain()


def ex_7_2(spark):
    totals = order_totals(load_items_with_total(spark))
    _timed("pandas UDF", totals.withColumn("size", order_size_pandas("order_value")))


def ex_7_3(spark):
    top3_per_category(spark).explain("formatted")


if __name__ == "__main__":
    run_cli({"7.1": ex_7_1, "7.2": ex_7_2, "7.3": ex_7_3}, "ch07-solutions")
