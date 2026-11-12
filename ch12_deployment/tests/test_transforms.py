"""Chapter 12 (exercise 12.3) - unit tests.  Run from the repo root: pytest ch12_deployment"""
import pytest
from pyspark.sql import SparkSession

from ch12_deployment.jobs.monthly_revenue import add_line_total, monthly_revenue


@pytest.fixture(scope="session")
def spark():
    session = SparkSession.builder.master("local[2]").appName("tests").getOrCreate()
    yield session
    session.stop()


def test_add_line_total(spark):
    df = spark.createDataFrame(
        [(2, 100_000.0, 0.0), (1, 200_000.0, 0.1)],
        "quantity INT, unit_price DOUBLE, discount DOUBLE",
    )
    result = [row["line_total"] for row in add_line_total(df).collect()]
    assert result == [200_000.0, 180_000.0]


def test_monthly_revenue_ignores_cancelled_orders(spark):
    orders = spark.createDataFrame(
        [(1, "2025-01-10 10:00:00", "COMPLETED"), (2, "2025-01-11 10:00:00", "CANCELLED")],
        "order_id INT, order_date STRING, status STRING",
    ).selectExpr("order_id", "CAST(order_date AS TIMESTAMP) AS order_date", "status")
    items = spark.createDataFrame(
        [(1, 1, 100_000.0, 0.0), (2, 1, 999_000.0, 0.0)],
        "order_id INT, quantity INT, unit_price DOUBLE, discount DOUBLE",
    )
    rows = monthly_revenue(orders, items).collect()
    assert len(rows) == 1
    assert rows[0]["month"] == "2025-01" and rows[0]["revenue"] == 100_000.0
