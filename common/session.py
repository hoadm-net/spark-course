"""Shared SparkSession factory and a tiny CLI runner used by every chapter."""
import os
import sys
from typing import Callable

from pyspark.sql import SparkSession


def get_spark(app_name: str = "vietmart") -> SparkSession:
    """Return a local SparkSession (override the master with SPARK_MASTER)."""
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def run_cli(sections: dict[str, Callable[[SparkSession], None]], app_name: str) -> None:
    """Run the sections named on the command line, or all of them.

    Example: python -m ch05_dataframe.solutions 5.2 5.3
    """
    spark = get_spark(app_name)
    selected = sys.argv[1:] or list(sections)
    for key in selected:
        if key not in sections:
            raise SystemExit(f"Unknown section {key!r}. Available: {', '.join(sections)}")
        print(f"\n=== {key} ===", flush=True)
        sections[key](spark)
