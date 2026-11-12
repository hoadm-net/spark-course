"""Chapter 10 solutions.  Run: python -m ch10_streaming.solutions [10.1 10.2 10.3]"""
import shutil

from pyspark.sql import functions as F

from ch10_streaming.examples import read_events
from common.data import EVENTS_SCHEMA, data_path, load_products, out_path
from common.session import run_cli


def ex_10_1(spark):
    aggregates = [F.count("*").alias("num_events"),
                  F.approx_count_distinct("user_id").alias("approx_users")]
    query = (
        read_events(spark, max_files_per_trigger=2)
        .groupBy("event_type").agg(*aggregates)
        .writeStream.outputMode("complete").format("memory").queryName("ex_10_1")
        .trigger(availableNow=True).start()
    )
    query.awaitTermination()
    spark.sql("SELECT * FROM ex_10_1 ORDER BY event_type").show()

    # Batch run on the same data: the result must be identical
    (spark.read.schema(EVENTS_SCHEMA).json(data_path("events"))
          .groupBy("event_type").agg(*aggregates)
          .orderBy("event_type").show())


def ex_10_2(spark):
    target, checkpoint = out_path("ex_10_2"), out_path("_chk/ex_10_2")
    shutil.rmtree(target, ignore_errors=True)
    shutil.rmtree(checkpoint, ignore_errors=True)

    def run_once() -> int:
        query = (
            read_events(spark)
            .filter(F.col("event_type") == "view")
            .withWatermark("event_time", "30 minutes")
            .groupBy(F.window("event_time", "30 minutes", "10 minutes"))
            .count()
            .writeStream.outputMode("append").format("parquet")
            .option("path", target)
            .option("checkpointLocation", checkpoint)
            .trigger(availableNow=True)
            .start()
        )
        query.awaitTermination()
        return spark.read.parquet(target).count()

    print("windows after run 1:", run_once())
    print("windows after run 2:", run_once())     # same: the checkpoint remembers progress


def ex_10_3(spark):
    target, checkpoint = out_path("live_revenue"), out_path("_chk/ex_10_3")
    shutil.rmtree(checkpoint, ignore_errors=True)

    def overwrite_latest(batch_df, batch_id):
        batch_df.write.mode("overwrite").parquet(target)

    query = (
        read_events(spark)
        .filter(F.col("event_type") == "purchase")
        .join(load_products(spark), "product_id")
        .groupBy("category")
        .agg(F.sum("price").alias("estimated_revenue"))
        .writeStream
        .outputMode("complete")
        .foreachBatch(overwrite_latest)
        .option("checkpointLocation", checkpoint)
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()
    spark.read.parquet(target).orderBy(F.desc("estimated_revenue")).show()


if __name__ == "__main__":
    run_cli({"10.1": ex_10_1, "10.2": ex_10_2, "10.3": ex_10_3}, "ch10-solutions")
