"""Chapter 10 - Structured Streaming.  Run: python -m ch10_streaming.examples

The data/events folder is treated as a stream: each file is a new batch of data.
"""
import shutil

from pyspark.sql import functions as F

from common.data import EVENTS_SCHEMA, data_path, load_products, out_path
from common.session import run_cli


def read_events(spark, max_files_per_trigger: int = 1):
    return (
        spark.readStream
             .schema(EVENTS_SCHEMA)                 # streaming sources require a schema
             .option("maxFilesPerTrigger", max_files_per_trigger)
             .json(data_path("events"))
    )


def count_events(spark):
    events = read_events(spark)
    print("isStreaming:", events.isStreaming)
    query = (
        events.groupBy("event_type").count()
              .writeStream
              .outputMode("complete")
              .format("memory")                     # in-memory table, for experiments only
              .queryName("event_counts")
              .trigger(availableNow=True)           # process everything available, then stop
              .start()
    )
    query.awaitTermination()
    spark.sql("SELECT * FROM event_counts ORDER BY count DESC").show()
    print("Micro-batches:", query.lastProgress["batchId"] + 1)


def windows_and_watermark(spark):
    target, checkpoint = out_path("stream_hourly"), out_path("_chk/stream_hourly")
    shutil.rmtree(target, ignore_errors=True)
    shutil.rmtree(checkpoint, ignore_errors=True)

    hourly = (
        read_events(spark)
        .withWatermark("event_time", "30 minutes")
        .groupBy(F.window("event_time", "1 hour"), "event_type")
        .count()
    )
    query = (
        hourly.writeStream
              .outputMode("append")
              .format("parquet")
              .option("path", target)
              .option("checkpointLocation", checkpoint)
              .trigger(availableNow=True)
              .start()
    )
    query.awaitTermination()
    (spark.read.parquet(target)
          .select(F.col("window.start").alias("window_start"), "event_type", "count")
          .orderBy("window_start", "event_type")
          .show(6, truncate=False))


def stream_static_join(spark):
    def handle_batch(batch_df, batch_id):
        print(f"batch {batch_id}: {batch_df.count()} purchase events")

    query = (
        read_events(spark)
        .filter(F.col("event_type") == "purchase")
        .join(load_products(spark), "product_id")   # stream-static join
        .writeStream
        .foreachBatch(handle_batch)
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    run_cli({"10.2": count_events, "10.3": windows_and_watermark, "10.5": stream_static_join},
            "ch10-examples")
