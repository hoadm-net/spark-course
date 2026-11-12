"""Chapter 10 - reading from Kafka (reference only, needs a running broker).

spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:<spark version> \
    ch10_streaming/kafka_source.py
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

EVENTS_SCHEMA = "event_time TIMESTAMP, user_id INT, event_type STRING, product_id INT"

if __name__ == "__main__":
    spark = SparkSession.builder.appName("kafka-source").getOrCreate()
    raw = (
        spark.readStream.format("kafka")
             .option("kafka.bootstrap.servers", "localhost:9092")
             .option("subscribe", "vietmart.events")
             .load()
    )
    events = (
        raw.select(F.from_json(F.col("value").cast("string"), EVENTS_SCHEMA).alias("e"))
           .select("e.*")
    )
    events.writeStream.format("console").start().awaitTermination()
