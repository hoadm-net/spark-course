"""Chapter 12 - connecting to a Spark Connect server (reference only).

Start a server first, e.g.  $SPARK_HOME/sbin/start-connect-server.sh
The DataFrame and SQL APIs work as usual; RDD and sparkContext are not available.
"""
from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder.remote("sc://localhost:15002").getOrCreate()
    spark.range(5).show()
