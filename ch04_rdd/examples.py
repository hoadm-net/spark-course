"""Chapter 4 - RDD basics.  Run: python -m ch04_rdd.examples"""
from common.session import run_cli


def basics(spark):
    sc = spark.sparkContext
    rdd = sc.parallelize(range(1, 11), 4)            # 10 elements, 4 partitions
    even_squares = rdd.map(lambda x: x * x).filter(lambda x: x % 2 == 0)
    print(even_squares.collect())                    # [4, 16, 36, 64, 100]
    print(rdd.reduce(lambda a, b: a + b))            # 55


def word_count(spark):
    sc = spark.sparkContext
    lines = sc.parallelize([
        "spark processes big data",
        "spark runs in memory",
        "big data needs spark",
    ])
    counts = (
        lines.flatMap(lambda line: line.split())
             .map(lambda word: (word, 1))
             .reduceByKey(lambda a, b: a + b)
    )
    print(counts.takeOrdered(3, key=lambda kv: -kv[1]))


def rdd_and_dataframe(spark):
    sc = spark.sparkContext
    pairs = sc.parallelize([("An", 30), ("Binh", 25)])
    df = pairs.toDF(["name", "age"])                 # RDD -> DataFrame
    df.show()
    print(df.rdd.take(1))                            # DataFrame -> RDD of Row


if __name__ == "__main__":
    run_cli({"4.2": basics, "4.3": word_count, "4.5": rdd_and_dataframe}, "ch04-examples")
