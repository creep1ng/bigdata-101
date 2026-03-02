"""
WordCount with Spark RDDs.

The same WordCount from pure Python and mrjob, now with Spark.
This is the "bridge" example — compare the three approaches:

Pure Python:  mapper(line) → yield (word, 1) → reducer sums
mrjob:        mapper(_, line) → yield (word, 1) → reducer sums
Spark RDD:    flatMap → map to pairs → reduceByKey

Key concepts demonstrated:
- flatMap: one line produces many words (1-to-many)
- map: one word produces one (word, 1) pair (1-to-1)
- reduceByKey: groups by key and sums — replaces shuffle + reduce

Run:
    docker compose exec spark-master /opt/spark/bin/spark-submit /app/01-rdd-basics/rdd_wordcount.py
"""

from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder \
        .appName("RDD WordCount") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    sc = spark.sparkContext

    # Same sample data from the pure Python example in 01-mapreduce
    text = [
        "MapReduce is a programming model",
        "MapReduce processes large volumes of data",
        "The MapReduce model has two main phases",
        "The Map phase transforms the data",
        "The Reduce phase aggregates the results"
    ]

    results = (
        sc.parallelize(text)                            # List → RDD (distributed)
        .flatMap(lambda line: line.lower().split())     # 1 line → many words (flatMap)
        .map(lambda word: (word, 1))                    # 1 word → 1 pair (map)
        .reduceByKey(lambda a, b: a + b)                # Group by word, sum counts
        .sortBy(lambda x: x[1], ascending=False)        # Sort by count descending
    )

    # take(N) only brings N results to the driver — safer than collect() for large datasets
    print("=" * 50)
    print("WordCount with Spark RDDs")
    print("=" * 50)
    for word, count in results.take(10):
        print(f"  {word}: {count}")

    # count() is an action — triggers the full pipeline execution
    print(f"\nUnique words: {results.count()}")
    spark.stop()


if __name__ == "__main__":
    main()
