"""
Spark Architecture Demo.

Run this and open http://localhost:4040 to explore the Spark UI.
The script pauses so you can inspect jobs, stages, and partitions.

Run:
    docker compose exec spark-master spark-submit /app/02-architecture/architecture_demo.py
"""

from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder \
        .appName("Architecture Demo") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    sc = spark.sparkContext

    # --- Partitions ---
    data = list(range(1, 101))
    rdd = sc.parallelize(data, numSlices=4)

    print("=" * 50)
    print("Partitions Demo")
    print("=" * 50)
    print(f"  Number of partitions: {rdd.getNumPartitions()}")
    print(f"  Elements per partition: {rdd.glom().map(len).collect()}")

    # --- DAG: transformations + shuffle ---
    transformed = (
        rdd
        .map(lambda x: (x % 10, x))
        .reduceByKey(lambda a, b: a + b)
        .filter(lambda x: x[1] > 100)
    )

    print(f"\n  Results (sum by last digit, > 100):")
    for key, total in transformed.collect():
        print(f"    Digit {key}: {total}")

    # --- Cache demo ---
    cached_rdd = sc.parallelize(data, 4).map(lambda x: x * 2).cache()
    cached_rdd.count()

    print(f"\n  Cached RDD count: {cached_rdd.count()}")
    print(f"  Check 'Storage' tab in Spark UI")

    print(f"\n{'=' * 50}")
    print("Open http://localhost:4040 to explore the Spark UI")
    print("Press Enter to stop...")
    print("=" * 50)

    try:
        input()
    except (KeyboardInterrupt, EOFError):
        pass

    spark.stop()


if __name__ == "__main__":
    main()
