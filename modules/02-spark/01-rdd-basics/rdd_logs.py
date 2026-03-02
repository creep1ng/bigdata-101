"""
Log Analysis with Spark RDDs.

Count log entries by severity and filter errors.
Demonstrates two different RDD patterns:

1. Count by key (same as wordcount/sales):
   map → reduceByKey

2. Filter — select only rows matching a condition:
   rdd.filter(lambda line: "ERROR" in line)

filter() is a transformation that Spark doesn't have in traditional MapReduce.
In MapReduce, you'd have to filter inside the mapper. In Spark, it's a
first-class operation you can chain anywhere in the pipeline.

Run:
    docker compose exec spark-master /opt/spark/bin/spark-submit /app/01-rdd-basics/rdd_logs.py
"""

from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder \
        .appName("RDD Logs") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    sc = spark.sparkContext

    logs = [
        "2024-01-15 10:23:01 ERROR Database connection failed",
        "2024-01-15 10:23:05 INFO Retrying connection",
        "2024-01-15 10:23:10 INFO Connection established",
        "2024-01-15 10:24:00 WARNING High memory usage detected",
        "2024-01-15 10:25:30 ERROR Timeout on request /api/users",
        "2024-01-15 10:25:31 INFO Request retried successfully",
        "2024-01-15 10:26:00 ERROR Disk space critical",
        "2024-01-15 10:26:15 WARNING CPU usage above 90%",
        "2024-01-15 10:27:00 INFO Scheduled backup started",
        "2024-01-15 10:28:00 INFO Backup completed",
    ]

    logs_rdd = sc.parallelize(logs)

    # --- Pattern 1: Count by key ---
    # Extract severity level (3rd field) and count occurrences
    counts = (
        logs_rdd
        .map(lambda line: (line.split()[2], 1))  # "ERROR" → ("ERROR", 1)
        .reduceByKey(lambda a, b: a + b)
        .sortBy(lambda x: x[1], ascending=False)
    )

    print("=" * 50)
    print("Log Entries by Severity (RDD)")
    print("=" * 50)
    for level, count in counts.collect():
        print(f"  {level}: {count}")

    # --- Pattern 2: Filter ---
    # filter() keeps only elements where the function returns True.
    # This is something MapReduce can't do natively — you'd need
    # to add an if-statement inside the mapper.
    errors = logs_rdd.filter(lambda line: "ERROR" in line)

    print(f"\nError messages:")
    for line in errors.collect():
        print(f"  {line}")

    spark.stop()


if __name__ == "__main__":
    main()
