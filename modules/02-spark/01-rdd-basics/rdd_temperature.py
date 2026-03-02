"""
Temperature Analysis with Spark RDDs.

Average temperature per city — same problem from the mrjob module.
Demonstrates the "average by key" pattern with RDDs.

The challenge: Spark RDDs don't have a built-in "averageByKey".
Solution: carry (sum, count) together, then divide at the end.

    (city, temp) → (city, (temp, 1)) → reduceByKey → (city, (total, n)) → avg = total/n

Compare with mrjob:
    mapper:  yield (city, temp)
    reducer: yield (city, sum(temps) / len(temps))

Run:
    docker compose exec spark-master /opt/spark/bin/spark-submit /app/01-rdd-basics/rdd_temperature.py
"""

from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder \
        .appName("RDD Temperature") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    sc = spark.sparkContext

    # Sample data — same structure as the mrjob temperature example
    readings = [
        {"city": "Bogota", "temperature": 18},
        {"city": "Medellin", "temperature": 28},
        {"city": "Bogota", "temperature": 15},
        {"city": "Cali", "temperature": 32},
        {"city": "Medellin", "temperature": 26},
        {"city": "Bogota", "temperature": 20},
        {"city": "Cali", "temperature": 30},
        {"city": "Medellin", "temperature": 27},
    ]

    results = (
        sc.parallelize(readings)
        # Step 1: Extract (city, (temperature, 1)) pairs
        # The 1 is a counter — we need it to compute the average later
        .map(lambda r: (r["city"], (r["temperature"], 1)))

        # Step 2: Sum temperatures and counts per city
        # (Bogota, (18,1)) + (Bogota, (15,1)) → (Bogota, (33, 2))
        .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))

        # Step 3: Compute average from (total, count)
        # mapValues only transforms the value, keeping the key unchanged
        .mapValues(lambda v: round(v[0] / v[1], 1))

        .sortBy(lambda x: x[1], ascending=False)
    )

    print("=" * 50)
    print("Average Temperature per City (RDD)")
    print("=" * 50)
    for city, avg in results.collect():
        print(f"  {city}: {avg}°C")

    spark.stop()


if __name__ == "__main__":
    main()
