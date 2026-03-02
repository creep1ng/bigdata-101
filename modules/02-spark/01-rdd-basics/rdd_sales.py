"""
Sales Analysis with Spark RDDs.

Total revenue per product — demonstrates the "sum by key" pattern.

Pattern:
    (product, revenue) → reduceByKey(sum) → sorted results

This is the simplest aggregation pattern:
- map extracts (key, value)
- reduceByKey combines values with the same key

Compare with the "average by key" pattern in rdd_temperature.py,
which needs to carry (sum, count) instead of just a value.

Run:
    docker compose exec spark-master /opt/spark/bin/spark-submit /app/01-rdd-basics/rdd_sales.py
"""

from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder \
        .appName("RDD Sales") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    sc = spark.sparkContext

    sales = [
        {"product": "Laptop", "quantity": 2, "price": 999.99},
        {"product": "Mouse", "quantity": 5, "price": 29.99},
        {"product": "Laptop", "quantity": 1, "price": 999.99},
        {"product": "Keyboard", "quantity": 3, "price": 79.99},
        {"product": "Mouse", "quantity": 2, "price": 29.99},
        {"product": "Monitor", "quantity": 1, "price": 499.99},
        {"product": "Keyboard", "quantity": 1, "price": 79.99},
    ]

    results = (
        sc.parallelize(sales)
        # Calculate revenue per sale: quantity × price
        .map(lambda s: (s["product"], s["quantity"] * s["price"]))
        # Sum revenue per product — same key gets combined
        .reduceByKey(lambda a, b: a + b)
        .sortBy(lambda x: x[1], ascending=False)
    )

    print("=" * 50)
    print("Total Revenue per Product (RDD)")
    print("=" * 50)
    for product, revenue in results.collect():
        print(f"  {product}: ${revenue:,.2f}")

    spark.stop()


if __name__ == "__main__":
    main()
