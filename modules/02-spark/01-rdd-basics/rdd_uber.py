"""
Uber Movement - Travel Times Analysis with Spark RDDs.

Analyzes real Uber Movement data across multiple cities:
Bogota, Boston, Paris, Sydney, Manila, Washington DC, Johannesburg.

This exercise demonstrates:
- Reading and parsing real CSV files with RDDs
- Handling different file formats (8 vs 10 columns)
- Multiple aggregations on the same cached dataset
- Saving RDD results to disk with timestamped folders

Run:
    docker compose exec spark-master /opt/spark/bin/spark-submit /app/01-rdd-basics/rdd_uber.py

Dataset: Uber Movement Travel Times (/datasets/uber/)
Methodology: See 'Uber Movement - Travel Times Methodology.pdf'
"""

from pyspark.sql import SparkSession
from datetime import datetime
import csv
import io

# Each city has a CSV file with travel times between zones.
# Some files have 10 columns (with geometry), others have 8 (without).
CITIES = {
    "Bogota": "Travel_Times - Bogota.csv",
    "Boston": "Travel_Times - Boston.csv",
    "Paris": "Travel_Times - Paris.csv",
    "Sydney": "Travel_Times - Sydney.csv",
    "Manila": "Travel_Times - Manila.csv",
    "Washington DC": "Travel_Times - Washington DC.csv",
    "Johannesburg": "Travel_Times - Johannesburg and Pretoria.csv",
}

DATA_DIR = "/datasets/uber"


def parse_csv_line(line):
    """
    Parse a CSV line using Python's csv module.

    We use csv.reader instead of split(',') because some fields
    contain commas inside quotes (e.g. zone names like "SANTA INES, 003107").

    Args:
        line: Raw CSV line as string

    Returns:
        List of field values
    """
    return next(csv.reader(io.StringIO(line)))


def extract_trip(city, fields):
    """
    Extract a trip tuple from parsed CSV fields.

    The CSV files come in two formats:
    - 10 columns: ID, Origin, OriginGeometry, ID, Dest, DestGeometry, DateRange, Mean, Lower, Upper
    - 8 columns:  ID, Origin, ID, Dest, DateRange, Mean, Lower, Upper

    In both cases, the last 3 columns are always: Mean, Lower, Upper.

    Args:
        city: City name (string)
        fields: List of parsed CSV fields

    Returns:
        Tuple: (city, origin_name, dest_name, mean_seconds, lower_seconds, upper_seconds)
    """
    has_geometry = len(fields) == 10

    origin_name = fields[1]
    dest_name = fields[4] if has_geometry else fields[3]
    mean_travel_time = int(fields[-3])   # Mean Travel Time (Seconds)
    lower_bound = int(fields[-2])        # Range - Lower Bound (Seconds)
    upper_bound = int(fields[-1])        # Range - Upper Bound (Seconds)

    return (city, origin_name, dest_name, mean_travel_time, lower_bound, upper_bound)


def load_city(sc, city, filename):
    """
    Load and parse a single city's CSV file into an RDD of trip tuples.

    Pipeline:
    1. Read the file as lines
    2. Skip the header row
    3. Parse each line with csv.reader
    4. Filter out malformed rows
    5. Extract structured trip tuples

    Args:
        sc: SparkContext
        city: City name (string)
        filename: CSV filename

    Returns:
        RDD of (city, origin, dest, mean_time, lower, upper) tuples
    """
    return (
        sc.textFile(f"{DATA_DIR}/{filename}")
        .filter(lambda line: not line.startswith('"Origin Movement'))  # Skip header
        .map(parse_csv_line)
        .filter(lambda fields: len(fields) >= 8)  # Discard malformed rows
        .map(lambda fields: extract_trip(city, fields))
    )


def save_results(rdd, output_path):
    """
    Save an RDD of (key, value) pairs as a single CSV file.

    coalesce(1) merges all partitions into one file.
    This is fine for small result sets (summaries), but should NOT
    be used for large datasets — it defeats the purpose of distribution.

    Args:
        rdd: RDD of (key, value) tuples
        output_path: Directory path to save the file
    """
    rdd.map(lambda x: f"{x[0]},{x[1]}") \
       .coalesce(1) \
       .saveAsTextFile(output_path)


def main():
    spark = SparkSession.builder.appName("Uber Movement Analysis").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    sc = spark.sparkContext

    # --- Load all cities into a single RDD ---
    city_rdds = [load_city(sc, city, filename) for city, filename in CITIES.items()]
    all_trips = sc.union(city_rdds)

    # Cache because we'll run multiple aggregations on the same data.
    # Without cache(), Spark would re-read and re-parse the CSVs for each action.
    all_trips.cache()

    total = all_trips.count()
    print("=" * 60)
    print("Uber Movement - Travel Times Analysis")
    print("=" * 60)
    print(f"\nTotal trip records: {total:,}")

    # --- 1. Trips per city ---
    # Pattern: map to (key, 1) → reduceByKey to sum
    trips_per_city = (
        all_trips
        .map(lambda trip: (trip[0], 1))              # (city, 1)
        .reduceByKey(lambda a, b: a + b)             # (city, total_count)
        .sortBy(lambda x: x[1], ascending=False)
    )

    print("\nTrips per city:")
    for city, count in trips_per_city.collect():
        print(f"  {city}: {count:,}")

    # --- 2. Average travel time per city (in minutes) ---
    # Pattern: map to (key, (sum, count)) → reduceByKey → mapValues to compute average
    avg_per_city = (
        all_trips
        .map(lambda trip: (trip[0], (trip[3], 1)))   # (city, (seconds, 1))
        .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))  # (city, (total_sec, count))
        .mapValues(lambda v: round(v[0] / v[1] / 60, 1))       # (city, avg_minutes)
        .sortBy(lambda x: x[1], ascending=False)
    )

    print("\nAverage travel time per city (minutes):")
    for city, avg in avg_per_city.collect():
        print(f"  {city}: {avg} min")

    # --- 3. Top 5 longest trips ---
    longest = all_trips.sortBy(lambda trip: trip[3], ascending=False).take(5)

    print("\nTop 5 longest average trips:")
    for city, origin, dest, mean_seconds, _, _ in longest:
        # Show only the first part of zone names (before the comma)
        origin_short = origin.split(",")[0]
        dest_short = dest.split(",")[0]
        print(f"  {city}: {origin_short} → {dest_short} = {mean_seconds // 60} min")

    # --- 4. Travel time variability per city ---
    # Variability = upper_bound - lower_bound (how unpredictable travel times are)
    variability = (
        all_trips
        .map(lambda trip: (trip[0], (trip[5] - trip[4], 1)))  # (city, (range, 1))
        .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
        .mapValues(lambda v: round(v[0] / v[1] / 60, 1))     # (city, avg_range_minutes)
        .sortBy(lambda x: x[1], ascending=False)
    )

    print("\nAverage travel time variability per city (minutes):")
    for city, var in variability.collect():
        print(f"  {city}: ±{var} min")

    # --- Save results to disk ---
    # Each run creates a new folder with a timestamp to avoid conflicts.
    # Spark's saveAsTextFile fails if the output directory already exists.
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = f"/app/01-rdd-basics/output/uber_results/{timestamp}"

    save_results(avg_per_city, f"{output_dir}/avg_per_city")
    save_results(variability, f"{output_dir}/variability_per_city")
    save_results(trips_per_city, f"{output_dir}/trips_per_city")

    print(f"\nResults saved to {output_dir}/")
    spark.stop()


if __name__ == "__main__":
    main()
