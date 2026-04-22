# Databricks notebook source
# MAGIC %md
# MAGIC # ML Features: predicción de duración de viaje

# COMMAND ----------

# MAGIC %run ../00-setup/config

# COMMAND ----------

spark.sql(f"USE CATALOG {CATALOG}")

spark.sql(f"""
    CREATE OR REPLACE TABLE {T_ML_FEATURES} AS
    SELECT
      trip_duration_min AS target_duration_min,
      PULocationID, DOLocationID,
      pickup_borough, dropoff_borough,
      pickup_hour, pickup_dayofweek,
      CAST(is_weekend AS INT) AS is_weekend,
      CAST(is_rush_hour AS INT) AS is_rush_hour,
      passenger_count, trip_distance, RatecodeID,
      tpep_pickup_datetime AS pickup_ts,
      pickup_date
    FROM {T_SILVER_TRIPS_ENRICHED}
    WHERE pickup_borough IS NOT NULL
      AND dropoff_borough IS NOT NULL
      AND trip_duration_min BETWEEN 2 AND 120
      AND trip_distance BETWEEN 0.2 AND 50
""")

display(spark.sql(f"""
    SELECT COUNT(*) AS rows,
      ROUND(AVG(target_duration_min), 2) AS avg_duration,
      ROUND(STDDEV(target_duration_min), 2) AS sd_duration,
      ROUND(AVG(trip_distance), 2) AS avg_distance
    FROM {T_ML_FEATURES}
"""))
