# Databricks notebook source
# MAGIC %md
# MAGIC # ML Features: predicción de duración de viaje

# COMMAND ----------

# MAGIC %run ../00-setup/config

# COMMAND ----------

configure_storage_access(spark)

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE TABLE {T_ML_FEATURES}
    USING DELTA
    AS
    SELECT
      -- Target
      trip_duration_min AS target_duration_min,

      -- Features disponibles al momento del pickup
      puLocationId,
      doLocationId,
      pickup_borough,
      dropoff_borough,
      pickup_hour,
      pickup_dayofweek,
      CAST(is_weekend AS INT)   AS is_weekend,
      CAST(is_rush_hour AS INT) AS is_rush_hour,
      passengerCount,
      tripDistance,
      rateCodeId,

      -- Keys para auditoría
      tpepPickupDateTime AS pickup_ts,
      pickup_date
    FROM {T_SILVER_TRIPS_ENRICHED}
    WHERE
      pickup_borough IS NOT NULL
      AND dropoff_borough IS NOT NULL
      AND trip_duration_min BETWEEN 2 AND 120
      AND tripDistance BETWEEN 0.2 AND 50
""")

# COMMAND ----------

display(spark.sql(f"""
    SELECT
      COUNT(*)                              AS rows,
      ROUND(AVG(target_duration_min), 2)    AS avg_duration,
      ROUND(STDDEV(target_duration_min), 2) AS sd_duration,
      ROUND(AVG(tripDistance), 2)           AS avg_distance,
      COUNT(DISTINCT puLocationId)          AS unique_pickup_zones,
      COUNT(DISTINCT doLocationId)          AS unique_dropoff_zones
    FROM {T_ML_FEATURES}
"""))
