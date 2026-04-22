# Databricks notebook source
# MAGIC %md
# MAGIC # Gold: demanda por hora y zona

# COMMAND ----------

# MAGIC %run ../00-setup/config

# COMMAND ----------

spark.sql(f"USE CATALOG {CATALOG}")

spark.sql(f"""
    CREATE OR REPLACE TABLE {T_GOLD_HOURLY_DEMAND} AS
    SELECT pickup_borough, pickup_zone, pickup_hour, pickup_dayofweek,
      is_weekend, is_rush_hour,
      COUNT(*) AS trips,
      ROUND(AVG(total_amount), 2) AS avg_fare,
      ROUND(AVG(avg_speed_mph), 2) AS avg_speed_mph
    FROM {T_SILVER_TRIPS_ENRICHED}
    WHERE pickup_zone IS NOT NULL
    GROUP BY pickup_borough, pickup_zone, pickup_hour, pickup_dayofweek, is_weekend, is_rush_hour
""")

spark.sql(f"OPTIMIZE {T_GOLD_HOURLY_DEMAND} ZORDER BY (pickup_zone, pickup_hour)")

display(spark.sql(f"""
    SELECT pickup_zone, pickup_hour, SUM(trips) AS trips
    FROM {T_GOLD_HOURLY_DEMAND}
    WHERE is_rush_hour = true
    GROUP BY pickup_zone, pickup_hour
    ORDER BY trips DESC LIMIT 10
"""))
