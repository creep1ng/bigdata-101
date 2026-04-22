# Databricks notebook source
# MAGIC %md
# MAGIC # Gold: métricas diarias

# COMMAND ----------

# MAGIC %run ../00-setup/config

# COMMAND ----------

spark.sql(f"USE CATALOG {CATALOG}")

spark.sql(f"""
    CREATE OR REPLACE TABLE {T_GOLD_DAILY_METRICS} AS
    SELECT pickup_date,
      COUNT(*) AS total_trips,
      ROUND(SUM(total_amount), 2) AS total_revenue,
      ROUND(AVG(total_amount), 2) AS avg_fare,
      ROUND(AVG(trip_distance), 2) AS avg_distance_mi,
      ROUND(AVG(trip_duration_min), 2) AS avg_duration_min,
      ROUND(AVG(avg_speed_mph), 2) AS avg_speed_mph,
      ROUND(SUM(tip_amount), 2) AS total_tips,
      ROUND(AVG(tip_rate) * 100, 2) AS avg_tip_pct,
      COUNT(DISTINCT PULocationID) AS unique_pickup_zones
    FROM {T_SILVER_TRIPS_ENRICHED}
    GROUP BY pickup_date ORDER BY pickup_date
""")

display(spark.sql(f"SELECT * FROM {T_GOLD_DAILY_METRICS} ORDER BY pickup_date DESC LIMIT 30"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Días anómalos (z-score > 2)

# COMMAND ----------

display(spark.sql(f"""
    WITH stats AS (
      SELECT AVG(total_trips) AS avg_trips, STDDEV(total_trips) AS sd_trips
      FROM {T_GOLD_DAILY_METRICS}
    )
    SELECT d.pickup_date, d.total_trips,
      ROUND((d.total_trips - s.avg_trips) / s.sd_trips, 2) AS z_score
    FROM {T_GOLD_DAILY_METRICS} d, stats s
    WHERE ABS((d.total_trips - s.avg_trips) / s.sd_trips) > 2
    ORDER BY z_score
"""))
