# Databricks notebook source
# MAGIC %md
# MAGIC # Gold: revenue por zona

# COMMAND ----------

# MAGIC %run ../00-setup/config

# COMMAND ----------

spark.sql(f"USE CATALOG {CATALOG}")

spark.sql(f"""
    CREATE OR REPLACE TABLE {T_GOLD_REVENUE_BY_ZONE} AS
    SELECT pickup_borough, pickup_zone, PULocationID,
      COUNT(*) AS total_trips,
      ROUND(SUM(total_amount), 2) AS total_revenue,
      ROUND(AVG(total_amount), 2) AS avg_fare,
      ROUND(SUM(tip_amount), 2) AS total_tips,
      ROUND(AVG(tip_rate) * 100, 2) AS avg_tip_pct,
      ROUND(AVG(trip_distance), 2) AS avg_distance_mi,
      ROUND(AVG(trip_duration_min), 2) AS avg_duration_min
    FROM {T_SILVER_TRIPS_ENRICHED}
    WHERE pickup_zone IS NOT NULL
    GROUP BY pickup_borough, pickup_zone, PULocationID
""")

spark.sql(f"OPTIMIZE {T_GOLD_REVENUE_BY_ZONE} ZORDER BY (pickup_borough)")

display(spark.sql(f"SELECT * FROM {T_GOLD_REVENUE_BY_ZONE} ORDER BY total_revenue DESC LIMIT 20"))
