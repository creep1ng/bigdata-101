# Databricks notebook source
# MAGIC %md
# MAGIC # Gold: demanda por hora y zona
# MAGIC
# MAGIC Tabla para heatmaps hora × zona. Útil para planificación de flotas
# MAGIC y pricing dinámico.

# COMMAND ----------

# MAGIC %run ../00-setup/config

# COMMAND ----------

configure_storage_access(spark)

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE TABLE {T_GOLD_HOURLY_DEMAND}
    USING DELTA
    AS
    SELECT
      pickup_borough,
      pickup_zone,
      pickup_hour,
      pickup_dayofweek,
      is_weekend,
      is_rush_hour,
      COUNT(*)                     AS trips,
      ROUND(AVG(totalAmount), 2)   AS avg_fare,
      ROUND(AVG(avg_speed_mph), 2) AS avg_speed_mph
    FROM {T_SILVER_TRIPS_ENRICHED}
    WHERE pickup_zone IS NOT NULL
    GROUP BY
      pickup_borough, pickup_zone,
      pickup_hour, pickup_dayofweek, is_weekend, is_rush_hour
""")

# COMMAND ----------

spark.sql(f"OPTIMIZE {T_GOLD_HOURLY_DEMAND} ZORDER BY (pickup_zone, pickup_hour)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ejemplo: top 10 pickups por hora pico

# COMMAND ----------

display(spark.sql(f"""
    SELECT pickup_zone, pickup_hour, SUM(trips) AS trips
    FROM {T_GOLD_HOURLY_DEMAND}
    WHERE is_rush_hour = true
    GROUP BY pickup_zone, pickup_hour
    ORDER BY trips DESC
    LIMIT 10
"""))
