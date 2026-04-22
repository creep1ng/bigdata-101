# Databricks notebook source
# MAGIC %md
# MAGIC # Reporte de calidad de datos

# COMMAND ----------

# MAGIC %run ../00-setup/config

# COMMAND ----------

spark.sql(f"USE CATALOG {CATALOG}")

# COMMAND ----------

display(spark.sql(f"""
    SELECT COUNT(*) AS total_rejected,
      SUM(CAST(NOT valid_distance AS INT))   AS fail_distance,
      SUM(CAST(NOT valid_fare AS INT))       AS fail_fare,
      SUM(CAST(NOT valid_total AS INT))      AS fail_total,
      SUM(CAST(NOT valid_passengers AS INT)) AS fail_passengers,
      SUM(CAST(NOT valid_dropoff AS INT))    AS fail_dropoff,
      SUM(CAST(NOT valid_duration AS INT))   AS fail_duration
    FROM {T_SILVER_REJECTED}
"""))

# COMMAND ----------

display(spark.sql(f"""
    WITH counts AS (
      SELECT
        (SELECT COUNT(*) FROM {T_BRONZE_TRIPS})    AS bronze_total,
        (SELECT COUNT(*) FROM {T_SILVER_TRIPS})    AS silver_valid,
        (SELECT COUNT(*) FROM {T_SILVER_REJECTED}) AS silver_rejected
    )
    SELECT *, ROUND(100.0 * silver_valid / bronze_total, 2) AS pct_valid,
      ROUND(100.0 * silver_rejected / bronze_total, 2) AS pct_rejected
    FROM counts
"""))
