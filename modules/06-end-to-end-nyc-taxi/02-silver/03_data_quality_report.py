# Databricks notebook source
# MAGIC %md
# MAGIC # Reporte de calidad de datos
# MAGIC
# MAGIC Inspecciona la tabla de rechazos para entender qué tipo de datos malos
# MAGIC estamos viendo y en qué proporción.

# COMMAND ----------

# MAGIC %run ../00-setup/config

# COMMAND ----------

configure_storage_access(spark)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Porcentaje de rechazo por regla

# COMMAND ----------

display(spark.sql(f"""
    SELECT
      COUNT(*) AS total_rejected,
      SUM(CAST(NOT valid_distance AS INT))   AS fail_distance,
      SUM(CAST(NOT valid_fare AS INT))       AS fail_fare,
      SUM(CAST(NOT valid_total AS INT))      AS fail_total,
      SUM(CAST(NOT valid_passengers AS INT)) AS fail_passengers,
      SUM(CAST(NOT valid_dropoff AS INT))    AS fail_dropoff,
      SUM(CAST(NOT valid_duration AS INT))   AS fail_duration
    FROM {T_SILVER_REJECTED}
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Distribución temporal de rechazos

# COMMAND ----------

display(spark.sql(f"""
    SELECT
      DATE_TRUNC('month', tpepPickupDateTime) AS month,
      COUNT(*) AS rejected_trips
    FROM {T_SILVER_REJECTED}
    GROUP BY 1
    ORDER BY 1
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Tasa global de calidad

# COMMAND ----------

display(spark.sql(f"""
    WITH counts AS (
      SELECT
        (SELECT COUNT(*) FROM {T_BRONZE_TRIPS})    AS bronze_total,
        (SELECT COUNT(*) FROM {T_SILVER_TRIPS})    AS silver_valid,
        (SELECT COUNT(*) FROM {T_SILVER_REJECTED}) AS silver_rejected
    )
    SELECT
      bronze_total,
      silver_valid,
      silver_rejected,
      ROUND(100.0 * silver_valid / bronze_total, 2)    AS pct_valid,
      ROUND(100.0 * silver_rejected / bronze_total, 2) AS pct_rejected
    FROM counts
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Muestra de rechazos

# COMMAND ----------

display(spark.sql(f"""
    SELECT
      tpepPickupDateTime, tpepDropoffDateTime,
      passengerCount, tripDistance, fareAmount, totalAmount,
      valid_distance, valid_fare, valid_total, valid_passengers,
      valid_dropoff, valid_duration
    FROM {T_SILVER_REJECTED}
    LIMIT 20
"""))
