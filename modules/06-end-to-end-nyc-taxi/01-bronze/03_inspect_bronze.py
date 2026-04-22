# Databricks notebook source
# MAGIC %md
# MAGIC # Inspección de la capa Bronze

# COMMAND ----------

# MAGIC %run ../00-setup/config

# COMMAND ----------

spark.sql(f"USE CATALOG {CATALOG}")

# COMMAND ----------

display(spark.sql(f"DESCRIBE HISTORY {T_BRONZE_TRIPS}"))

# COMMAND ----------

display(spark.sql(f"DESCRIBE DETAIL {T_BRONZE_TRIPS}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transaction log

# COMMAND ----------

detail = spark.sql(f"DESCRIBE DETAIL {T_BRONZE_TRIPS}").first()
table_path = detail["location"]
print(f"Path físico: {table_path}")

files = dbutils.fs.ls(f"{table_path}/_delta_log")
for f in files[:10]:
    print(f"  {f.name:30s} {f.size} bytes")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Time travel

# COMMAND ----------

display(spark.sql(f"SELECT COUNT(*) AS rows_v0 FROM {T_BRONZE_TRIPS} VERSION AS OF 0"))
display(spark.sql(f"SELECT COUNT(*) AS rows_current FROM {T_BRONZE_TRIPS}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Distribución por mes

# COMMAND ----------

display(spark.sql(f"""
    SELECT
      DATE_TRUNC('month', tpep_pickup_datetime) AS month,
      COUNT(*) AS trips,
      COUNT(DISTINCT _source_file) AS files
    FROM {T_BRONZE_TRIPS}
    GROUP BY 1
    ORDER BY 1
"""))
