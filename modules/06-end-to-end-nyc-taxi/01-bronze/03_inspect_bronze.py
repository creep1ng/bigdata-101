# Databricks notebook source
# MAGIC %md
# MAGIC # Inspección de la capa Bronze
# MAGIC
# MAGIC Comprobar el transaction log y los archivos físicos de la tabla Delta.

# COMMAND ----------

# MAGIC %run ../00-setup/config

# COMMAND ----------

configure_storage_access(spark)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Historial de operaciones

# COMMAND ----------

display(spark.sql(f"DESCRIBE HISTORY {T_BRONZE_TRIPS}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Detalle físico: path, número de archivos, tamaño

# COMMAND ----------

display(spark.sql(f"DESCRIBE DETAIL {T_BRONZE_TRIPS}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Contenido del transaction log
# MAGIC
# MAGIC Cada commit es un archivo JSON en `_delta_log/`. Así es como Delta Lake
# MAGIC logra ACID sobre archivos Parquet.

# COMMAND ----------

detail = spark.sql(f"DESCRIBE DETAIL {T_BRONZE_TRIPS}").first()
table_path = detail["location"]
print(f"Path físico: {table_path}")

files = dbutils.fs.ls(f"{table_path}/_delta_log")
for f in files[:10]:
    print(f"  {f.name:30s} {f.size} bytes")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Time travel: consultar la tabla como estaba antes

# COMMAND ----------

display(spark.sql(f"SELECT COUNT(*) AS rows_v0 FROM {T_BRONZE_TRIPS} VERSION AS OF 0"))

# COMMAND ----------

display(spark.sql(f"SELECT COUNT(*) AS rows_current FROM {T_BRONZE_TRIPS}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Distribución por mes

# COMMAND ----------

display(spark.sql(f"""
    SELECT
      puYear, puMonth,
      COUNT(*) AS trips
    FROM {T_BRONZE_TRIPS}
    GROUP BY puYear, puMonth
    ORDER BY puYear, puMonth
"""))
