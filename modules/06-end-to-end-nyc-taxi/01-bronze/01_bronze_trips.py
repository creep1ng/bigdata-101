# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze: ingesta de viajes desde el landing compartido
# MAGIC
# MAGIC Lee los Parquet que el profesor depositó en el landing y los persiste
# MAGIC como tabla Delta managed en TU catálogo de Unity Catalog.

# COMMAND ----------

# MAGIC %run ../00-setup/config

# COMMAND ----------

from pyspark.sql import functions as F

spark.sql(f"USE CATALOG {CATALOG}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Leer el landing

# COMMAND ----------

raw = (
    spark.read.schema(LANDING_SCHEMA).parquet(LANDING_TRIPS_PATH)
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.input_file_name())
)

print(f"Leyendo desde: {LANDING_TRIPS_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Escribir a Delta managed en Unity Catalog

# COMMAND ----------

(
    raw.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(T_BRONZE_TRIPS)
)

print(f"✓ Bronze escrito en {T_BRONZE_TRIPS}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Verificación

# COMMAND ----------

bronze = spark.table(T_BRONZE_TRIPS)

bronze.agg(
    F.count("*").alias("total_rows"),
    F.min("tpep_pickup_datetime").alias("min_pickup"),
    F.max("tpep_pickup_datetime").alias("max_pickup"),
).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Anatomía de la tabla Delta
# MAGIC
# MAGIC Lo que escribimos no es un Parquet normal — es una tabla Delta
# MAGIC con transaction log, time travel y ACID.

# COMMAND ----------

display(spark.sql(f"DESCRIBE DETAIL {T_BRONZE_TRIPS}"))

# COMMAND ----------

display(spark.sql(f"DESCRIBE HISTORY {T_BRONZE_TRIPS}"))
