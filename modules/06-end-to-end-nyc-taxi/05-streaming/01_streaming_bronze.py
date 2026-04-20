# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze streaming: Auto Loader en modo continuo
# MAGIC
# MAGIC Mismo código que el Bronze batch, pero con trigger de procesamiento
# MAGIC continuo. Cada 30 segundos Auto Loader revisa el landing zone y
# MAGIC procesa lo que haya nuevo.

# COMMAND ----------

# MAGIC %run ../00-setup/config

# COMMAND ----------

spark.sql(f"USE CATALOG {CATALOG}")

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

raw_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation", f"{SCHEMA_BASE}/yellow_trips_stream")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("cloudFiles.inferColumnTypes", "true")
    .load(LANDING_PATH)
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.col("_metadata.file_path"))
)

# COMMAND ----------

# Trigger processingTime → cada 30s revisa archivos nuevos
stream_query = (
    raw_stream.writeStream
    .format("delta")
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/yellow_trips_stream")
    .option("mergeSchema", "true")
    .trigger(processingTime="30 seconds")
    .queryName("bronze_yellow_trips_stream")
    .toTable(f"{CATALOG}.{SCHEMA_BRONZE}.yellow_trips_stream")
)

# COMMAND ----------

# MAGIC %md
# MAGIC El stream está corriendo. Ir a la pestaña **Structured Streaming** del notebook
# MAGIC para ver throughput, batch size y latencia en vivo.
# MAGIC
# MAGIC Para detenerlo:
# MAGIC ```python
# MAGIC stream_query.stop()
# MAGIC ```
