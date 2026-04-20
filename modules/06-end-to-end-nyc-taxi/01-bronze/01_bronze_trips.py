# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze: ingesta de viajes desde Azure Open Datasets
# MAGIC
# MAGIC Lee el landing público (Azure Open Datasets NYC TLC) y escribe una
# MAGIC tabla Delta en TU storage personal, filtrando por el rango de años
# MAGIC configurado.
# MAGIC
# MAGIC La primera corrida es la carga inicial (`overwrite`). Si corres de
# MAGIC nuevo con un rango mayor, usa el flag `APPEND_MODE = True` más abajo
# MAGIC para simular llegada de datos nuevos.

# COMMAND ----------

# MAGIC %run ../00-setup/config

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# Configurar acceso a ambos storages: landing (SAS público) y TU storage personal
configure_landing_access(spark)
configure_storage_access(spark)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Leer el rango configurado del landing
# MAGIC
# MAGIC Usamos los filtros `puYear` / `puMonth` que son las particiones del
# MAGIC dataset — Spark solo lee los archivos que importan (partition pruning).

# COMMAND ----------

raw = (
    spark.read.parquet(LANDING_WASBS)
    .filter(F.col("puYear").between(YEAR_FROM, YEAR_TO))
    .filter(F.col("puMonth").between(MONTH_FROM, MONTH_TO))
    # Metadatos de ingesta — trazabilidad
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source", F.lit("azureopendatastorage/nyctlc/yellow"))
)

print(f"Ventana: {YEAR_FROM}-{MONTH_FROM:02d} a {YEAR_TO}-{MONTH_TO:02d}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Escribir a Delta en TU storage
# MAGIC
# MAGIC Particionamos por año y mes — facilita reprocesamiento y partition pruning
# MAGIC en las consultas posteriores.

# COMMAND ----------

APPEND_MODE = False  # Cambiar a True para simular ingesta incremental

write_mode = "append" if APPEND_MODE else "overwrite"

(
    raw.write
    .format("delta")
    .mode(write_mode)
    .option("overwriteSchema", "true" if not APPEND_MODE else "false")
    .partitionBy("puYear", "puMonth")
    .saveAsTable(T_BRONZE_TRIPS)
)

print(f"✓ Bronze escrito en {T_BRONZE_TRIPS} (mode={write_mode})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Verificación

# COMMAND ----------

bronze = spark.table(T_BRONZE_TRIPS)

bronze.agg(
    F.count("*").alias("total_rows"),
    F.min("tpepPickupDateTime").alias("min_pickup"),
    F.max("tpepPickupDateTime").alias("max_pickup"),
    F.countDistinct("puYear", "puMonth").alias("partitions"),
).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Anatomía de la tabla Delta
# MAGIC
# MAGIC Lo que escribimos no es "un Parquet" — es una **tabla Delta** con
# MAGIC transaction log, time travel y ACID.

# COMMAND ----------

display(spark.sql(f"DESCRIBE DETAIL {T_BRONZE_TRIPS}"))

# COMMAND ----------

display(spark.sql(f"DESCRIBE HISTORY {T_BRONZE_TRIPS}"))
