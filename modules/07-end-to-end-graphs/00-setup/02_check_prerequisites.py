# Databricks notebook source
# MAGIC %md
# MAGIC # Validación de prerrequisitos
# MAGIC
# MAGIC Antes de construir el grafo verificamos:
# MAGIC
# MAGIC 1. Runtime **DBR 17.0 o superior** (necesario para `WITH RECURSIVE`)
# MAGIC 2. Las tablas del módulo 06 existen y tienen datos
# MAGIC 3. **NetworkX** está disponible (viene en DBR ML)

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Runtime version

# COMMAND ----------

dbr_version = spark.conf.get("spark.databricks.clusterUsageTags.sparkVersion", "unknown")
print(f"Runtime: {dbr_version}")

major = int(dbr_version.split(".")[0]) if dbr_version[0].isdigit() else 0
if major < 17:
    raise RuntimeError(
        f"Este módulo requiere DBR 17.0 o superior para recursive CTEs.\n"
        f"Runtime actual: {dbr_version}\n"
        f"Reinicia el cluster con una imagen 17.x o 18.x ML."
    )
print(f"✓ Runtime compatible con recursive CTE")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Fuente del módulo 06

# COMMAND ----------

assert_source_ready(spark)

# COMMAND ----------

from pyspark.sql import functions as F

trips = spark.table(T_SOURCE_TRIPS)
zones = spark.table(T_SOURCE_ZONES)

trips.agg(
    F.count("*").alias("total_trips"),
    F.countDistinct("PULocationID").alias("distinct_pu"),
    F.countDistinct("DOLocationID").alias("distinct_do"),
    F.min("tpep_pickup_datetime").alias("min_date"),
    F.max("tpep_pickup_datetime").alias("max_date"),
).display()

zones.agg(
    F.count("*").alias("total_zones"),
    F.countDistinct("Borough").alias("boroughs"),
).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. NetworkX

# COMMAND ----------

import networkx as nx
print(f"✓ NetworkX {nx.__version__}")

# COMMAND ----------

# MAGIC %md
# MAGIC Listo. Continúa con `01-build-graph/`.
