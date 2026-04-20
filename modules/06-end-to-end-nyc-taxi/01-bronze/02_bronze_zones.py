# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze: tabla estática de zonas
# MAGIC
# MAGIC Azure Open Datasets no incluye el lookup de zonas. Lo descargamos
# MAGIC una vez desde el CDN oficial de la TLC y lo persistimos como Delta
# MAGIC en TU storage personal.
# MAGIC
# MAGIC Son 265 filas — no amerita ningún proceso especial.

# COMMAND ----------

# MAGIC %run ../00-setup/config

# COMMAND ----------

import urllib.request
import tempfile
from pathlib import Path

configure_storage_access(spark)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Descargar el CSV de zonas
# MAGIC
# MAGIC Lo bajamos al filesystem local del driver (temporal) y de ahí lo leemos.

# COMMAND ----------

ZONES_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as tmp:
    urllib.request.urlretrieve(ZONES_URL, tmp.name)
    local_path = tmp.name

print(f"Descargado en: {local_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Leer y escribir como Delta

# COMMAND ----------

zones_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(f"file:{local_path}")
)

zones_df.printSchema()
zones_df.display()

# COMMAND ----------

(
    zones_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(T_BRONZE_ZONES)
)

print(f"✓ Zones escritas en {T_BRONZE_ZONES}")

# COMMAND ----------

display(spark.sql(f"""
    SELECT COUNT(*) AS total_zones, COUNT(DISTINCT Borough) AS boroughs
    FROM {T_BRONZE_ZONES}
"""))
