# Databricks notebook source
# MAGIC %md
# MAGIC # ADMIN ONLY — Descarga del dataset al landing (Volume UC)
# MAGIC
# MAGIC Crea un catálogo admin `nytaxi_landing` con un Volume donde se depositan
# MAGIC los Parquet mensuales de NYC TLC. Los estudiantes leen desde ahí.
# MAGIC
# MAGIC Los Volumes de UC se montan en `/Volumes/<catalog>/<schema>/<volume>/`
# MAGIC y son accesibles como filesystem local — sin restricciones de clusters
# MAGIC Shared/Serverless.
# MAGIC
# MAGIC Es idempotente: si un archivo ya existe, se salta.

# COMMAND ----------

import urllib.request
import os

# =============================================================================
# Configuración
# =============================================================================
TLC_BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
ZONES_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

# Catálogo admin para el landing compartido
LANDING_CATALOG = "nytaxi_landing"
LANDING_SCHEMA = "raw"
LANDING_VOLUME = "files"

# Path del Volume en el filesystem
VOLUME_PATH = f"/Volumes/{LANDING_CATALOG}/{LANDING_SCHEMA}/{LANDING_VOLUME}"
TRIPS_DIR = f"{VOLUME_PATH}/yellow_trips"
ZONES_DIR = f"{VOLUME_PATH}/zones"

# Meses a descargar
MONTHS = [
    "2023-01", "2023-02", "2023-03",
    "2023-04", "2023-05", "2023-06",
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Crear catálogo, schema y volume para el landing

# COMMAND ----------

EXTERNAL_LOCATION = "abfss://landing@dl25604nytaxi.dfs.core.windows.net"

spark.sql(f"""
    CREATE CATALOG IF NOT EXISTS {LANDING_CATALOG}
      MANAGED LOCATION '{EXTERNAL_LOCATION}/{LANDING_CATALOG}'
      COMMENT 'Landing compartido con los archivos crudos de NYC TLC'
""")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {LANDING_CATALOG}.{LANDING_SCHEMA}")

spark.sql(f"""
    CREATE VOLUME IF NOT EXISTS {LANDING_CATALOG}.{LANDING_SCHEMA}.{LANDING_VOLUME}
      COMMENT 'Parquets mensuales de NYC TLC Yellow Taxi'
""")

print(f"✓ Volume listo en {VOLUME_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Dar acceso de lectura a los estudiantes

# COMMAND ----------

spark.sql(f"GRANT USE CATALOG ON CATALOG {LANDING_CATALOG} TO `bigdata-students`")
spark.sql(f"GRANT USE SCHEMA ON SCHEMA {LANDING_CATALOG}.{LANDING_SCHEMA} TO `bigdata-students`")
spark.sql(f"GRANT READ VOLUME ON VOLUME {LANDING_CATALOG}.{LANDING_SCHEMA}.{LANDING_VOLUME} TO `bigdata-students`")
print("✓ Permisos de lectura otorgados a bigdata-students")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Descargar parquets mensuales
# MAGIC
# MAGIC `urllib.request.urlretrieve` escribe directo a `/Volumes/...` que es
# MAGIC un path local válido en clusters UC. Sin `dbutils.fs.cp`, sin `/tmp`.

# COMMAND ----------

os.makedirs(TRIPS_DIR, exist_ok=True)

for month in MONTHS:
    filename = f"yellow_tripdata_{month}.parquet"
    dest = f"{TRIPS_DIR}/{filename}"

    if os.path.exists(dest) and os.path.getsize(dest) > 0:
        size_mb = os.path.getsize(dest) / 1024 / 1024
        print(f"  skip  {filename:40s} ({size_mb:.1f} MB ya existe)")
        continue

    url = f"{TLC_BASE_URL}/{filename}"
    print(f"  downloading {filename}...", end="", flush=True)
    urllib.request.urlretrieve(url, dest)
    size_mb = os.path.getsize(dest) / 1024 / 1024
    print(f" {size_mb:.1f} MB ✓")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Descargar tabla de zonas

# COMMAND ----------

os.makedirs(ZONES_DIR, exist_ok=True)
zones_file = f"{ZONES_DIR}/taxi_zone_lookup.csv"

if os.path.exists(zones_file) and os.path.getsize(zones_file) > 0:
    print(f"  ✓ zones ya existe ({os.path.getsize(zones_file)} bytes)")
else:
    urllib.request.urlretrieve(ZONES_URL, zones_file)
    print(f"  ✓ zones descargado ({os.path.getsize(zones_file)} bytes)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Verificación

# COMMAND ----------

print("Yellow trips:")
for f in os.listdir(TRIPS_DIR):
    size = os.path.getsize(f"{TRIPS_DIR}/{f}") / 1024 / 1024
    print(f"  {f:45s} {size:>8.1f} MB")

print(f"\nZones:")
for f in os.listdir(ZONES_DIR):
    size = os.path.getsize(f"{ZONES_DIR}/{f}") / 1024
    print(f"  {f:45s} {size:>8.1f} KB")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Prueba de lectura con Spark

# COMMAND ----------

from pyspark.sql import functions as F

df = spark.read.parquet(f"{VOLUME_PATH}/yellow_trips")
df.agg(
    F.count("*").alias("total_rows"),
    F.min("tpep_pickup_datetime").alias("min_pickup"),
    F.max("tpep_pickup_datetime").alias("max_pickup"),
).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Los estudiantes lo leen así
# MAGIC
# MAGIC ```python
# MAGIC df = spark.read.parquet("/Volumes/nytaxi_landing/raw/files/yellow_trips")
# MAGIC zones = spark.read.option("header", True).csv("/Volumes/nytaxi_landing/raw/files/zones/taxi_zone_lookup.csv")
# MAGIC ```
