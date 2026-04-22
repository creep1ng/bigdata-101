# Databricks notebook source
# MAGIC %md
# MAGIC # Configuración centralizada del pipeline NYC Taxi
# MAGIC
# MAGIC Este notebook se importa con `%run ../00-setup/config`.
# MAGIC Centraliza paths, schemas y parámetros del pipeline.
# MAGIC
# MAGIC ## Arquitectura
# MAGIC
# MAGIC - **Landing compartido**: Volume UC `/Volumes/nytaxi_landing/raw/files/` — el
# MAGIC   profesor descargó ahí los parquets de NYC TLC y todos los estudiantes leen
# MAGIC   de ahí con permisos de Unity Catalog.
# MAGIC - **Catálogo del estudiante**: `<iniciales>_nytaxi` con esquemas
# MAGIC   `bronze / silver / gold / ml`.
# MAGIC - Namespace de 3 niveles: `catálogo.esquema.tabla`.

# COMMAND ----------

# =============================================================================
# Identidad del estudiante
# =============================================================================
# Cambia estas iniciales por las tuyas antes de correr cualquier notebook.
# Ejemplo: para "Camilo Soto Montoya" → "casm"
USER_INITIALS = None

# =============================================================================
# Unity Catalog
# =============================================================================
CATALOG = f"{USER_INITIALS}_nytaxi"
SCHEMA_BRONZE = "bronze"
SCHEMA_SILVER = "silver"
SCHEMA_GOLD   = "gold"
SCHEMA_ML     = "ml"

# =============================================================================
# External Location: catálogo del estudiante (managed storage)
# =============================================================================
EXTERNAL_LOCATION = "abfss://landing@dl25604nytaxi.dfs.core.windows.net"

# =============================================================================
# Landing compartido (Volume UC, preparado por el profesor)
# =============================================================================
LANDING_CATALOG = "nytaxi_landing"
LANDING_VOLUME_PATH = f"/Volumes/{LANDING_CATALOG}/raw/files"
LANDING_TRIPS_PATH = f"{LANDING_VOLUME_PATH}/yellow_trips"
LANDING_ZONES_PATH = f"{LANDING_VOLUME_PATH}/zones"

# =============================================================================
# Esquema canónico del landing (Yellow Taxi TLC)
# =============================================================================
# Los parquets mensuales de la TLC tienen inconsistencias de tipos entre meses
# (INT vs BIGINT, airport_fee vs Airport_fee). Este esquema fuerza tipos
# uniformes para que spark.read funcione sin errores.
from pyspark.sql.types import (
    StructType, StructField, LongType, DoubleType, StringType, TimestampNTZType,
)

LANDING_SCHEMA = StructType([
    StructField("VendorID",              LongType()),
    StructField("tpep_pickup_datetime",  TimestampNTZType()),
    StructField("tpep_dropoff_datetime", TimestampNTZType()),
    StructField("passenger_count",       DoubleType()),
    StructField("trip_distance",         DoubleType()),
    StructField("RatecodeID",            DoubleType()),
    StructField("store_and_fwd_flag",    StringType()),
    StructField("PULocationID",          LongType()),
    StructField("DOLocationID",          LongType()),
    StructField("payment_type",          LongType()),
    StructField("fare_amount",           DoubleType()),
    StructField("extra",                 DoubleType()),
    StructField("mta_tax",              DoubleType()),
    StructField("tip_amount",            DoubleType()),
    StructField("tolls_amount",          DoubleType()),
    StructField("improvement_surcharge", DoubleType()),
    StructField("total_amount",          DoubleType()),
    StructField("congestion_surcharge",  DoubleType()),
    StructField("airport_fee",           DoubleType()),
])

# =============================================================================
# Tablas (3 niveles)
# =============================================================================
T_BRONZE_TRIPS = f"{CATALOG}.{SCHEMA_BRONZE}.yellow_trips"
T_BRONZE_ZONES = f"{CATALOG}.{SCHEMA_BRONZE}.taxi_zones"

T_SILVER_TRIPS          = f"{CATALOG}.{SCHEMA_SILVER}.trips_clean"
T_SILVER_TRIPS_ENRICHED = f"{CATALOG}.{SCHEMA_SILVER}.trips_enriched"
T_SILVER_REJECTED       = f"{CATALOG}.{SCHEMA_SILVER}.trips_rejected"

T_GOLD_REVENUE_BY_ZONE = f"{CATALOG}.{SCHEMA_GOLD}.revenue_by_zone"
T_GOLD_DAILY_METRICS   = f"{CATALOG}.{SCHEMA_GOLD}.daily_metrics"
T_GOLD_HOURLY_DEMAND   = f"{CATALOG}.{SCHEMA_GOLD}.hourly_demand"

T_ML_FEATURES    = f"{CATALOG}.{SCHEMA_ML}.trip_duration_features"
T_ML_PREDICTIONS = f"{CATALOG}.{SCHEMA_ML}.trip_duration_predictions"

MODEL_NAME = f"{CATALOG}.{SCHEMA_ML}.trip_duration_regressor"

# COMMAND ----------

print(f"User initials:     {USER_INITIALS}")
print(f"Catalog:           {CATALOG}")
print(f"Landing trips:     {LANDING_TRIPS_PATH}")
print(f"Landing zones:     {LANDING_ZONES_PATH}")
