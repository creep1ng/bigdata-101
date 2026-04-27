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
# Cada alumno tiene su propio storage account: dl25604<iniciales>
EXTERNAL_LOCATION = f"abfss://landing@dl25604{USER_INITIALS}.dfs.core.windows.net"

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
# Inconsistencias detectadas en los Parquet mensuales de la TLC (2023):
#
#   Tipos que cambian entre enero y el resto del año:
#     VendorID        → Ene: BIGINT,  Feb–Dic: INT
#     PULocationID    → Ene: BIGINT,  Feb–Dic: INT
#     DOLocationID    → Ene: BIGINT,  Feb–Dic: INT
#     RatecodeID      → Ene: DOUBLE,  Feb–Dic: BIGINT
#     passenger_count → Ene: DOUBLE,  Feb–Dic: BIGINT
#
#   Nombre de columna que cambia:
#     airport_fee     → solo en Ene (minúscula)
#     Airport_fee     → Feb–Dic (mayúscula inicial)
#
# Estrategia:
#   Definimos un esquema nativo por variante (v1 = Ene, v2 = Feb–Dic) para
#   leer sin errores, y luego casteamos al esquema unificado (UNIFIED_SCHEMA).
# =============================================================================
from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, LongType, DoubleType, StringType, TimestampNTZType,
)

# ── Esquema unificado: el esquema final que tendrán TODOS los DataFrames ─────
UNIFIED_SCHEMA = StructType([
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

# Para compatibilidad con código existente que referencie LANDING_SCHEMA
LANDING_SCHEMA = UNIFIED_SCHEMA

# ── Esquemas nativos por variante ────────────────────────────────────────────
# v1: Enero 2023 — tipos originales del TLC para ese mes
_SCHEMA_V1 = StructType([
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
    StructField("airport_fee",           DoubleType()),       # minúscula
])

# v2: Feb–Dic 2023 — tipos cambiados por la TLC
_SCHEMA_V2 = StructType([
    StructField("VendorID",              IntegerType()),
    StructField("tpep_pickup_datetime",  TimestampNTZType()),
    StructField("tpep_dropoff_datetime", TimestampNTZType()),
    StructField("passenger_count",       LongType()),
    StructField("trip_distance",         DoubleType()),
    StructField("RatecodeID",            LongType()),
    StructField("store_and_fwd_flag",    StringType()),
    StructField("PULocationID",          IntegerType()),
    StructField("DOLocationID",          IntegerType()),
    StructField("payment_type",          LongType()),
    StructField("fare_amount",           DoubleType()),
    StructField("extra",                 DoubleType()),
    StructField("mta_tax",              DoubleType()),
    StructField("tip_amount",            DoubleType()),
    StructField("tolls_amount",          DoubleType()),
    StructField("improvement_surcharge", DoubleType()),
    StructField("total_amount",          DoubleType()),
    StructField("congestion_surcharge",  DoubleType()),
    StructField("Airport_fee",           DoubleType()),       # Mayúscula
])

# ── Mapeo: qué variante de esquema usa cada mes ─────────────────────────────
# Agregar aquí si se incorporan meses de otros años con esquemas distintos.
_MONTH_SCHEMA_MAP = {
    "2023-01": "v1",
    # Feb–Dic 2023 usan v2
    "2023-02": "v2", "2023-03": "v2", "2023-04": "v2",
    "2023-05": "v2", "2023-06": "v2", "2023-07": "v2",
    "2023-08": "v2", "2023-09": "v2", "2023-10": "v2",
    "2023-11": "v2", "2023-12": "v2",
}

_SCHEMA_VARIANTS = {
    "v1": _SCHEMA_V1,
    "v2": _SCHEMA_V2,
}

# Columnas que necesitan casteo de v1 → unificado (ninguna, ya coincide)
# Columnas que necesitan casteo de v2 → unificado
_V2_CASTS = {
    "VendorID":        "long",
    "PULocationID":    "long",
    "DOLocationID":    "long",
    "passenger_count": "double",
    "RatecodeID":      "double",
}

# Columnas que necesitan renombrar por variante
_V2_RENAMES = {
    "Airport_fee": "airport_fee",
}


def _detect_variant(filename):
    """Extrae YYYY-MM del nombre del archivo y devuelve la variante de esquema."""
    import re
    match = re.search(r"(\d{4}-\d{2})", filename)
    if match:
        month_key = match.group(1)
        return _MONTH_SCHEMA_MAP.get(month_key, "v2")  # default v2 para meses nuevos
    return "v2"


def _read_and_unify(spark, file_path, variant):
    """Lee un Parquet con su esquema nativo y lo transforma al esquema unificado."""
    from pyspark.sql import functions as F

    schema = _SCHEMA_VARIANTS[variant]
    df = (
        spark.read.schema(schema).parquet(file_path)
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )

    # Aplicar renombramientos
    renames = _V2_RENAMES if variant == "v2" else {}
    for old_name, new_name in renames.items():
        if old_name in df.columns:
            df = df.withColumnRenamed(old_name, new_name)

    # Aplicar casteos al esquema unificado
    casts = _V2_CASTS if variant == "v2" else {}
    for col_name, target_type in casts.items():
        df = df.withColumn(col_name, F.col(col_name).cast(target_type))

    return df


def read_yellow_trips(spark, path=None):
    """Lee todos los Parquet del landing, normalizando esquemas por mes.

    Cada archivo se lee con su esquema nativo (v1 o v2 según el mes) y se
    transforma al UNIFIED_SCHEMA. Luego se unen con unionByName.
    """
    from functools import reduce

    if path is None:
        path = LANDING_TRIPS_PATH

    files = [f for f in dbutils.fs.ls(path) if f.name.endswith(".parquet")]

    if not files:
        raise FileNotFoundError(f"No se encontraron archivos .parquet en {path}")

    dfs = []
    for f in files:
        variant = _detect_variant(f.name)
        df = _read_and_unify(spark, f.path, variant)
        dfs.append(df)
        print(f"  ✓ {f.name:45s} → esquema {variant}")

    return reduce(lambda a, b: a.unionByName(b), dfs)

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
