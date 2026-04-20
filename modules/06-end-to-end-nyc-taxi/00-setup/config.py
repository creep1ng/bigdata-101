# Databricks notebook source
# MAGIC %md
# MAGIC # Configuración centralizada del pipeline NYC Taxi
# MAGIC
# MAGIC Este notebook se importa en los demás con `%run ../00-setup/config`.
# MAGIC Centraliza todos los paths, schemas y parámetros del pipeline.
# MAGIC
# MAGIC ## Arquitectura (sin Unity Catalog)
# MAGIC
# MAGIC - **Landing**: Azure Open Datasets (NYC TLC) — público, read-only, SAS "r"
# MAGIC - **Bronze/Silver/Gold**: storage personal del estudiante (`dl25604<iniciales>`)
# MAGIC - **Catálogo**: `hive_metastore` con schemas `<iniciales>_nytaxi_bronze/silver/gold/ml`
# MAGIC - **Secret scope**: `nytaxi-course` con la access key del storage personal

# COMMAND ----------

# =============================================================================
# Identidad del estudiante
# =============================================================================
# Cambiar estas iniciales por las tuyas antes de correr cualquier notebook.
# Deben coincidir con el nombre de tu storage: dl25604<USER_INITIALS>
# Ejemplo: para "Camilo Soto" cuyo storage es dl25604soto → USER_INITIALS = "soto"
USER_INITIALS = None

# =============================================================================
# Schemas en hive_metastore (dos niveles: schema.tabla)
# =============================================================================
SCHEMA_BRONZE = f"{USER_INITIALS}_nytaxi_bronze"
SCHEMA_SILVER = f"{USER_INITIALS}_nytaxi_silver"
SCHEMA_GOLD   = f"{USER_INITIALS}_nytaxi_gold"
SCHEMA_ML     = f"{USER_INITIALS}_nytaxi_ml"

# =============================================================================
# Storage del estudiante (para Bronze/Silver/Gold/ML)
# =============================================================================
STORAGE_ACCOUNT = f"dl25604{USER_INITIALS}"
STORAGE_CONTAINER = "nytaxi"

# Secret scope compartido donde viven las access keys
SECRET_SCOPE = "nytaxi-course"
SECRET_KEY_NAME = f"adls-key-{USER_INITIALS}"

# Paths abfss de las capas en TU storage
def _layer_path(layer: str) -> str:
    return f"abfss://{STORAGE_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/{layer}"

PATH_BRONZE = _layer_path("bronze")
PATH_SILVER = _layer_path("silver")
PATH_GOLD   = _layer_path("gold")
PATH_ML     = _layer_path("ml")

# Paths técnicos
PATH_CHECKPOINTS = _layer_path("_checkpoints")
PATH_SCHEMAS     = _layer_path("_schemas")

# =============================================================================
# Landing: Azure Open Datasets (público)
# =============================================================================
# https://learn.microsoft.com/en-us/azure/open-datasets/dataset-taxi-yellow
OPEN_DATASETS_ACCOUNT = "azureopendatastorage"
OPEN_DATASETS_CONTAINER = "nyctlc"
OPEN_DATASETS_PATH = "yellow"
OPEN_DATASETS_SAS = "r"  # SAS token público de lectura

LANDING_WASBS = (
    f"wasbs://{OPEN_DATASETS_CONTAINER}@{OPEN_DATASETS_ACCOUNT}"
    f".blob.core.windows.net/{OPEN_DATASETS_PATH}"
)

# =============================================================================
# Tablas del pipeline
# =============================================================================
# Bronze
T_BRONZE_TRIPS = f"{SCHEMA_BRONZE}.yellow_trips"
T_BRONZE_ZONES = f"{SCHEMA_BRONZE}.taxi_zones"

# Silver
T_SILVER_TRIPS           = f"{SCHEMA_SILVER}.trips_clean"
T_SILVER_TRIPS_ENRICHED  = f"{SCHEMA_SILVER}.trips_enriched"
T_SILVER_REJECTED        = f"{SCHEMA_SILVER}.trips_rejected"

# Gold
T_GOLD_REVENUE_BY_ZONE = f"{SCHEMA_GOLD}.revenue_by_zone"
T_GOLD_DAILY_METRICS   = f"{SCHEMA_GOLD}.daily_metrics"
T_GOLD_HOURLY_DEMAND   = f"{SCHEMA_GOLD}.hourly_demand"

# ML
T_ML_FEATURES    = f"{SCHEMA_ML}.trip_duration_features"
T_ML_PREDICTIONS = f"{SCHEMA_ML}.trip_duration_predictions"

# Modelo en el workspace MLflow registry (no en UC porque no hay UC)
MODEL_NAME = f"{USER_INITIALS}_trip_duration_regressor"

# =============================================================================
# Parámetros de ejecución
# =============================================================================
# Años/meses a procesar. Azure Open Datasets tiene datos desde 2009.
# Usa una ventana pequeña para las primeras corridas.
YEAR_FROM = 2018
YEAR_TO   = 2018      # inclusivo
MONTH_FROM = 1
MONTH_TO   = 3        # inclusivo

# =============================================================================
# Inicializador: configurar acceso al storage personal con la access key
# =============================================================================
def configure_storage_access(spark_session):
    """
    Lee la access key del secret scope y la registra en la sesión de Spark
    para que TU storage personal sea accesible vía abfss://.

    El landing NO requiere config — usa SAS público, se configura en cada
    notebook que lo use.
    """
    key = dbutils.secrets.get(SECRET_SCOPE, SECRET_KEY_NAME)
    spark_session.conf.set(
        f"fs.azure.account.key.{STORAGE_ACCOUNT}.dfs.core.windows.net",
        key,
    )
    return True


def configure_landing_access(spark_session):
    """
    Configura el SAS público de Azure Open Datasets para leer el landing.
    """
    spark_session.conf.set(
        f"fs.azure.sas.{OPEN_DATASETS_CONTAINER}.{OPEN_DATASETS_ACCOUNT}.blob.core.windows.net",
        OPEN_DATASETS_SAS,
    )
    return True

# COMMAND ----------

print(f"User initials:     {USER_INITIALS}")
print(f"Storage account:   {STORAGE_ACCOUNT}")
print(f"Container:         {STORAGE_CONTAINER}")
print(f"Schema Bronze:     {SCHEMA_BRONZE}")
print(f"Schema Silver:     {SCHEMA_SILVER}")
print(f"Schema Gold:       {SCHEMA_GOLD}")
print(f"Schema ML:         {SCHEMA_ML}")
print(f"Landing:           {LANDING_WASBS}")
print(f"Secret scope/key:  {SECRET_SCOPE} / {SECRET_KEY_NAME}")
print(f"Period:            {YEAR_FROM}-{MONTH_FROM:02d} to {YEAR_TO}-{MONTH_TO:02d}")
