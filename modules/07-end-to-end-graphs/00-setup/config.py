# Databricks notebook source
# MAGIC %md
# MAGIC # Configuración centralizada
# MAGIC
# MAGIC Se importa con `%run ../00-setup/config` desde cualquier notebook del módulo.
# MAGIC
# MAGIC ## Dependencia con módulo 06
# MAGIC
# MAGIC El grafo se construye leyendo:
# MAGIC - `<iniciales>_nytaxi.silver.trips_clean`
# MAGIC - `<iniciales>_nytaxi.bronze.taxi_zones`
# MAGIC
# MAGIC Las tablas del módulo 07 viven en el **mismo catálogo** (`<iniciales>_nytaxi`)
# MAGIC bajo esquemas separados `graph`, `graph_gold`, `graph_viz` para no chocar con
# MAGIC las existentes del módulo 06.

# COMMAND ----------

# Tus iniciales. Deben coincidir con las del módulo 06.
USER_INITIALS = None

# =============================================================================
# Catálogo: reutilizamos el del módulo 06
# =============================================================================
CATALOG = f"{USER_INITIALS}_nytaxi"

# Esquemas del módulo 06 (solo lectura)
SCHEMA_SOURCE_BRONZE = "bronze"
SCHEMA_SOURCE_SILVER = "silver"

# Esquemas nuevos del módulo 07
SCHEMA_GRAPH = "graph"        # vertices + edges
SCHEMA_GOLD  = "graph_gold"   # resultados de algoritmos
SCHEMA_VIZ   = "graph_viz"    # tablas y assets para visualización

# =============================================================================
# Source: tablas del módulo 06 (solo lectura)
# =============================================================================
T_SOURCE_TRIPS = f"{CATALOG}.{SCHEMA_SOURCE_SILVER}.trips_clean"
T_SOURCE_ZONES = f"{CATALOG}.{SCHEMA_SOURCE_BRONZE}.taxi_zones"

# =============================================================================
# Target: tablas del grafo (se crean en 01-build-graph)
# =============================================================================
T_VERTICES = f"{CATALOG}.{SCHEMA_GRAPH}.vertices"
T_EDGES    = f"{CATALOG}.{SCHEMA_GRAPH}.edges"

# =============================================================================
# Volume para GeoJSON / shapefile de taxi zones y HTML de mapas
# =============================================================================
VIZ_VOLUME_NAME = "assets"
VIZ_VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA_VIZ}/{VIZ_VOLUME_NAME}"

# COMMAND ----------

def assert_source_ready(spark):
    """Falla con mensaje claro si el módulo 06 no corrió hasta Silver."""
    missing = [t for t in [T_SOURCE_TRIPS, T_SOURCE_ZONES]
               if not spark.catalog.tableExists(t)]
    if missing:
        raise RuntimeError(
            "Faltan tablas del módulo 06. Corre primero "
            "modules/06-end-to-end-nyc-taxi/02-silver/ hasta generar:\n  - "
            + "\n  - ".join(missing)
        )
    print("✓ Source del módulo 06 disponible:")
    print(f"    {T_SOURCE_TRIPS}")
    print(f"    {T_SOURCE_ZONES}")

# COMMAND ----------

print(f"User initials:  {USER_INITIALS}")
print(f"Catalog:        {CATALOG}")
print(f"Graph schemas:  {SCHEMA_GRAPH} / {SCHEMA_GOLD} / {SCHEMA_VIZ}")
print(f"Viz volume:     {VIZ_VOLUME_PATH}")
