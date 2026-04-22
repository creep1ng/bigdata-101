# Databricks notebook source
# MAGIC %md
# MAGIC # Setup: catálogo personal en Unity Catalog
# MAGIC
# MAGIC Crea el catálogo del estudiante con sus esquemas Medallion y un volume
# MAGIC para archivos no tabulares. Cada catálogo vive en la External Location
# MAGIC compartida `nyctaxi_lake`, en su propia subcarpeta.
# MAGIC
# MAGIC ## Antes de correr
# MAGIC Abrir `config.py` y poner tus iniciales en `USER_INITIALS`.

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Crear catálogo managed en la External Location compartida

# COMMAND ----------

managed_location = f"{EXTERNAL_LOCATION}/{CATALOG}"

spark.sql(f"""
    CREATE CATALOG IF NOT EXISTS {CATALOG}
      MANAGED LOCATION '{managed_location}'
      COMMENT 'Lakehouse NYC Taxi — estudiante {USER_INITIALS}'
""")

spark.sql(f"USE CATALOG {CATALOG}")
print(f"✓ Catálogo activo: {CATALOG}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Esquemas Medallion + ML

# COMMAND ----------

for schema, comment in [
    (SCHEMA_BRONZE, "Capa Bronze: datos crudos tal cual llegan"),
    (SCHEMA_SILVER, "Capa Silver: datos limpios y validados"),
    (SCHEMA_GOLD,   "Capa Gold: KPIs y agregaciones de negocio"),
    (SCHEMA_ML,     "Features, predicciones y modelos de ML"),
]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema} COMMENT '{comment}'")
    print(f"  ✓ {CATALOG}.{schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Volume para archivos no tabulares (landing local)

# COMMAND ----------

spark.sql(f"""
    CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA_BRONZE}.{VOLUME_LANDING}
      COMMENT 'Archivos no tabulares (CSVs, etc.)'
""")
print(f"  ✓ Volume: {CATALOG}.{SCHEMA_BRONZE}.{VOLUME_LANDING}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Verificación

# COMMAND ----------

display(spark.sql(f"SHOW SCHEMAS IN {CATALOG}"))

# COMMAND ----------

display(spark.sql(f"SHOW VOLUMES IN {CATALOG}.{SCHEMA_BRONZE}"))
