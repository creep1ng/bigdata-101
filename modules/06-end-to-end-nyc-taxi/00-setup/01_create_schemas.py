# Databricks notebook source
# MAGIC %md
# MAGIC # Setup: schemas en hive_metastore
# MAGIC
# MAGIC Crea los 4 schemas del estudiante en `hive_metastore` y configura el
# MAGIC acceso a su storage personal leyendo la access key del secret scope.
# MAGIC
# MAGIC Prerrequisitos (que el profesor ya hizo):
# MAGIC - Secret scope `nytaxi-course` existe
# MAGIC - Tu key `adls-key-<tus-iniciales>` está en el scope
# MAGIC - Tu storage account `dl25604<tus-iniciales>` tiene un container `nytaxi`
# MAGIC
# MAGIC ## Antes de correr
# MAGIC
# MAGIC Abrir `config.py` y poner tus iniciales en `USER_INITIALS`.

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Probar acceso al secret scope
# MAGIC
# MAGIC Si esto falla: el profesor aún no ha cargado tu key, o no estás en
# MAGIC el grupo con permiso READ sobre el scope.

# COMMAND ----------

try:
    key = dbutils.secrets.get(SECRET_SCOPE, SECRET_KEY_NAME)
    print(f"✓ Key leída del scope '{SECRET_SCOPE}' ({len(key)} chars, no se imprime)")
except Exception as exc:
    raise RuntimeError(
        f"No se pudo leer '{SECRET_KEY_NAME}' del scope '{SECRET_SCOPE}'. "
        f"Verifica que tu iniciales son correctas en config.py. Detalle: {exc}"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Registrar la access key en la sesión de Spark

# COMMAND ----------

configure_storage_access(spark)
print(f"✓ Storage key registrada para {STORAGE_ACCOUNT}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Verificar acceso al container `nytaxi`

# COMMAND ----------

root = f"abfss://{STORAGE_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/"
try:
    items = dbutils.fs.ls(root)
    print(f"✓ Acceso OK a {root} ({len(items)} items actualmente)")
except Exception as exc:
    raise RuntimeError(
        f"No se pudo listar {root}. "
        f"Verifica que el container 'nytaxi' existe en tu storage. Detalle: {exc}"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Crear los schemas en hive_metastore
# MAGIC
# MAGIC Cada schema queda apuntado a una carpeta física dentro del container.
# MAGIC Esto hace que las tablas managed caigan en tu storage y no en el DBFS
# MAGIC por defecto.

# COMMAND ----------

schemas = [
    (SCHEMA_BRONZE, PATH_BRONZE, "Capa Bronze: datos crudos"),
    (SCHEMA_SILVER, PATH_SILVER, "Capa Silver: limpios y enriquecidos"),
    (SCHEMA_GOLD,   PATH_GOLD,   "Capa Gold: KPIs y agregaciones"),
    (SCHEMA_ML,     PATH_ML,     "Feature tables y predicciones"),
]

for name, location, comment in schemas:
    spark.sql(f"""
        CREATE SCHEMA IF NOT EXISTS {name}
          COMMENT '{comment}'
          LOCATION '{location}'
    """)
    print(f"  ✓ {name:40s} -> {location}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Verificar

# COMMAND ----------

display(spark.sql(f"SHOW SCHEMAS LIKE '{USER_INITIALS}_nytaxi_*'"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Siguiente paso
# MAGIC
# MAGIC Ir a `../01-bronze/01_bronze_trips.py` para empezar a ingestar los datos.
