# Databricks notebook source
# MAGIC %md
# MAGIC # Setup: esquemas del grafo
# MAGIC
# MAGIC Reutilizamos el catálogo `<iniciales>_nytaxi` del módulo 06 y agregamos
# MAGIC tres esquemas nuevos para el módulo 07:
# MAGIC
# MAGIC - `graph` — vértices y aristas
# MAGIC - `graph_gold` — resultados de algoritmos (PageRank, comunidades, etc.)
# MAGIC - `graph_viz` — tablas agregadas y volume `assets` para mapas/figuras
# MAGIC
# MAGIC Prefijo `graph_` para no chocar con los esquemas existentes del módulo 06
# MAGIC (`bronze`, `silver`, `gold`, `ml`).

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

spark.sql(f"USE CATALOG {CATALOG}")
print(f"✓ Catálogo activo: {CATALOG}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Esquemas

# COMMAND ----------

for schema, comment in [
    (SCHEMA_GRAPH, "Vértices y aristas del grafo de movilidad"),
    (SCHEMA_GOLD,  "Resultados de algoritmos: centralidad, comunidades, caminos"),
    (SCHEMA_VIZ,   "Tablas agregadas y assets no tabulares para visualización"),
]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema} COMMENT '{comment}'")
    print(f"  ✓ {CATALOG}.{schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Volume para assets de visualización
# MAGIC
# MAGIC Aquí se descargará el shapefile oficial de taxi zones y se guardarán los
# MAGIC HTML de los mapas de Folium.

# COMMAND ----------

spark.sql(f"""
    CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA_VIZ}.{VIZ_VOLUME_NAME}
      COMMENT 'Shapefiles, mapas HTML y figuras del módulo de grafos'
""")
print(f"  ✓ Volume: {VIZ_VOLUME_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verificación

# COMMAND ----------

display(spark.sql(f"SHOW SCHEMAS IN {CATALOG}"))

# COMMAND ----------

display(spark.sql(f"SHOW VOLUMES IN {CATALOG}.{SCHEMA_VIZ}"))
