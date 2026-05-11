# Databricks notebook source
# MAGIC %md
# MAGIC # Visualización con `display()` nativo
# MAGIC
# MAGIC `display()` en Databricks abre un panel con gráficos interactivos (bar,
# MAGIC scatter, pie, heatmap, etc.). Sin instalar nada.
# MAGIC
# MAGIC Este notebook muestra los resultados del módulo sobre las tablas `gold.*`.
# MAGIC Cada celda llama `display()` y el estudiante puede cambiar el tipo de
# MAGIC gráfico desde la UI (esquina superior derecha del resultado).

# COMMAND ----------

# MAGIC %run ../00-setup/config

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Top 15 zonas por PageRank
# MAGIC
# MAGIC En el panel: cambia a **Bar chart**, keys = `zone`, values = `pagerank`.

# COMMAND ----------

display(
    spark.sql(f"""
        SELECT zone, borough, ROUND(pagerank, 5) AS pagerank
        FROM {CATALOG}.{SCHEMA_GOLD}.pagerank
        ORDER BY pagerank DESC
        LIMIT 15
    """)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. PageRank por borough
# MAGIC
# MAGIC Agregando: ¿qué borough concentra más "importancia" de la red?

# COMMAND ----------

display(
    spark.sql(f"""
        SELECT borough,
               COUNT(*) AS zones,
               ROUND(SUM(pagerank), 4) AS total_pagerank,
               ROUND(AVG(pagerank), 5) AS avg_pagerank
        FROM {CATALOG}.{SCHEMA_GOLD}.pagerank
        GROUP BY borough
        ORDER BY total_pagerank DESC
    """)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Scatter: PageRank vs Betweenness
# MAGIC
# MAGIC Cambia a **Scatter plot**, x = `pagerank`, y = `betweenness`, label = `zone`.
# MAGIC Puntos arriba-a-la-derecha son zonas que son hub *y* puente.

# COMMAND ----------

display(
    spark.sql(f"""
        SELECT v.zone, v.borough,
               ROUND(pr.pagerank, 5)    AS pagerank,
               ROUND(bc.betweenness, 5) AS betweenness
        FROM {T_VERTICES} v
        JOIN {CATALOG}.{SCHEMA_GOLD}.pagerank    pr ON pr.id = v.id
        JOIN {CATALOG}.{SCHEMA_GOLD}.betweenness bc ON bc.id = v.id
    """)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Tamaño de comunidades detectadas

# COMMAND ----------

display(
    spark.sql(f"""
        SELECT community_id, COUNT(*) AS num_zones
        FROM {CATALOG}.{SCHEMA_GOLD}.communities
        GROUP BY community_id
        ORDER BY num_zones DESC
    """)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Heatmap: comunidades × boroughs
# MAGIC
# MAGIC Cambia a **Pivot table** o **Heatmap** con:
# MAGIC - rows = `community_id`
# MAGIC - columns = `borough`
# MAGIC - values = `count`

# COMMAND ----------

display(
    spark.sql(f"""
        SELECT community_id, borough, COUNT(*) AS count
        FROM {CATALOG}.{SCHEMA_GOLD}.communities
        GROUP BY community_id, borough
        ORDER BY community_id, borough
    """)
)
