# Databricks notebook source
# MAGIC %md
# MAGIC # Reachability: ¿a dónde puedo llegar desde X?
# MAGIC
# MAGIC Pregunta: partiendo de una zona origen, ¿qué zonas puedo alcanzar pasando
# MAGIC por a lo más `max_hops` saltos, usando solo aristas con `num_trips >= min_trips`?
# MAGIC
# MAGIC Este es el uso canónico de `WITH RECURSIVE` para grafos: **transitive closure
# MAGIC acotado por profundidad**.
# MAGIC
# MAGIC ## Estructura del recursive CTE
# MAGIC
# MAGIC 1. **Base case**: solo el nodo origen, con `hops = 0` y `path = [origen]`
# MAGIC 2. **Recursive step**: por cada nodo alcanzado, seguir cualquier arista
# MAGIC    saliente que no regrese a un nodo ya visitado (control de ciclos)
# MAGIC 3. **Stop**: cuando `hops >= max_hops` o no haya más aristas que seguir

# COMMAND ----------

# MAGIC %run ../00-setup/config

# COMMAND ----------

dbutils.widgets.text("source_zone", "Times Sq/Theatre District", "Zona origen")
dbutils.widgets.text("max_hops", "3", "Máximo número de saltos")
dbutils.widgets.text("min_trips", "100", "Viajes mínimos por arista")

source_zone = dbutils.widgets.get("source_zone")
max_hops    = int(dbutils.widgets.get("max_hops"))
min_trips   = int(dbutils.widgets.get("min_trips"))

print(f"Desde: {source_zone}")
print(f"Hasta: {max_hops} hops, aristas con >= {min_trips} viajes")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Reachability

# COMMAND ----------

reachable_df = spark.sql(f"""
    WITH RECURSIVE reachable(node_id, hops, path) AS (
      -- Base case: nodo origen
      SELECT v.id, 0, array(v.id)
      FROM {T_VERTICES} v
      WHERE v.zone = '{source_zone}'

      UNION ALL

      -- Recursive step: expandir por aristas salientes
      SELECT
        e.dst,
        r.hops + 1,
        concat(r.path, array(e.dst))
      FROM reachable r
      JOIN {T_EDGES} e ON e.src = r.node_id
      WHERE r.hops < {max_hops}
        AND e.num_trips >= {min_trips}
        AND NOT array_contains(r.path, e.dst)    -- evitar ciclos
    )
    SELECT
      r.node_id,
      v.zone,
      v.borough,
      MIN(r.hops) AS min_hops
    FROM reachable r
    JOIN {T_VERTICES} v ON v.id = r.node_id
    WHERE r.hops > 0                               -- excluir el propio origen
    GROUP BY r.node_id, v.zone, v.borough
    ORDER BY min_hops, v.borough, v.zone
""")

display(reachable_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Resumen: alcance por número de saltos

# COMMAND ----------

reachable_df.groupBy("min_hops").count().orderBy("min_hops").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Cobertura por borough
# MAGIC
# MAGIC ¿Qué fracción de cada borough puedo alcanzar desde la zona origen?

# COMMAND ----------

reachable_df.createOrReplaceTempView("_reachable")

spark.sql(f"""
    SELECT
      v.borough,
      COUNT(DISTINCT v.id) AS total_zones,
      COUNT(DISTINCT r.node_id) AS reached_zones,
      ROUND(100.0 * COUNT(DISTINCT r.node_id) / COUNT(DISTINCT v.id), 1) AS pct_reached
    FROM {T_VERTICES} v
    LEFT JOIN _reachable r ON v.id = r.node_id
    GROUP BY v.borough
    ORDER BY pct_reached DESC
""").display()
