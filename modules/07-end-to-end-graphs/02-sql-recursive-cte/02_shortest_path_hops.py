# Databricks notebook source
# MAGIC %md
# MAGIC # Shortest path: menor número de saltos entre A y B
# MAGIC
# MAGIC BFS clásico con recursive CTE. Nos importa **solo el número de saltos**,
# MAGIC ignorando el peso de cada arista. Para el camino más barato ver
# MAGIC `03_cheapest_path.py`.
# MAGIC
# MAGIC ## Idea
# MAGIC
# MAGIC Recursión idéntica a reachability, pero:
# MAGIC
# MAGIC 1. Filtramos hasta llegar a la zona destino
# MAGIC 2. Nos quedamos con la ruta de menor profundidad

# COMMAND ----------

# MAGIC %run ../00-setup/config

# COMMAND ----------

dbutils.widgets.text("source_zone", "JFK Airport", "Zona origen")
dbutils.widgets.text("target_zone", "Times Sq/Theatre District", "Zona destino")
dbutils.widgets.text("max_hops", "3", "Máximo número de saltos")
dbutils.widgets.text("min_trips", "50", "Viajes mínimos por arista")

source_zone = dbutils.widgets.get("source_zone")
target_zone = dbutils.widgets.get("target_zone")
max_hops    = int(dbutils.widgets.get("max_hops"))
min_trips   = int(dbutils.widgets.get("min_trips"))

print(f"De {source_zone!r} a {target_zone!r} en <= {max_hops} hops")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Todos los caminos de A a B
# MAGIC
# MAGIC Truco de optimización: **no expandimos desde el nodo destino**. Una vez
# MAGIC que un path llega, lo guardamos pero no seguimos explorando desde ahí.
# MAGIC Sin este pruning, con `max_hops=4` y ~50K aristas la recursión supera
# MAGIC el guardrail de 1M filas.

# COMMAND ----------

target_id = spark.sql(
    f"SELECT id FROM {T_VERTICES} WHERE zone = '{target_zone}'"
).collect()[0]["id"]

paths_df = spark.sql(f"""
    WITH RECURSIVE paths(node_id, hops, path) AS (
      SELECT v.id, 0, array(v.id)
      FROM {T_VERTICES} v
      WHERE v.zone = '{source_zone}'

      UNION ALL

      SELECT
        e.dst,
        p.hops + 1,
        concat(p.path, array(e.dst))
      FROM paths p
      JOIN {T_EDGES} e ON e.src = p.node_id
      WHERE p.hops < {max_hops}
        AND e.num_trips >= {min_trips}
        AND NOT array_contains(p.path, e.dst)
        AND p.node_id != {target_id}      -- no expandir desde el destino
    )
    SELECT p.hops, p.path
    FROM paths p
    WHERE p.node_id = {target_id}
    ORDER BY p.hops
""")

display(paths_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. El camino más corto, con nombres de zonas

# COMMAND ----------

shortest = paths_df.limit(1)

if shortest.count() == 0:
    print(f"⚠ No hay camino de '{source_zone}' a '{target_zone}' "
          f"en <= {max_hops} hops con aristas de >= {min_trips} viajes.")
    print(f"  Prueba a subir max_hops o bajar min_trips.")
else:
    shortest.createOrReplaceTempView("_shortest")
    spark.sql(f"""
        SELECT
          posexplode(path) AS (step, node_id),
          hops AS total_hops
        FROM _shortest
    """).createOrReplaceTempView("_shortest_steps")

    spark.sql(f"""
        SELECT
          s.step,
          v.id,
          v.zone,
          v.borough
        FROM _shortest_steps s
        JOIN {T_VERTICES} v ON v.id = s.node_id
        ORDER BY s.step
    """).display()
