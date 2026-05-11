# Databricks notebook source
# MAGIC %md
# MAGIC # Cheapest path: ruta con menor tarifa acumulada
# MAGIC
# MAGIC Similar al notebook anterior pero ahora el peso importa: cada arista suma
# MAGIC su `avg_fare` al costo total del camino. Esto es shortest path ponderado
# MAGIC implementado con recursive CTE.
# MAGIC
# MAGIC ## Advertencia importante
# MAGIC
# MAGIC Recursive CTE **no es Dijkstra**. Enumeramos caminos y nos quedamos con el
# MAGIC más barato, lo cual es aceptable para grafos pequeños y `max_hops` bajo.
# MAGIC Para caminos profundos usa NetworkX (`nx.shortest_path(..., weight='avg_fare')`)
# MAGIC en `03-networkx/`.

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

print(f"De {source_zone!r} a {target_zone!r}: camino más barato en <= {max_hops} hops")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Todos los caminos ponderados
# MAGIC
# MAGIC Igual que en `02_shortest_path_hops.py`, **no expandimos desde el destino**
# MAGIC para evitar que la recursión exceda el guardrail de 1M filas.

# COMMAND ----------

target_id = spark.sql(
    f"SELECT id FROM {T_VERTICES} WHERE zone = '{target_zone}'"
).collect()[0]["id"]

paths_df = spark.sql(f"""
    WITH RECURSIVE paths(node_id, hops, total_fare, path) AS (
      SELECT v.id, 0, CAST(0.0 AS DOUBLE), array(v.id)
      FROM {T_VERTICES} v
      WHERE v.zone = '{source_zone}'

      UNION ALL

      SELECT
        e.dst,
        p.hops + 1,
        p.total_fare + e.avg_fare,
        concat(p.path, array(e.dst))
      FROM paths p
      JOIN {T_EDGES} e ON e.src = p.node_id
      WHERE p.hops < {max_hops}
        AND e.num_trips >= {min_trips}
        AND NOT array_contains(p.path, e.dst)
        AND p.node_id != {target_id}      -- no expandir desde el destino
    )
    SELECT
      p.hops,
      ROUND(p.total_fare, 2) AS total_fare,
      p.path
    FROM paths p
    WHERE p.node_id = {target_id}
    ORDER BY p.total_fare
    LIMIT 10
""")

display(paths_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. El camino más barato con nombres

# COMMAND ----------

cheapest = paths_df.limit(1)

if cheapest.count() == 0:
    print(f"⚠ No hay camino de '{source_zone}' a '{target_zone}' "
          f"en <= {max_hops} hops con aristas de >= {min_trips} viajes.")
else:
    cheapest.createOrReplaceTempView("_cheapest")

    spark.sql(f"""
        SELECT
          s.step,
          v.id,
          v.zone,
          v.borough,
          c.total_fare AS path_total_fare
        FROM (
          SELECT posexplode(path) AS (step, node_id), total_fare
          FROM _cheapest
        ) s
        CROSS JOIN _cheapest c
        JOIN {T_VERTICES} v ON v.id = s.node_id
        ORDER BY s.step
    """).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Comparativa: camino más corto vs más barato
# MAGIC
# MAGIC Con los mismos endpoints, ¿el de menos saltos coincide con el más barato?
# MAGIC A veces dar una vuelta extra por una zona con aristas más cortas sale más
# MAGIC económico que ir directo.

# COMMAND ----------

paths_df.select(
    "hops", "total_fare", "path"
).orderBy("hops").limit(5).display()

paths_df.select(
    "hops", "total_fare", "path"
).orderBy("total_fare").limit(5).display()
