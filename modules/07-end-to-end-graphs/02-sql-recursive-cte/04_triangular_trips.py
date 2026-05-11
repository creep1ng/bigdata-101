# Databricks notebook source
# MAGIC %md
# MAGIC # Triangular trips: ciclos A → B → C → A
# MAGIC
# MAGIC ¿Existen tríos de zonas A, B, C donde hay flujo observado A→B, B→C y
# MAGIC C→A? Esto captura **patrones circulares de movilidad**: por ejemplo
# MAGIC turistas que salen de hotel → atracción → restaurante → hotel.
# MAGIC
# MAGIC ## Nota técnica
# MAGIC
# MAGIC Triangle counting en grafos dirigidos NO necesita recursive CTE — se
# MAGIC resuelve con 3 joins. Lo implementamos de las dos formas para mostrar que
# MAGIC **no todo problema de grafos necesita recursión**: cuando el patrón tiene
# MAGIC longitud fija, joins directos es más eficiente.

# COMMAND ----------

# MAGIC %run ../00-setup/config

# COMMAND ----------

dbutils.widgets.text("min_trips", "500", "Viajes mínimos por arista del triángulo")

min_trips = int(dbutils.widgets.get("min_trips"))
print(f"Buscando triángulos con cada arista >= {min_trips} viajes")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Enfoque recomendado: 3 joins
# MAGIC
# MAGIC Para eliminar duplicados (A→B→C→A, B→C→A→B y C→A→B→C son el mismo
# MAGIC triángulo), quedamos con la permutación donde `a < b AND a < c`.

# COMMAND ----------

triangles = spark.sql(f"""
    SELECT
      va.id AS a_id, va.zone AS a_zone, va.borough AS a_borough,
      vb.id AS b_id, vb.zone AS b_zone, vb.borough AS b_borough,
      vc.id AS c_id, vc.zone AS c_zone, vc.borough AS c_borough,
      e1.num_trips AS a_to_b,
      e2.num_trips AS b_to_c,
      e3.num_trips AS c_to_a,
      e1.num_trips + e2.num_trips + e3.num_trips AS total_trips
    FROM {T_EDGES} e1
    JOIN {T_EDGES} e2 ON e1.dst = e2.src
    JOIN {T_EDGES} e3 ON e2.dst = e3.src AND e3.dst = e1.src
    JOIN {T_VERTICES} va ON va.id = e1.src
    JOIN {T_VERTICES} vb ON vb.id = e2.src
    JOIN {T_VERTICES} vc ON vc.id = e3.src
    WHERE e1.num_trips >= {min_trips}
      AND e2.num_trips >= {min_trips}
      AND e3.num_trips >= {min_trips}
      AND e1.src < e2.src                          -- evita permutaciones
      AND e1.src < e3.src
""")

print(f"Triángulos encontrados: {triangles.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Top triángulos por volumen total

# COMMAND ----------

triangles.orderBy("total_trips", ascending=False).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Triángulos entre boroughs distintos
# MAGIC
# MAGIC Un triángulo donde las tres zonas están en el mismo borough suele ser
# MAGIC trivial (movilidad interna). Más interesante: triángulos que cruzan
# MAGIC fronteras.

# COMMAND ----------

from pyspark.sql import functions as F

(
    triangles
    .filter((F.col("a_borough") != F.col("b_borough"))
          | (F.col("b_borough") != F.col("c_borough")))
    .orderBy("total_trips", ascending=False)
    .display()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bonus: misma pregunta con recursive CTE
# MAGIC
# MAGIC Solo para mostrar la sintaxis. Es ~10x más lento que la versión con 3
# MAGIC joins, pero útil cuando la longitud del ciclo es variable.
# MAGIC
# MAGIC **Pruning de canonización**: para que cada triángulo se cuente una sola
# MAGIC vez y no explotemos el guardrail de 1M filas, la recursión solo extiende
# MAGIC a nodos con ID mayor que `start_id`, y solo permite cerrar el ciclo en
# MAGIC `depth = 2`. Así, de las 3 rotaciones posibles de cada triángulo
# MAGIC (A→B→C→A, B→C→A→B, C→A→B→C) solo sobrevive la canónica que empieza por
# MAGIC el nodo de menor ID.

# COMMAND ----------

spark.sql(f"""
    WITH RECURSIVE cycles(start_id, node_id, depth, path) AS (
      SELECT v.id, v.id, 0, array(v.id)
      FROM {T_VERTICES} v

      UNION ALL

      SELECT
        c.start_id,
        e.dst,
        c.depth + 1,
        concat(c.path, array(e.dst))
      FROM cycles c
      JOIN {T_EDGES} e ON e.src = c.node_id
      WHERE c.depth < 3
        AND e.num_trips >= {min_trips}
        AND (
          -- cerrando ciclo: solo en depth = 2 (triángulo exacto)
          (e.dst = c.start_id AND c.depth = 2)
          OR
          -- extendiendo: solo a nodos mayores que start, y no visitados
          (e.dst > c.start_id AND NOT array_contains(c.path, e.dst))
        )
    )
    SELECT COUNT(*) AS triangles_via_recursive_cte
    FROM cycles
    WHERE depth = 3 AND node_id = start_id
""").display()
