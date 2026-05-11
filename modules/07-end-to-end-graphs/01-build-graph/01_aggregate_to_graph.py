# Databricks notebook source
# MAGIC %md
# MAGIC # Construcción del grafo
# MAGIC
# MAGIC Transforma las 3-4M filas de `silver.trips_clean` en un grafo dirigido ponderado:
# MAGIC
# MAGIC - **Vértices** = 265 taxi zones (desde `bronze.taxi_zones`)
# MAGIC - **Aristas** = pares `(PULocationID, DOLocationID)` con métricas agregadas
# MAGIC
# MAGIC Persistimos las dos tablas en Delta bajo Unity Catalog. El resto del módulo
# MAGIC lee de ahí — ni las recursive CTEs ni NetworkX tocan `silver.trips_clean`
# MAGIC directamente.
# MAGIC
# MAGIC ## Decisiones de diseño
# MAGIC
# MAGIC - **Self-loops excluidos** (src == dst). Un viaje dentro de la misma zona no es
# MAGIC   una arista de movilidad entre zonas. Cambia el filtro si los necesitas.
# MAGIC - **No filtramos por número mínimo de viajes**. La tabla `edges` guarda todas
# MAGIC   las combinaciones observadas; cada notebook de análisis filtra con un
# MAGIC   widget `min_trips` si lo necesita.
# MAGIC - **Sin filtro temporal**. Usamos todo lo que haya en `silver.trips_clean`.

# COMMAND ----------

# MAGIC %run ../00-setup/config

# COMMAND ----------

from pyspark.sql import functions as F

spark.sql(f"USE CATALOG {CATALOG}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Vértices
# MAGIC
# MAGIC Una fila por zona. `id` es el LocationID que viene en los viajes.

# COMMAND ----------

vertices = (
    spark.table(T_SOURCE_ZONES)
    .select(
        F.col("LocationID").cast("long").alias("id"),
        F.col("Zone").alias("zone"),
        F.col("Borough").alias("borough"),
        F.col("service_zone").alias("service"),
    )
)

(
    vertices.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(T_VERTICES)
)

print(f"✓ {T_VERTICES}: {spark.table(T_VERTICES).count()} vértices")

# COMMAND ----------

display(spark.table(T_VERTICES).orderBy("borough", "zone"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Aristas
# MAGIC
# MAGIC Agregamos los viajes de Silver agrupando por origen y destino. Cada par
# MAGIC `(src, dst)` se convierte en una arista con:
# MAGIC
# MAGIC - `num_trips`: total de viajes observados
# MAGIC - `avg_fare`: tarifa promedio (proxy de distancia/costo)
# MAGIC - `avg_duration_min`: duración promedio
# MAGIC - `total_revenue`: facturación total de la arista

# COMMAND ----------

edges = (
    spark.table(T_SOURCE_TRIPS)
    .where(F.col("PULocationID") != F.col("DOLocationID"))    # sin self-loops
    .groupBy(
        F.col("PULocationID").cast("long").alias("src"),
        F.col("DOLocationID").cast("long").alias("dst"),
    )
    .agg(
        F.count("*").alias("num_trips"),
        F.round(F.avg("total_amount"), 2).alias("avg_fare"),
        F.round(F.avg("trip_duration_min"), 2).alias("avg_duration_min"),
        F.round(F.sum("total_amount"), 2).alias("total_revenue"),
    )
)

(
    edges.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(T_EDGES)
)

print(f"✓ {T_EDGES}: {spark.table(T_EDGES).count()} aristas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Verificación

# COMMAND ----------

# MAGIC %md
# MAGIC ### Integridad referencial: ¿todas las aristas apuntan a vértices conocidos?

# COMMAND ----------

spark.sql(f"""
    WITH orphan_src AS (
      SELECT COUNT(*) AS n
      FROM {T_EDGES} e
      LEFT ANTI JOIN {T_VERTICES} v ON e.src = v.id
    ),
    orphan_dst AS (
      SELECT COUNT(*) AS n
      FROM {T_EDGES} e
      LEFT ANTI JOIN {T_VERTICES} v ON e.dst = v.id
    )
    SELECT
      (SELECT n FROM orphan_src) AS edges_without_src_vertex,
      (SELECT n FROM orphan_dst) AS edges_without_dst_vertex
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Distribución de pesos de aristas
# MAGIC
# MAGIC Útil para elegir el umbral de `min_trips` cuando filtremos ruido en los
# MAGIC notebooks de análisis.

# COMMAND ----------

spark.sql(f"""
    SELECT
      COUNT(*) AS total_edges,
      SUM(CASE WHEN num_trips >= 10   THEN 1 ELSE 0 END) AS with_10_plus,
      SUM(CASE WHEN num_trips >= 100  THEN 1 ELSE 0 END) AS with_100_plus,
      SUM(CASE WHEN num_trips >= 1000 THEN 1 ELSE 0 END) AS with_1000_plus,
      MIN(num_trips)     AS min_trips,
      MAX(num_trips)     AS max_trips,
      PERCENTILE(num_trips, 0.5)  AS median_trips,
      PERCENTILE(num_trips, 0.9)  AS p90_trips,
      PERCENTILE(num_trips, 0.99) AS p99_trips
    FROM {T_EDGES}
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Top 10 aristas por volumen de viajes

# COMMAND ----------

spark.sql(f"""
    SELECT
      e.src, vs.zone AS src_zone, vs.borough AS src_borough,
      e.dst, vd.zone AS dst_zone, vd.borough AS dst_borough,
      e.num_trips, e.avg_fare, e.avg_duration_min, e.total_revenue
    FROM {T_EDGES} e
    JOIN {T_VERTICES} vs ON e.src = vs.id
    JOIN {T_VERTICES} vd ON e.dst = vd.id
    ORDER BY e.num_trips DESC
    LIMIT 10
""").display()
