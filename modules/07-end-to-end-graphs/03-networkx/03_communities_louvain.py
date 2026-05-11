# Databricks notebook source
# MAGIC %md
# MAGIC # Community detection: ¿qué barrios funcionales emergen?
# MAGIC
# MAGIC Louvain encuentra grupos de nodos densamente conectados entre sí y poco
# MAGIC conectados con el resto. En nuestro grafo, esto revela **barrios
# MAGIC funcionales** según cómo se mueve la gente, no según fronteras
# MAGIC administrativas.
# MAGIC
# MAGIC Hipótesis: las comunidades detectadas van a cruzar líneas de borough. Por
# MAGIC ejemplo, "aeropuertos + Midtown + zonas turísticas" podrían formar una
# MAGIC comunidad, diferente de "Brooklyn residencial + Queens residencial".
# MAGIC
# MAGIC ## Nota técnica
# MAGIC
# MAGIC Louvain requiere grafo **no dirigido**. Convertimos el DiGraph a Graph
# MAGIC sumando los pesos de A→B y B→A en una única arista.

# COMMAND ----------

# MAGIC %run ../00-setup/config

# COMMAND ----------

import networkx as nx
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Cargar grafo como DiGraph

# COMMAND ----------

vertices_pdf = spark.table(T_VERTICES).toPandas()
edges_pdf    = spark.table(T_EDGES).toPandas()

DiG = nx.from_pandas_edgelist(
    edges_pdf,
    source="src", target="dst",
    edge_attr=["num_trips"],
    create_using=nx.DiGraph,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Convertir a undirected sumando pesos
# MAGIC
# MAGIC Si tenemos A→B con 500 viajes y B→A con 300 viajes, la arista undirected
# MAGIC A↔B queda con peso 800.

# COMMAND ----------

UG = nx.Graph()

for u, v, data in DiG.edges(data=True):
    w = data["num_trips"]
    if UG.has_edge(u, v):
        UG[u][v]["weight"] += w
    else:
        UG.add_edge(u, v, weight=w)

print(f"DiGraph original: {DiG.number_of_edges()} aristas dirigidas")
print(f"Graph undirected: {UG.number_of_edges()} aristas no dirigidas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Louvain
# MAGIC
# MAGIC Retorna una lista de sets: cada set es una comunidad con los IDs de sus
# MAGIC nodos. El algoritmo es estocástico, fijamos `seed` para reproducibilidad.

# COMMAND ----------

communities = nx.community.louvain_communities(UG, weight="weight", seed=42)

print(f"Comunidades encontradas: {len(communities)}")
print(f"Tamaños: {sorted([len(c) for c in communities], reverse=True)}")
print(f"Modularity: {nx.community.modularity(UG, communities, weight='weight'):.4f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Persistir asignación nodo → comunidad

# COMMAND ----------

rows = []
for comm_id, members in enumerate(communities):
    for node_id in members:
        rows.append((int(node_id), int(comm_id)))

comm_df = spark.createDataFrame(rows, schema="id LONG, community_id LONG")
comm_df = (
    comm_df.join(spark.table(T_VERTICES), on="id", how="left")
    .select("id", "zone", "borough", "service", "community_id")
)

t_communities = f"{CATALOG}.{SCHEMA_GOLD}.communities"
comm_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(t_communities)

print(f"✓ {t_communities}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Análisis: ¿qué son las comunidades detectadas?

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tamaño y borough dominante de cada comunidad

# COMMAND ----------

spark.sql(f"""
    WITH borough_counts AS (
      SELECT community_id, borough, COUNT(*) AS n
      FROM {t_communities}
      GROUP BY community_id, borough
    ),
    ranked AS (
      SELECT community_id, borough, n,
             ROW_NUMBER() OVER (PARTITION BY community_id ORDER BY n DESC) AS rk
      FROM borough_counts
    )
    SELECT
      c.community_id,
      COUNT(*) AS num_zones,
      FIRST(r.borough) AS dominant_borough,
      FIRST(r.n) AS dominant_count,
      COUNT(DISTINCT c.borough) AS distinct_boroughs
    FROM {t_communities} c
    LEFT JOIN ranked r
      ON c.community_id = r.community_id AND r.rk = 1
    GROUP BY c.community_id
    ORDER BY num_zones DESC
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Comunidades multi-borough: ¿qué zonas comparten comunidad cruzando fronteras?

# COMMAND ----------

spark.sql(f"""
    WITH multi_borough AS (
      SELECT community_id
      FROM {t_communities}
      GROUP BY community_id
      HAVING COUNT(DISTINCT borough) > 1
    )
    SELECT c.community_id, c.zone, c.borough, c.service
    FROM {t_communities} c
    JOIN multi_borough m ON c.community_id = m.community_id
    ORDER BY c.community_id, c.borough, c.zone
""").display()
