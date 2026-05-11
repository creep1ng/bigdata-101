# Databricks notebook source
# MAGIC %md
# MAGIC # Centralidad: ¿qué zonas son "importantes" en la red?
# MAGIC
# MAGIC Tres medidas complementarias de centralidad:
# MAGIC
# MAGIC - **Degree centrality** — número relativo de conexiones directas
# MAGIC - **PageRank** — importancia recursiva (soy importante si gente importante me apunta)
# MAGIC - **Betweenness centrality** — frecuencia con la que una zona aparece en los caminos más cortos entre otras zonas
# MAGIC
# MAGIC Los tres se calculan con NetworkX y se persisten como tablas Delta en `gold`.

# COMMAND ----------

# MAGIC %run ../00-setup/config

# COMMAND ----------

import networkx as nx
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Cargar grafo

# COMMAND ----------

vertices_pdf = spark.table(T_VERTICES).toPandas()
edges_pdf    = spark.table(T_EDGES).toPandas()

G = nx.from_pandas_edgelist(
    edges_pdf,
    source="src", target="dst",
    edge_attr=["num_trips", "avg_fare"],
    create_using=nx.DiGraph,
)

print(f"G: {G.number_of_nodes()} nodos, {G.number_of_edges()} aristas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Degree centrality
# MAGIC
# MAGIC NetworkX normaliza por `(N-1)` para que el valor esté en [0, 1].
# MAGIC Para grafos dirigidos hay in-degree y out-degree separados.

# COMMAND ----------

in_cent  = nx.in_degree_centrality(G)
out_cent = nx.out_degree_centrality(G)

degree_df = spark.createDataFrame(
    [(int(n), float(in_cent[n]), float(out_cent[n])) for n in G.nodes],
    schema="id LONG, in_degree_centrality DOUBLE, out_degree_centrality DOUBLE",
)

degree_df = (
    degree_df.join(spark.table(T_VERTICES), on="id", how="left")
    .select("id", "zone", "borough", "in_degree_centrality", "out_degree_centrality")
)

t_degree = f"{CATALOG}.{SCHEMA_GOLD}.degree_centrality"
degree_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(t_degree)

print(f"✓ {t_degree}")
spark.table(t_degree).orderBy(F.desc("in_degree_centrality")).limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. PageRank ponderado
# MAGIC
# MAGIC Un nodo es importante si **muchos viajes** terminan ahí, dándole peso extra
# MAGIC a los que vienen de zonas que a su vez son importantes.
# MAGIC
# MAGIC Peso = `num_trips`. Si quieres usar facturación cambia a `total_revenue`
# MAGIC al construir el grafo.

# COMMAND ----------

pr = nx.pagerank(G, alpha=0.85, weight="num_trips")

pr_df = spark.createDataFrame(
    [(int(n), float(pr[n])) for n in G.nodes],
    schema="id LONG, pagerank DOUBLE",
)

pr_df = (
    pr_df.join(spark.table(T_VERTICES), on="id", how="left")
    .select("id", "zone", "borough", "service", "pagerank")
)

t_pagerank = f"{CATALOG}.{SCHEMA_GOLD}.pagerank"
pr_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(t_pagerank)

print(f"✓ {t_pagerank}")
spark.table(t_pagerank).orderBy(F.desc("pagerank")).limit(15).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Betweenness centrality
# MAGIC
# MAGIC Fracción de caminos más cortos (entre todos los pares) que pasan por cada
# MAGIC nodo. Captura nodos **puente** — si desaparecen, muchos caminos se rompen.
# MAGIC
# MAGIC NetworkX calcula esto con Brandes' algorithm, O(V·E) con peso. Con ~265
# MAGIC nodos tarda pocos segundos. Usamos `avg_fare` como peso de arista porque
# MAGIC es proxy de costo/distancia — caminos más cortos = caminos más baratos.

# COMMAND ----------

bc = nx.betweenness_centrality(G, weight="avg_fare")

bc_df = spark.createDataFrame(
    [(int(n), float(bc[n])) for n in G.nodes],
    schema="id LONG, betweenness DOUBLE",
)

bc_df = (
    bc_df.join(spark.table(T_VERTICES), on="id", how="left")
    .select("id", "zone", "borough", "betweenness")
)

t_betweenness = f"{CATALOG}.{SCHEMA_GOLD}.betweenness"
bc_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(t_betweenness)

print(f"✓ {t_betweenness}")
spark.table(t_betweenness).orderBy(F.desc("betweenness")).limit(15).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Comparativa: ¿coinciden las tres medidas?
# MAGIC
# MAGIC Las zonas con alto PageRank (hubs naturales) no siempre coinciden con las
# MAGIC de alta betweenness (nodos puente). Lo vemos lado a lado.

# COMMAND ----------

spark.sql(f"""
    SELECT
      v.zone,
      v.borough,
      ROUND(pr.pagerank, 5)    AS pagerank,
      ROUND(bc.betweenness, 5) AS betweenness,
      ROUND(dc.in_degree_centrality,  3) AS in_cent,
      ROUND(dc.out_degree_centrality, 3) AS out_cent
    FROM {T_VERTICES} v
    JOIN {t_pagerank}    pr ON pr.id = v.id
    JOIN {t_betweenness} bc ON bc.id = v.id
    JOIN {t_degree}      dc ON dc.id = v.id
    ORDER BY pr.pagerank DESC
    LIMIT 20
""").display()
