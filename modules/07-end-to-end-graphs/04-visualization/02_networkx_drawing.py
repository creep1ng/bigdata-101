# Databricks notebook source
# MAGIC %md
# MAGIC # Dibujar el grafo con NetworkX + matplotlib
# MAGIC
# MAGIC Dibujar los 265 nodos con todas las aristas es ilegible. Mostramos dos
# MAGIC visualizaciones complementarias:
# MAGIC
# MAGIC 1. **Subgrafo top-N por PageRank**: los nodos más "importantes" y cómo
# MAGIC    se conectan entre sí
# MAGIC 2. **Subgrafo coloreado por comunidad**: mismo top-N, ahora con colores
# MAGIC    según la comunidad detectada por Louvain

# COMMAND ----------

# MAGIC %run ../00-setup/config

# COMMAND ----------

import networkx as nx
import matplotlib.pyplot as plt
import matplotlib.cm as cm

# COMMAND ----------

dbutils.widgets.text("top_n", "25", "Cantidad de nodos a dibujar")
dbutils.widgets.text("min_trips", "100", "Viajes mínimos por arista visible")

top_n     = int(dbutils.widgets.get("top_n"))
min_trips = int(dbutils.widgets.get("min_trips"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Cargar grafo + PageRank + comunidades

# COMMAND ----------

vertices_pdf = spark.table(T_VERTICES).toPandas()
edges_pdf    = spark.table(T_EDGES).filter(f"num_trips >= {min_trips}").toPandas()

pagerank_pdf    = spark.table(f"{CATALOG}.{SCHEMA_GOLD}.pagerank").select("id", "pagerank").toPandas()
communities_pdf = spark.table(f"{CATALOG}.{SCHEMA_GOLD}.communities").select("id", "community_id").toPandas()

G = nx.from_pandas_edgelist(
    edges_pdf, source="src", target="dst",
    edge_attr=["num_trips"],
    create_using=nx.DiGraph,
)

print(f"G filtrado (>= {min_trips} viajes/arista): {G.number_of_nodes()} nodos, {G.number_of_edges()} aristas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Top N por PageRank

# COMMAND ----------

top_ids = pagerank_pdf.nlargest(top_n, "pagerank")["id"].tolist()
top_ids_in_g = [n for n in top_ids if n in G.nodes]

subG = G.subgraph(top_ids_in_g)
print(f"Subgrafo top-{top_n}: {subG.number_of_nodes()} nodos, {subG.number_of_edges()} aristas")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Visualización 1: tamaño de nodo = PageRank

# COMMAND ----------

labels = {
    row.id: row.zone
    for row in vertices_pdf[vertices_pdf["id"].isin(top_ids_in_g)].itertuples(index=False)
}

pr_dict = pagerank_pdf.set_index("id")["pagerank"].to_dict()
node_sizes = [pr_dict[n] * 80000 for n in subG.nodes]

edge_widths = [d["num_trips"] / 5000 for _, _, d in subG.edges(data=True)]

fig, ax = plt.subplots(figsize=(14, 11))
pos = nx.spring_layout(subG, seed=42, k=1.2)

nx.draw_networkx_edges(subG, pos, ax=ax,
                       width=edge_widths, edge_color="#aaa",
                       arrows=True, arrowsize=12, alpha=0.6)
nx.draw_networkx_nodes(subG, pos, ax=ax,
                       node_size=node_sizes, node_color="#ffb347",
                       edgecolors="#333", linewidths=0.8)
nx.draw_networkx_labels(subG, pos, labels=labels, ax=ax, font_size=8)

ax.set_title(f"Top {top_n} zonas por PageRank — tamaño ∝ PageRank, grosor ∝ num_trips")
ax.axis("off")
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Visualización 2: color = comunidad detectada

# COMMAND ----------

comm_dict = communities_pdf.set_index("id")["community_id"].to_dict()
n_communities = communities_pdf["community_id"].nunique()
palette = cm.get_cmap("tab20", n_communities)

node_colors = [palette(comm_dict.get(n, 0)) for n in subG.nodes]

fig, ax = plt.subplots(figsize=(14, 11))
nx.draw_networkx_edges(subG, pos, ax=ax,
                       width=edge_widths, edge_color="#ccc",
                       arrows=True, arrowsize=12, alpha=0.5)
nx.draw_networkx_nodes(subG, pos, ax=ax,
                       node_size=node_sizes, node_color=node_colors,
                       edgecolors="#333", linewidths=0.8)
nx.draw_networkx_labels(subG, pos, labels=labels, ax=ax, font_size=8)

ax.set_title(f"Top {top_n} zonas — color = comunidad Louvain")
ax.axis("off")
plt.tight_layout()
plt.show()
