# Databricks notebook source
# MAGIC %md
# MAGIC # Cargar el grafo como NetworkX DiGraph
# MAGIC
# MAGIC Patrón reutilizable para los notebooks de este módulo:
# MAGIC
# MAGIC 1. Spark lee las tablas Delta (distribuido)
# MAGIC 2. `.toPandas()` colapsa al driver
# MAGIC 3. `nx.from_pandas_edgelist` construye un `DiGraph` con atributos de arista
# MAGIC 4. Agregamos atributos de nodo con `G.nodes[id].update(...)`
# MAGIC
# MAGIC Después mostramos estadísticas básicas del grafo: tamaño, densidad,
# MAGIC conectividad. Los algoritmos pesados (PageRank, Louvain) viven en los
# MAGIC notebooks siguientes.

# COMMAND ----------

# MAGIC %run ../00-setup/config

# COMMAND ----------

import networkx as nx
import pandas as pd

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Sanity check: ¿cabe el grafo en memoria del driver?

# COMMAND ----------

n_vertices = spark.table(T_VERTICES).count()
n_edges    = spark.table(T_EDGES).count()

# Estimación grosera: ~80 bytes por arista (long, long, long, 3 doubles)
edges_mb = (n_edges * 80) / (1024 * 1024)

print(f"Vértices:             {n_vertices:>8,}")
print(f"Aristas:              {n_edges:>8,}")
print(f"Memoria estimada:     {edges_mb:>8.2f} MB en el driver")
print(f"Driver default DBR:   ~8,000 MB")
print(f"Uso:                  {100 * edges_mb / 8000:>8.3f}%")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Traer a pandas

# COMMAND ----------

vertices_pdf = spark.table(T_VERTICES).toPandas()
edges_pdf    = spark.table(T_EDGES).toPandas()

print(f"vertices_pdf: {vertices_pdf.shape}, {vertices_pdf.memory_usage(deep=True).sum() / 1024:.1f} KB")
print(f"edges_pdf:    {edges_pdf.shape}, {edges_pdf.memory_usage(deep=True).sum() / 1024:.1f} KB")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Construir el DiGraph

# COMMAND ----------

G = nx.from_pandas_edgelist(
    edges_pdf,
    source="src",
    target="dst",
    edge_attr=["num_trips", "avg_fare", "avg_duration_min", "total_revenue"],
    create_using=nx.DiGraph,
)

# Agregar atributos de nodo (zone, borough, service)
for row in vertices_pdf.itertuples(index=False):
    if row.id in G.nodes:
        G.nodes[row.id].update({
            "zone": row.zone,
            "borough": row.borough,
            "service": row.service,
        })

print(f"G: {G.number_of_nodes()} nodos, {G.number_of_edges()} aristas")

# COMMAND ----------

# MAGIC %md
# MAGIC Notar: es posible que haya vértices en `graph.vertices` que no aparezcan
# MAGIC en `graph.edges` (zonas sin viajes). `from_pandas_edgelist` solo crea
# MAGIC nodos que aparecen en aristas. Agreguemos los faltantes.

# COMMAND ----------

missing = set(vertices_pdf["id"]) - set(G.nodes)
print(f"Vértices sin aristas (se agregarán como nodos aislados): {len(missing)}")

for row in vertices_pdf[vertices_pdf["id"].isin(missing)].itertuples(index=False):
    G.add_node(row.id, zone=row.zone, borough=row.borough, service=row.service)

print(f"G final: {G.number_of_nodes()} nodos, {G.number_of_edges()} aristas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Estadísticas básicas

# COMMAND ----------

stats = {
    "nodes": G.number_of_nodes(),
    "edges": G.number_of_edges(),
    "density": nx.density(G),
    "is_strongly_connected": nx.is_strongly_connected(G),
    "is_weakly_connected": nx.is_weakly_connected(G),
    "num_weakly_connected_components": nx.number_weakly_connected_components(G),
    "num_strongly_connected_components": nx.number_strongly_connected_components(G),
}

for k, v in stats.items():
    print(f"{k:40s} {v}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Distribución de grado
# MAGIC
# MAGIC In-degree y out-degree de cada nodo. Sin aplicar aún algoritmos pesados
# MAGIC — solo `dict(G.in_degree())` que es O(V).

# COMMAND ----------

degrees_df = pd.DataFrame({
    "id": list(G.nodes),
    "in_degree":  [G.in_degree(n)  for n in G.nodes],
    "out_degree": [G.out_degree(n) for n in G.nodes],
})
degrees_df = degrees_df.merge(vertices_pdf, on="id", how="left")

top10_by_in = degrees_df.nlargest(10, "in_degree")
top10_by_out = degrees_df.nlargest(10, "out_degree")

print("Top 10 por in-degree (zonas más 'sumidero'):")
print(top10_by_in[["zone", "borough", "in_degree", "out_degree"]].to_string(index=False))

print()
print("Top 10 por out-degree (zonas más 'surtidor'):")
print(top10_by_out[["zone", "borough", "in_degree", "out_degree"]].to_string(index=False))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Preview del grafo: draw_networkx sobre un subconjunto
# MAGIC
# MAGIC Dibujar 265 nodos con matplotlib es ilegible. Limitamos al subgrafo de
# MAGIC los 20 nodos con mayor `in_degree + out_degree`, para ver la forma.

# COMMAND ----------

import matplotlib.pyplot as plt

top20_ids = degrees_df.assign(
    total=lambda d: d["in_degree"] + d["out_degree"]
).nlargest(20, "total")["id"].tolist()

subG = G.subgraph(top20_ids)
labels = {n: G.nodes[n]["zone"] for n in subG.nodes}

fig, ax = plt.subplots(figsize=(12, 10))
nx.draw_networkx(
    subG, ax=ax,
    labels=labels,
    node_size=800, node_color="#ffd27a",
    font_size=8, arrows=True, arrowsize=12,
    edge_color="#888", width=0.7,
)
ax.set_title(f"Top 20 zonas por grado total (subgrafo inducido)")
ax.axis("off")
plt.tight_layout()
plt.show()
