# 03 - Análisis con NetworkX

Algoritmos clásicos de grafos (centralidad, comunidades) usando NetworkX preinstalado en DBR ML.

## Cuándo NetworkX (vs Spark / Recursive CTE)

| Algoritmo | NetworkX | Recursive CTE | Por qué |
|---|---|---|---|
| BFS, paths, reachability | OK | **Mejor** | SQL más declarativo |
| Triangle counting (longitud fija) | OK | **Mejor** | 3 joins, sin recursión |
| PageRank | **Único viable** | No | Requiere iteración hasta convergencia |
| Betweenness centrality | **Único viable** | No | O(V·E), no encaja en recursión bounded |
| Community detection (Louvain) | **Único viable** | No | Algoritmo iterativo de optimización |

## Patrón arquitectónico

```
Spark DataFrame ──toPandas()──▶ nx.DiGraph ──algoritmo──▶ dict {node: value}
       ▲                                                      │
       │                                                      ▼
 Unity Catalog ◀──saveAsTable──  spark.createDataFrame ◀──pd.DataFrame
```

1. **Spark lee** `graph.vertices` y `graph.edges` (distribuido)
2. **`.toPandas()`** colapsa al driver. Con 50K aristas ocupa ~4 MB.
3. **NetworkX** corre el algoritmo en el driver
4. Los resultados vuelven a Spark con `spark.createDataFrame` y se persisten en `gold.*`

## Notebooks

| # | Notebook | Produce |
|---|---|---|
| 01 | `01_load_as_networkx.py` | Patrón reutilizable + estadísticas básicas del grafo |
| 02 | `02_centrality.py` | `gold.degree_centrality`, `gold.pagerank`, `gold.betweenness` |
| 03 | `03_communities_louvain.py` | `gold.communities` (zona → comunidad detectada) |

## Tamaño del grafo y RAM

- 265 nodos × pocas columnas: 10-20 KB
- ~50K aristas × 6 columnas (long/double): ~4 MB
- Total en memoria del driver: < 5 MB

Driver default de Databricks: 8 GB. Estamos usando 0.06% de la RAM. Si algún día el grafo crece por encima de 1M aristas, considera cambiar a **GraphFrames** para los algoritmos distribuidos.
