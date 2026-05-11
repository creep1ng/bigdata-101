# 01 - Construcción del grafo

Un solo notebook que transforma los viajes limpios del módulo 06 en un grafo dirigido ponderado persistido como dos tablas Delta.

## Qué produce

| Tabla | Filas aprox. | Contenido |
|---|---|---|
| `<iniciales>_nytaxi.graph.vertices` | 265 | Una por taxi zone: `id`, `zone`, `borough`, `service` |
| `<iniciales>_nytaxi.graph.edges` | ~40-60K | Una por par `(src, dst)` observado: `num_trips`, `avg_fare`, `avg_duration_min`, `total_revenue` |

## Decisiones de diseño (ver notebook para detalles)

- Self-loops (`src == dst`) excluidos
- Sin filtro por número mínimo de viajes (cada notebook de análisis filtra si lo necesita)
- Sin filtro temporal: usa todo `silver.trips_clean`

## Después de correr esto

Todos los notebooks de `02-sql-recursive-cte/` y `03-networkx/` leen exclusivamente de `graph.vertices` y `graph.edges`. Nunca vuelven a tocar `silver.trips_clean`.
