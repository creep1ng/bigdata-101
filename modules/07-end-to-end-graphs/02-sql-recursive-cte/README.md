# 02 - Análisis con Recursive CTE

Análisis de grafos usando `WITH RECURSIVE` de SQL ANSI, nativo en DBR 17+.

## Por qué SQL recursivo

Para **traversals, caminos y reachability** las recursive CTEs son:
- Declarativas (más cortas y legibles que código Python)
- Nativas en Databricks (zero setup)
- Optimizables por el planner (Catalyst las ejecuta con Spark)

## Notebooks

| # | Notebook | Responde |
|---|---|---|
| 01 | `01_reachability.py` | ¿Qué zonas puedo alcanzar desde X con ≤N saltos? |
| 02 | `02_shortest_path_hops.py` | Ruta con menor número de saltos entre A y B |
| 03 | `03_cheapest_path.py` | Ruta con menor tarifa acumulada entre A y B |
| 04 | `04_triangular_trips.py` | Triángulos A→B→C→A emergentes en la red |

## Guardrails importantes

Databricks limita recursive CTE a **100 iteraciones** y **1M filas** por default. Con 265 nodos y ~50K aristas, exploraciones sin filtrar explotan este límite rápido.

Cada notebook usa dos widgets para acotar el espacio de búsqueda:

- `min_trips`: solo consideramos aristas con al menos N viajes (filtra ruido)
- `max_hops`: profundidad máxima de la recursión

Tune estos widgets si las queries tardan más de 30 segundos.

## Pruning: no expandir desde el destino

En los notebooks de shortest/cheapest path, una vez que un camino llega al destino **dejamos de expandirlo**. Sin este pruning, con `max_hops=4` en un grafo denso (filtrado por `min_trips>=50`, ~5K aristas) la recursión excede fácil el guardrail de 1M filas. Con el pruning, `max_hops=3` corre en segundos.

## Decisión de diseño

Para evitar ciclos durante la recursión (fundamental en grafos con ciclos como el nuestro: A→B y B→A son comunes), trackeamos el `path` como `ARRAY<LONG>` y verificamos `NOT array_contains(path, next_id)` antes de expandir.
## Pruning: canonización en triangle counting

En `04_triangular_trips.py` el bonus de recursive CTE enumera triángulos. Cada triángulo tiene 3 rotaciones (A→B→C→A, B→C→A→B, C→A→B→C). Para contar cada uno una sola vez sin explotar el guardrail, la recursión **solo extiende a nodos con ID mayor que start_id** y **solo cierra en depth=2**. Así sobrevive únicamente la rotación canónica que empieza por el nodo de menor ID.

Este pruning funciona porque la longitud del ciclo está fija en 3. Para ciclos de longitud variable, el enfoque no generaliza — en ese caso conviene NetworkX (`nx.simple_cycles`).
