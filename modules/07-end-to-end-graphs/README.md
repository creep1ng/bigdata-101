# Pipeline End-to-End: Análisis de Grafos con NYC Taxi

Caso de estudio integrador que transforma el dataset de NYC Yellow Taxi en una **red de movilidad urbana** y aplica análisis de grafos sobre Databricks usando las herramientas nativas de la plataforma.

> Continuación del [módulo 06](../06-end-to-end-nyc-taxi/). Reutiliza la capa Silver de su pipeline Medallion.

## Objetivos de aprendizaje

Al terminar el módulo el estudiante es capaz de:

- Modelar un dataset tabular como grafo dirigido ponderado
- Construir y persistir vértices y aristas en Delta Lake bajo Unity Catalog
- Escribir **Recursive CTEs** en SQL para traversals, reachability y shortest path
- Usar **NetworkX** para algoritmos clásicos (centralidad, comunidades) en Databricks Runtime ML
- Decidir **cuándo SQL, cuándo NetworkX, cuándo GraphFrames**
- Visualizar resultados de grafos con `display()`, Folium (mapa coroplético) y NetworkX

## El problema

~3-4M de viajes de taxi al mes entre 265 zonas de NYC. Vista como tabla respondemos _"¿cuánto se facturó en Manhattan?"_. Vista como **grafo** respondemos:

- ¿Qué zonas son los **hubs** de la red? (PageRank / degree centrality)
- ¿Cuál es la **ruta más barata** entre dos zonas no directamente conectadas? (Weighted shortest path)
- ¿Qué zonas son alcanzables desde Times Square con ≤3 saltos? (Reachability)
- ¿Qué zonas forman **barrios funcionales emergentes**? (Community detection)

## Tres formas de analizar grafos en Databricks

Databricks ofrece **tres opciones** según el tamaño del grafo y el tipo de análisis. Este módulo explora las dos primeras porque nuestro grafo (~265 nodos, ~60K aristas) no necesita procesamiento distribuido.

| Herramienta | Cuándo usarla | Qué resuelve bien |
|---|---|---|
| **Recursive CTE (SQL)** | Traversals, caminos, reachability | BFS, shortest path, transitive closure, detección de ciclos |
| **NetworkX** | Grafos que caben en un nodo (~<1M aristas) | PageRank, betweenness, comunidades, todo el catálogo clásico |
| **GraphFrames** | Grafos de 10M+ aristas, procesamiento distribuido | Los mismos algoritmos en Spark distribuido |

> **GraphFrames** queda fuera del módulo por diseño: nuestro grafo es pequeño y single-node es más rápido, con APIs más ricas. Si algún día trabajas con grafos de billones de aristas (fraude financiero, social graphs), esa es la opción — se instala como librería Maven y la documentación oficial está [aquí](https://docs.databricks.com/aws/en/integrations/graphframes/).

## El grafo

```
Vértices (V)   — 265 taxi zones
├── id          LONG        (LocationID de la TLC)
├── zone        STRING      ("Times Sq/Theatre District")
├── borough     STRING      (Manhattan, Brooklyn, Queens, Bronx, Staten Island, EWR)
└── service     STRING      (Yellow Zone, Boro Zone, Airports)

Aristas (E)   — agregadas desde silver.trips_clean
├── src             LONG    (PULocationID)
├── dst             LONG    (DOLocationID)
├── num_trips       LONG
├── avg_fare        DOUBLE
├── avg_duration_min DOUBLE
└── total_revenue   DOUBLE
```

## Arquitectura

```
Módulo 06 (prerrequisito)                   Módulo 07
┌──────────────────────────────────────────────────────────────────┐
│ <iniciales>_nytaxi                                               │
│                                                                  │
│ silver.trips_clean ──┐                                           │
│ bronze.taxi_zones  ──┼── Spark agg ───▶  graph.vertices          │
│                       │                  graph.edges             │
│                       │                                          │
│                       │   ├── SQL recursive CTEs                 │
│                       │   │   → graph_gold.reachable_from        │
│                       │   │   → graph_gold.cheapest_paths        │
│                       │   │                                      │
│                       │   └── NetworkX (driver)                  │
│                       │       → graph_gold.pagerank              │
│                       │       → graph_gold.betweenness           │
│                       │       → graph_gold.communities           │
│                       │                                          │
│                       │   graph_viz.volume:assets                │
│                       │   (shapefile, mapas HTML)                │
└──────────────────────────────────────────────────────────────────┘
                          │
                          ▼
                    Visualización
                    ├── display() nativo
                    ├── NetworkX + matplotlib
                    └── Folium choropleth
```

Todo vive en el catálogo `<iniciales>_nytaxi` del módulo 06. Los esquemas nuevos usan el prefijo `graph_` para no chocar con los del 06 (`bronze`, `silver`, `gold`, `ml`).

## Estructura del módulo

```
00-setup/              Catálogo, esquemas, volume, validación de prerrequisitos
01-build-graph/        Agregación Spark: silver.trips → vertices + edges Delta
02-sql-recursive-cte/  Paths, reachability y ciclos con WITH RECURSIVE
03-networkx/           Centralidad, comunidades y el catálogo clásico de NetworkX
04-visualization/      display(), NetworkX drawing y Folium choropleth
```

## Secuencia recomendada (4 sesiones)

| Sesión | Tema | Carpetas |
|---|---|---|
| 1 | Setup + construcción del grafo | `00-setup/`, `01-build-graph/` |
| 2 | Recursive CTE: paths y reachability | `02-sql-recursive-cte/` |
| 3 | NetworkX: centralidad y comunidades | `03-networkx/` |
| 4 | Visualización | `04-visualization/` |

## Prerrequisitos de plataforma

- Haber completado hasta `02-silver/` del módulo 06
- Workspace de Databricks con Unity Catalog
- Cluster con **DBR 17.0 ML o superior, access mode `Single User`**. Validamos DBR 18.2 ML.

No hay que instalar ninguna librería: NetworkX viene preinstalado en DBR ML y recursive CTE es SQL nativo.

## Convenciones

- Cada estudiante usa el **catálogo `<iniciales>_nytaxi`** del módulo 06 (el mismo)
- Las iniciales van en `00-setup/config.py` (mismas del módulo 06)
- Nombres de tablas nuevas: `<iniciales>_nytaxi.graph.*`, `<iniciales>_nytaxi.graph_gold.*`, `<iniciales>_nytaxi.graph_viz.*`
- Widgets de notebook para parámetros (top-N, umbral de aristas, rango de fechas)
