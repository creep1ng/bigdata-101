# 00 - Setup

Bootstrap del módulo de grafos: crea los 3 esquemas nuevos y el volume dentro del catálogo `<iniciales>_nytaxi` del módulo 06.

## Prerrequisitos

- **Cluster con DBR 17.0 ML o superior** (para `WITH RECURSIVE` y NetworkX preinstalado). Validamos con DBR 18.2 ML.
- **Módulo 06 corrido hasta `02-silver/`** con las tablas:
  - `<iniciales>_nytaxi.silver.trips_clean`
  - `<iniciales>_nytaxi.bronze.taxi_zones`

## Por qué reusamos el catálogo del 06

Mismo patrón que el módulo 04: nuevos esquemas dentro de un catálogo que ya existe, en vez de crear uno nuevo. Evita fricciones con managed storage de Unity Catalog, permisos del metastore y External Locations.

## Orden de ejecución

1. `config.py` — se importa con `%run`, no se ejecuta directo. **Editar `USER_INITIALS`** con las mismas iniciales del módulo 06.
2. `01_create_catalog.py` — usa el catálogo existente y crea los 3 esquemas nuevos + el volume.
3. `02_check_prerequisites.py` — valida runtime, existencia de fuente del 06 y NetworkX.

## Resultado

Dentro de `<iniciales>_nytaxi`:

```
<iniciales>_nytaxi/
├── bronze/        ← módulo 06
├── silver/        ← módulo 06
├── gold/          ← módulo 06
├── ml/            ← módulo 06
├── graph/         ← nuevo: vertices, edges
├── graph_gold/    ← nuevo: pagerank, betweenness, communities, etc.
└── graph_viz/
    └── volume:assets/   ← shapefile de taxi zones + HTML de mapas
```
