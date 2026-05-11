# 04 - Visualización

Tres formas complementarias de visualizar los resultados del análisis:

| # | Notebook | Tipo | Qué muestra |
|---|---|---|---|
| 01 | `01_display_native.py` | Built-in Databricks | Bar, scatter, heatmap, pivot — sin instalar nada |
| 02 | `02_networkx_drawing.py` | NetworkX + matplotlib | Subgrafos top-N, coloreados por comunidad |
| 03 | `03_folium_choropleth.py` | Folium mapa interactivo | Choropleth de NYC: PageRank y comunidades sobre geografía real |

## Cuándo usar cada una

- **Display nativo** → exploración rápida durante desarrollo, o cuando vas a compartir un notebook y quieres que el lector interactúe con los gráficos sin instalar nada.
- **NetworkX drawing** → para mostrar estructura del grafo (nodos, aristas, layout). Inútil con muchos nodos, así que siempre sobre un subgrafo.
- **Folium** → cuando los nodos tienen geografía (como las taxi zones de NYC). El mapa hace el análisis interpretable de un vistazo.

## Detalle importante: Folium y geopandas

`folium` **no viene preinstalado** en DBR ML. El notebook 03 lo instala al inicio con `%pip install folium --quiet`. `geopandas` sí viene en DBR ML y lo usamos para leer el shapefile oficial de la TLC.

El shapefile se descarga **una sola vez** al volume `viz.assets` desde el CDN oficial (`d37ci6vzurychx.cloudfront.net/misc/taxi_zones.zip`, misma fuente que los datos de viajes del módulo 06). Viene en proyección NAD 1983 / NY Long Island (pies) y lo reproyectamos a WGS 84 (EPSG:4326) con `.to_crs(4326)`.

## HTML persistido

Los mapas se guardan como HTML en `viz.assets/map_pagerank.html` y `viz.assets/map_communities.html`. Se pueden abrir directamente desde el Catalog Explorer (click derecho → Download) o compartir como archivo.
