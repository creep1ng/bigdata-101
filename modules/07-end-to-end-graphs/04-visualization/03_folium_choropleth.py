# Databricks notebook source
# MAGIC %md
# MAGIC # Mapa coroplético con Folium
# MAGIC
# MAGIC Pintamos las 265 taxi zones sobre el mapa de NYC, coloreadas por el
# MAGIC valor de PageRank. El shapefile con los polígonos se descarga una vez
# MAGIC al volume `viz.assets` y se reutiliza en corridas posteriores.
# MAGIC
# MAGIC También generamos un segundo mapa coloreado por **comunidad detectada**,
# MAGIC para ver si los barrios funcionales coinciden con la geografía.
# MAGIC
# MAGIC ## Fuente de los polígonos
# MAGIC
# MAGIC Shapefile oficial de la TLC, publicado en el mismo CDN que los datos de
# MAGIC viajes:
# MAGIC
# MAGIC `https://d37ci6vzurychx.cloudfront.net/misc/taxi_zones.zip`
# MAGIC
# MAGIC Viene en proyección **NAD 1983 / New York Long Island (ft)** — hay que
# MAGIC reproyectarlo a **WGS 84 (EPSG:4326)** para que folium lo muestre
# MAGIC correctamente. `geopandas` lo hace con `.to_crs(4326)` en una línea.
# MAGIC
# MAGIC ## Requisitos
# MAGIC
# MAGIC `folium` no viene preinstalado en DBR ML, `geopandas` sí.

# COMMAND ----------

# MAGIC %pip install folium --quiet

# COMMAND ----------

# MAGIC %run ../00-setup/config

# COMMAND ----------

import json
import os
import urllib.request
import zipfile

import folium
import geopandas as gpd
from folium.features import GeoJson, GeoJsonTooltip

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Descargar shapefile al volume (una sola vez)

# COMMAND ----------

SHAPEFILE_URL  = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zones.zip"
ZIP_PATH       = f"{VIZ_VOLUME_PATH}/taxi_zones.zip"
SHAPEFILE_DIR  = f"{VIZ_VOLUME_PATH}/taxi_zones"
SHAPEFILE_PATH = f"{SHAPEFILE_DIR}/taxi_zones.shp"

if os.path.exists(SHAPEFILE_PATH):
    print(f"✓ Ya existe: {SHAPEFILE_PATH}")
else:
    print(f"Descargando {SHAPEFILE_URL} ...")
    urllib.request.urlretrieve(SHAPEFILE_URL, ZIP_PATH)

    with zipfile.ZipFile(ZIP_PATH) as z:
        z.extractall(VIZ_VOLUME_PATH)
    print(f"✓ Shapefile listo: {SHAPEFILE_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Leer con geopandas y reproyectar a WGS84

# COMMAND ----------

gdf = gpd.read_file(SHAPEFILE_PATH)

print(f"CRS original: {gdf.crs}")
print(f"Features: {len(gdf)}")
print(f"Columnas: {list(gdf.columns)}")

# Reproyectar a EPSG:4326 (lat/lon) — necesario para folium
gdf = gdf.to_crs(epsg=4326)
print(f"CRS reproyectado: {gdf.crs}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Enriquecer con PageRank y comunidad

# COMMAND ----------

pr_pdf   = spark.table(f"{CATALOG}.{SCHEMA_GOLD}.pagerank").select("id", "pagerank").toPandas()
comm_pdf = spark.table(f"{CATALOG}.{SCHEMA_GOLD}.communities").select("id", "community_id").toPandas()

# LocationID en el shapefile es numérico; igual lo casteamos defensivamente.
gdf["LocationID"] = gdf["LocationID"].astype(int)
pr_pdf["id"]      = pr_pdf["id"].astype(int)
comm_pdf["id"]    = comm_pdf["id"].astype(int)

gdf = (
    gdf.merge(pr_pdf,   left_on="LocationID", right_on="id", how="left")
       .drop(columns=["id"])
       .merge(comm_pdf, left_on="LocationID", right_on="id", how="left")
       .drop(columns=["id"])
)

gdf["pagerank"]     = gdf["pagerank"].fillna(0.0)
gdf["community_id"] = gdf["community_id"].fillna(-1).astype(int)

print(f"✓ Enriquecido. Preview:")
print(gdf[["LocationID", "zone", "borough", "pagerank", "community_id"]].head().to_string(index=False))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Mapa 1: Choropleth por PageRank

# COMMAND ----------

NYC_CENTER = [40.7580, -73.9855]

m1 = folium.Map(location=NYC_CENTER, zoom_start=11, tiles="CartoDB positron")

folium.Choropleth(
    geo_data=gdf.__geo_interface__,
    data=gdf[["LocationID", "pagerank"]],
    columns=["LocationID", "pagerank"],
    key_on="feature.properties.LocationID",
    fill_color="YlOrRd",
    fill_opacity=0.75,
    line_opacity=0.3,
    legend_name="PageRank",
    nan_fill_color="#eee",
).add_to(m1)

GeoJson(
    gdf.__geo_interface__,
    style_function=lambda _: {"fillColor": "transparent", "color": "#666", "weight": 0.3},
    tooltip=GeoJsonTooltip(
        fields=["zone", "borough", "pagerank"],
        aliases=["Zona", "Borough", "PageRank"],
        localize=True,
    ),
).add_to(m1)

html_path_pr = f"{VIZ_VOLUME_PATH}/map_pagerank.html"
m1.save(html_path_pr)
print(f"✓ Mapa PageRank: {html_path_pr}")

displayHTML(m1._repr_html_())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Mapa 2: Choropleth por comunidad detectada
# MAGIC
# MAGIC Si las comunidades se ven como manchas contiguas en el mapa, la movilidad
# MAGIC tiene lógica geográfica. Si cruzan boroughs, Louvain encontró algo que la
# MAGIC geografía no revela.

# COMMAND ----------

import matplotlib.cm as cm
import matplotlib.colors as mcolors

n_communities = gdf["community_id"].max() + 1
cmap = cm.get_cmap("tab20", n_communities)
community_colors = {i: mcolors.to_hex(cmap(i)) for i in range(n_communities)}
community_colors[-1] = "#cccccc"   # zonas sin comunidad

def style_by_community(feat):
    cid = feat["properties"]["community_id"]
    return {
        "fillColor": community_colors.get(cid, "#ccc"),
        "color": "#333",
        "weight": 0.3,
        "fillOpacity": 0.7,
    }

m2 = folium.Map(location=NYC_CENTER, zoom_start=11, tiles="CartoDB positron")

GeoJson(
    gdf.__geo_interface__,
    style_function=style_by_community,
    tooltip=GeoJsonTooltip(
        fields=["zone", "borough", "community_id"],
        aliases=["Zona", "Borough", "Comunidad"],
        localize=True,
    ),
).add_to(m2)

html_path_comm = f"{VIZ_VOLUME_PATH}/map_communities.html"
m2.save(html_path_comm)
print(f"✓ Mapa comunidades: {html_path_comm}")

displayHTML(m2._repr_html_())
