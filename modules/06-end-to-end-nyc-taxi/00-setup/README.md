# 00 - Setup

Bootstrap del pipeline: prepara el catálogo de Unity Catalog, los esquemas, el volume para archivos crudos, y descarga los primeros archivos de NYC Taxi al landing zone.

## Prerrequisitos

Antes de correr los notebooks, el admin del workspace debe tener configurada una **External Location** en Unity Catalog apuntando al container de ADLS Gen2. Esta External Location es **compartida** por todos los estudiantes — cada uno tendrá su propio catálogo apuntando a una subcarpeta distinta del mismo container. Si no la tienes, usa el script `_admin_external_location.sql` como referencia para que te la creen.

## Orden de ejecución

1. `config.py` — No se ejecuta, se importa con `%run`. Centraliza los paths y nombres. **Editar `USER_INITIALS` antes de todo.**
2. `01_create_catalog.py` — Crea catálogo, esquemas y volume.
3. `02_download_raw_files.py` — Descarga archivos de la TLC al volume.
4. `03_explore_raw_data.py` — Inspecciona el esquema y contenido para entender el dataset.

## Qué vas a tener después de correr esto

- Catálogo `<iniciales>_nytaxi` con esquemas `bronze`, `silver`, `gold`, `ml`
- Volume `<iniciales>_nytaxi.bronze.landing` con archivos Parquet mensuales
- Tabla `<iniciales>_nytaxi.bronze.taxi_zones` con el lookup de zonas

## Variables que debes ajustar

En `config.py`:

- `USER_INITIALS`: tus iniciales (por ejemplo `casm`). Determina el nombre del catálogo.
- `EXTERNAL_LOCATION`: ruta `abfss://` de tu External Location
- `MONTHS_TO_LOAD`: meses a descargar en el primer arranque
