# 01 - Bronze

Ingesta incremental con **Auto Loader** desde el landing zone a tablas Delta en la capa Bronze. El principio: los datos entran tal cual, en Delta, sin transformaciones.

## Conceptos que se enseñan aquí

- **Auto Loader** (`cloudFiles`): ingesta incremental de archivos con tracking automático de qué archivos ya se procesaron
- **Schema inference + schema evolution**: el esquema se infiere del primer archivo y evoluciona automáticamente
- **`availableNow` trigger**: "procesa todo lo pendiente y para" — ideal para jobs batch que imitan streaming
- **Metadatos de ingesta**: agregar columnas técnicas (`ingested_at`, `source_file`) para trazabilidad
- **Tablas estáticas**: cuando la fuente es un archivo pequeño (zonas) no necesitas Auto Loader

## Orden de ejecución

1. `01_bronze_autoloader.py` — Auto Loader sobre los Parquets mensuales
2. `02_bronze_zones.py` — Tabla estática de zonas
3. `03_inspect_bronze.py` — Verificar el resultado y el transaction log

## Resultado esperado

- `<iniciales>_nytaxi.bronze.yellow_trips` — Delta table append-only con ~20M filas (6 meses)
- `<iniciales>_nytaxi.bronze.taxi_zones` — Delta table con 265 zonas
