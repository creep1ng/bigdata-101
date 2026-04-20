# Pipeline End-to-End: NYC Taxi en Azure Databricks

Caso de estudio integrador que recorre todo el ciclo de vida de un pipeline de datos moderno sobre la plataforma Databricks en Azure, usando el dataset público de NYC Taxi & Limousine Commission (TLC).

## Objetivos de aprendizaje

Al terminar este módulo el estudiante es capaz de:

- Diseñar e implementar un pipeline Medallion (Bronze/Silver/Gold) sobre Delta Lake
- Ingestar archivos de forma incremental con Auto Loader desde ADLS Gen2
- Aplicar limpieza, validación y enriquecimiento con DataFrame API
- Gobernar el lakehouse con Unity Catalog (catálogos, esquemas, volumes, permisos)
- Entrenar, registrar y servir un modelo de ML con MLflow
- Construir dashboards con Databricks SQL
- Orquestar todo con Databricks Workflows y Asset Bundles
- Agregar una capa de streaming sobre la misma arquitectura

## El dataset

**NYC TLC Yellow Taxi Trip Records** — ~1.5B filas acumuladas desde 2009, distribuidas como archivos Parquet mensuales.

- Origen oficial: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
- CDN de descarga directa: `https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_YYYY-MM.parquet`
- Tabla de zonas (lookup): `https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv`
- Volumen mensual: 40-80 MB comprimido, 3-4M filas

Para la cátedra recomendamos arrancar con 6 meses (2024-01 a 2024-06) — suficiente para ver patrones estacionales sin explotar el cluster.

## Arquitectura

```
┌──────────────────┐   ┌─────────────┐   ┌──────────────┐   ┌──────────────┐
│ CloudFront TLC   │──▶│ ADLS Gen2   │──▶│ Bronze       │──▶│ Silver       │
│ (Parquet mensual)│   │ landing/    │   │ Auto Loader  │   │ Clean + Join │
└──────────────────┘   └─────────────┘   └──────────────┘   └──────────────┘
                                                                    │
                                           ┌────────────────────────┼───────────────────────┐
                                           ▼                        ▼                       ▼
                                    ┌──────────────┐         ┌──────────────┐        ┌──────────────┐
                                    │ Gold: KPIs   │         │ Gold: ML     │        │ Streaming    │
                                    │ Revenue      │         │ Features     │        │ (opcional)   │
                                    │ Heatmaps     │         │ Duration pred│        │              │
                                    └──────┬───────┘         └──────┬───────┘        └──────────────┘
                                           ▼                        ▼
                                    ┌──────────────┐         ┌──────────────┐
                                    │ Dashboard    │         │ Model Serving│
                                    │ Databricks   │         │ MLflow       │
                                    │ SQL          │         │ endpoint     │
                                    └──────────────┘         └──────────────┘
```

Todo gobernado por **Unity Catalog**: catálogo `<iniciales>_nytaxi` (uno por estudiante), esquemas `bronze`/`silver`/`gold`/`ml`, volume `landing` para los archivos crudos.

## Prerrequisitos

- Workspace de Azure Databricks con Unity Catalog habilitado
- Una storage account ADLS Gen2 con un container (ej: `nyctaxi-lake`)
- Una **External Location** en Unity Catalog apuntando al container
- Permisos `CREATE CATALOG` o un catálogo ya provisto por el admin
- Cluster o SQL Warehouse con Runtime 14.3 LTS o superior (15.4 LTS recomendado)

> **Para el profesor/admin**: todos los pasos para configurar esto están en [`ADMIN_SETUP.md`](ADMIN_SETUP.md). Una vez hechos, los estudiantes solo deben cambiar sus iniciales en `00-setup/config.py`.

## Estructura del módulo

```
00-setup/          Bootstrap: catálogo, esquemas, volume, descarga inicial de archivos
01-bronze/         Ingesta incremental con Auto Loader
02-silver/         Limpieza, validación, enriquecimiento con zonas
03-gold/           KPIs de negocio y feature tables para ML
04-ml/             Training, MLflow Registry, batch inference
05-streaming/      Versión streaming de Bronze→Silver
06-dashboard/      Queries SQL para dashboards de Databricks SQL
07-workflow/       Asset Bundle con el job orquestado
```

Cada carpeta tiene su propio README con la secuencia de ejecución sugerida.

## Secuencia recomendada (8 sesiones)

| Sesión | Tema | Carpetas |
|--------|------|----------|
| 1 | Setup + inspección del dataset | `00-setup/` |
| 2 | Bronze con Auto Loader | `01-bronze/` |
| 3 | Silver: limpieza y joins | `02-silver/` |
| 4 | Gold: KPIs y optimización (OPTIMIZE, ZORDER) | `03-gold/` |
| 5 | Dashboard en Databricks SQL | `06-dashboard/` |
| 6 | ML end-to-end con MLflow | `04-ml/` |
| 7 | Streaming con la misma arquitectura | `05-streaming/` |
| 8 | Orquestación con Workflows y Asset Bundles | `07-workflow/` |

## Ejecución rápida

```bash
# Desde Databricks CLI con Asset Bundles
databricks bundle deploy --target dev
databricks bundle run nyctaxi_pipeline --target dev
```

O notebook por notebook desde el workspace siguiendo el orden de las carpetas.

## Convenciones

- Cada estudiante trabaja en su propio catálogo: `<iniciales>_nytaxi` (ej: `casm_nytaxi`)
- Las iniciales se configuran en `00-setup/config.py` (constante `USER_INITIALS`)
- Nombres de tablas: `<iniciales>_nytaxi.<capa>.<entidad>` (ej: `casm_nytaxi.silver.trips_clean`)
- Widgets de notebook para parametrizar fechas y paths
- Todo el código en Python (PySpark) con SQL embebido donde sea más claro
- Variables de configuración centralizadas en `00-setup/config.py`
