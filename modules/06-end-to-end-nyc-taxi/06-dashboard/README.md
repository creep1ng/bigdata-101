# 06 - Dashboard

Queries SQL para construir un dashboard en **Databricks SQL**. El dashboard se crea desde la UI apuntando a un SQL Warehouse y cada widget usa una de las queries de esta carpeta.

## Dashboard sugerido: "NYC Taxi Operations"

| Widget | Tipo | Query |
|--------|------|-------|
| Trips totales | Counter | `01_kpi_totals.sql` |
| Revenue total | Counter | `01_kpi_totals.sql` |
| Trips diarios | Timeseries | `02_daily_trend.sql` |
| Top 10 zonas por revenue | Bar | `03_top_zones_revenue.sql` |
| Heatmap hora × día | Heatmap | `04_heatmap_demand.sql` |
| Top pares origen→destino | Table | `05_top_trip_pairs.sql` |
| Distribución de propinas | Histogram | `06_tip_distribution.sql` |

## Cómo crear el dashboard

1. Ir a **SQL** → **Dashboards** → **New Dashboard**
2. Agregar un **parámetro** llamado `catalog` con valor por defecto `<tus-iniciales>_nytaxi`
   (por ejemplo `casm_nytaxi`). Las queries lo usan con `IDENTIFIER(:catalog || '.silver.trips_enriched')`.
3. Agregar parámetros `start_date`, `end_date` y `borough` (opcional)
4. Agregar cada widget usando el SQL Warehouse como compute
5. Conectar filtros globales (rango de fechas, borough)
6. Compartir con el grupo `bigdata-students`

## Databricks Marketplace y AI/BI

Una vez el dashboard funciona, se puede:
- Publicar como producto de datos vía **Delta Sharing**
- Agregar AI/BI Genie para que usuarios de negocio hagan preguntas en lenguaje natural
