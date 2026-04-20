# 03 - Gold

Agregaciones de negocio y feature tables. Los datos de Silver se consolidan en métricas pre-calculadas listas para consumo.

## Conceptos que se enseñan aquí

- Agregaciones con `GROUP BY` sobre tablas grandes
- Feature engineering para ML (separado de los KPIs de negocio)
- Particionamiento y `ZORDER` según patrones de consulta
- Modelado dimensional básico (hechos + dimensiones)

## Orden de ejecución

1. `01_gold_revenue_by_zone.py` — Revenue agregado por zona, útil para heatmaps
2. `02_gold_daily_metrics.py` — Métricas diarias (trips, revenue, avg_fare)
3. `03_gold_hourly_demand.py` — Demanda por hora para detectar patrones
4. `04_gold_ml_features.py` — Feature table para el modelo de predicción de duración

## Tablas generadas

| Tabla | Propósito |
|-------|-----------|
| `gold.revenue_by_zone` | KPI: revenue total, trips y propinas por zona |
| `gold.daily_metrics` | Timeseries diario para dashboards |
| `gold.hourly_demand` | Demanda hora-zona para heatmaps |
| `ml.trip_duration_features` | Features listas para entrenar modelo |
