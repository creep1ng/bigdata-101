# 02 - Silver

Limpieza, validación y enriquecimiento. Los datos de Bronze pasan por reglas de calidad y se unen con la tabla de zonas para tener nombres legibles.

## Conceptos que se enseñan aquí

- Reglas de calidad con `filter` + tabla de **rechazos** para no perder datos
- **Schema enforcement**: tipos estrictos en Silver
- **Broadcast join** con una tabla pequeña (zonas) → join eficiente sin shuffle
- Derivar columnas analíticas (duración, hora del día, día de semana)
- Idempotencia con `overwrite` + `partitionBy` — reprocesable desde Bronze

## Orden de ejecución

1. `01_silver_trips_clean.py` — Aplicar reglas de limpieza y separar válidos/rechazados
2. `02_silver_trips_enriched.py` — Enriquecer con zonas y features temporales
3. `03_data_quality_report.py` — Reporte de calidad y métricas de rechazo

## Reglas de limpieza aplicadas

| # | Regla | Por qué |
|---|-------|---------|
| 1 | `trip_distance > 0` | Trip con 0 millas es un error del taxímetro |
| 2 | `fare_amount >= 0` | Tarifas negativas son reembolsos, no viajes |
| 3 | `total_amount > 0` | Lo mismo |
| 4 | `passenger_count BETWEEN 1 AND 8` | Taxis caben hasta 6-8 pasajeros; 0 o null es error |
| 5 | `tpep_dropoff_datetime > tpep_pickup_datetime` | El dropoff debe ser posterior al pickup |
| 6 | `duration BETWEEN 1 minuto Y 6 horas` | Viajes fuera de ese rango son anómalos |
| 7 | `PULocationID` y `DOLocationID` en el lookup | IDs que no existen se descartan |
