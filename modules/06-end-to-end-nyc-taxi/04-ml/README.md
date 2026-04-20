# 04 - Machine Learning

Entrenamiento, registro y despliegue de un modelo de regresión que predice la duración de un viaje en minutos a partir de información conocida al momento del pickup.

## Conceptos que se enseñan aquí

- **MLflow Tracking**: auto-logging de experimentos en Databricks
- **Unity Catalog Model Registry**: versionado y promoción de modelos
- **Batch inference** con el modelo registrado
- (Opcional) **Model Serving** como endpoint REST

## Orden de ejecución

1. `01_train_trip_duration.py` — Entrenar el modelo y loguearlo a MLflow
2. `02_register_model.py` — Registrar en Unity Catalog con aliases `@champion` / `@challenger`
3. `03_batch_inference.py` — Usar el modelo registrado para inferencia sobre Silver

## Modelo

Regresión con `GradientBoostingRegressor` o `XGBoost` sobre la tabla `ml.trip_duration_features`. El objetivo es predecir `target_duration_min`.

Baseline esperado: RMSE ~4-6 minutos, R² ~0.7-0.8 con 6 meses de datos.
