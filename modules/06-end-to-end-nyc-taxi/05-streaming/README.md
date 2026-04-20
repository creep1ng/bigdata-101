# 05 - Streaming (opcional)

Versión streaming de Bronze → Silver usando la misma arquitectura Medallion. Sirve para mostrar que Delta Lake y Auto Loader soportan batch y streaming con el mismo código — uno de los diferenciadores del Lakehouse.

## Conceptos que se enseñan aquí

- Structured Streaming sobre Delta
- `foreachBatch` para escrituras complejas
- Watermarks y manejo de datos tardíos
- `readStream` sobre una tabla Delta (CDC implícito)

## Orden de ejecución

1. `01_streaming_bronze.py` — Auto Loader con trigger continuo
2. `02_streaming_silver.py` — Stream sobre Bronze que alimenta Silver

## Nota

Esta sección es **demostrativa**. Para correr de verdad necesitas:
- Un cluster siempre encendido (costoso)
- Archivos llegando periódicamente (puedes usar `99_simulate_arrivals.py` para simularlos)

Para fines del curso, correr los notebooks por unos minutos para ver la UI de streaming es suficiente.
