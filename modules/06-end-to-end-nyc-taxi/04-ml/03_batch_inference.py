# Databricks notebook source
# MAGIC %md
# MAGIC # Batch inference con el modelo en Staging
# MAGIC
# MAGIC Cargamos el modelo desde el MLflow workspace registry y lo aplicamos
# MAGIC a los viajes recientes.

# COMMAND ----------

# MAGIC %run ../00-setup/config

# COMMAND ----------

import mlflow
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

configure_storage_access(spark)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Cargar el modelo como UDF de Spark
# MAGIC
# MAGIC `mlflow.pyfunc.spark_udf` crea una UDF que puede aplicarse
# MAGIC distribuidamente — ideal para inferencia a escala.

# COMMAND ----------

# Stage 'Staging' del workspace registry
model_uri = f"models:/{MODEL_NAME}/Staging"
predict_udf = mlflow.pyfunc.spark_udf(spark, model_uri, result_type=DoubleType())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Seleccionar los viajes recientes

# COMMAND ----------

features_df = spark.table(T_ML_FEATURES)
max_date = features_df.agg(F.max("pickup_date")).first()[0]
recent = features_df.filter(F.col("pickup_date") >= F.date_sub(F.lit(max_date), 7))

print(f"Filas a predecir: {recent.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Aplicar el modelo

# COMMAND ----------

FEATURE_COLS = [
    "pickup_borough", "dropoff_borough",
    "puLocationId", "doLocationId", "pickup_hour", "pickup_dayofweek",
    "is_weekend", "is_rush_hour", "passengerCount", "tripDistance", "rateCodeId",
]

predictions = (
    recent
    .withColumn("predicted_duration_min", predict_udf(*FEATURE_COLS))
    .withColumn("prediction_error_min", F.col("target_duration_min") - F.col("predicted_duration_min"))
    .withColumn("predicted_at", F.current_timestamp())
)

# COMMAND ----------

(
    predictions.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(T_ML_PREDICTIONS)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Monitoreo: errores del modelo por día

# COMMAND ----------

display(spark.sql(f"""
    SELECT
      pickup_date,
      COUNT(*)                                         AS predictions,
      ROUND(AVG(prediction_error_min), 2)              AS avg_error,
      ROUND(SQRT(AVG(POW(prediction_error_min, 2))), 2) AS rmse,
      ROUND(AVG(ABS(prediction_error_min)), 2)         AS mae
    FROM {T_ML_PREDICTIONS}
    GROUP BY pickup_date
    ORDER BY pickup_date
"""))
