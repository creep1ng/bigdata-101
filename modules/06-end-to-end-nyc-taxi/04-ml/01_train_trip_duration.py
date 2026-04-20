# Databricks notebook source
# MAGIC %md
# MAGIC # Entrenamiento: predicción de duración de viaje
# MAGIC
# MAGIC - Lee la feature table de Gold/ML
# MAGIC - Split temporal (train con primeros meses, test con el último)
# MAGIC - Entrena un GradientBoostingRegressor con autologging de MLflow
# MAGIC - Mide RMSE, MAE y R² en test

# COMMAND ----------

# MAGIC %run ../00-setup/config

# COMMAND ----------

import mlflow
import mlflow.sklearn
import pandas as pd
from math import sqrt
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score

configure_storage_access(spark)

# Autologging de sklearn
mlflow.sklearn.autolog(log_input_examples=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Leer features y hacer split temporal

# COMMAND ----------

features_df = spark.table(T_ML_FEATURES)

# Tomamos una muestra manejable para entrenar en el driver
sample = features_df.sample(fraction=0.05, seed=42).toPandas()

cutoff = sample["pickup_date"].max() - pd.Timedelta(days=30)
train_df = sample[sample["pickup_date"] < cutoff]
test_df  = sample[sample["pickup_date"] >= cutoff]

print(f"Train: {len(train_df):,} filas")
print(f"Test:  {len(test_df):,} filas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Separar target y features

# COMMAND ----------

TARGET = "target_duration_min"
CATEGORICAL = ["pickup_borough", "dropoff_borough"]
NUMERIC = [
    "puLocationId", "doLocationId", "pickup_hour", "pickup_dayofweek",
    "is_weekend", "is_rush_hour", "passengerCount", "tripDistance", "rateCodeId",
]

X_train = train_df[CATEGORICAL + NUMERIC]
y_train = train_df[TARGET]
X_test  = test_df[CATEGORICAL + NUMERIC]
y_test  = test_df[TARGET]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Pipeline con preprocesamiento + modelo

# COMMAND ----------

preprocessor = ColumnTransformer(
    transformers=[
        ("cat", OneHotEncoder(handle_unknown="ignore"), CATEGORICAL),
        ("num", "passthrough", NUMERIC),
    ]
)

pipeline = Pipeline([
    ("prep", preprocessor),
    ("model", GradientBoostingRegressor(
        n_estimators=100,
        max_depth=5,
        learning_rate=0.1,
        random_state=42,
    )),
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Entrenar con tracking automático

# COMMAND ----------

with mlflow.start_run(run_name=f"{USER_INITIALS}_gbr_trip_duration") as run:
    pipeline.fit(X_train, y_train)

    preds = pipeline.predict(X_test)
    rmse = sqrt(mean_squared_error(y_test, preds))
    mae = mean_absolute_error(y_test, preds)
    r2 = r2_score(y_test, preds)

    mlflow.log_metric("test_rmse", rmse)
    mlflow.log_metric("test_mae", mae)
    mlflow.log_metric("test_r2", r2)
    mlflow.log_param("train_rows", len(train_df))
    mlflow.log_param("test_rows", len(test_df))
    mlflow.log_param("student", USER_INITIALS)

    run_id = run.info.run_id
    print(f"Run: {run_id}")
    print(f"RMSE test: {rmse:.3f} min")
    print(f"MAE  test: {mae:.3f} min")
    print(f"R²   test: {r2:.4f}")

# COMMAND ----------

# Guardamos el run_id para el siguiente notebook
dbutils.jobs.taskValues.set(key="training_run_id", value=run_id)
