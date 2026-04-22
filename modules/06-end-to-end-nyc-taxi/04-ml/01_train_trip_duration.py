# Databricks notebook source
# MAGIC %md
# MAGIC # Entrenamiento: predicción de duración de viaje

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

spark.sql(f"USE CATALOG {CATALOG}")
mlflow.set_registry_uri("databricks-uc")
mlflow.sklearn.autolog(log_input_examples=False)

# COMMAND ----------

features_df = spark.table(T_ML_FEATURES)
sample = features_df.sample(fraction=0.05, seed=42).toPandas()

cutoff = sample["pickup_date"].max() - pd.Timedelta(days=30)
train_df = sample[sample["pickup_date"] < cutoff]
test_df  = sample[sample["pickup_date"] >= cutoff]

print(f"Train: {len(train_df):,} | Test: {len(test_df):,}")

# COMMAND ----------

TARGET = "target_duration_min"
CATEGORICAL = ["pickup_borough", "dropoff_borough"]
NUMERIC = [
    "PULocationID", "DOLocationID", "pickup_hour", "pickup_dayofweek",
    "is_weekend", "is_rush_hour", "passenger_count", "trip_distance", "RatecodeID",
]

X_train, y_train = train_df[CATEGORICAL + NUMERIC], train_df[TARGET]
X_test,  y_test  = test_df[CATEGORICAL + NUMERIC],  test_df[TARGET]

# COMMAND ----------

preprocessor = ColumnTransformer([
    ("cat", OneHotEncoder(handle_unknown="ignore"), CATEGORICAL),
    ("num", "passthrough", NUMERIC),
])

pipeline = Pipeline([
    ("prep", preprocessor),
    ("model", GradientBoostingRegressor(n_estimators=100, max_depth=5, learning_rate=0.1, random_state=42)),
])

with mlflow.start_run(run_name=f"{USER_INITIALS}_gbr_trip_duration") as run:
    pipeline.fit(X_train, y_train)
    preds = pipeline.predict(X_test)

    rmse = sqrt(mean_squared_error(y_test, preds))
    mae = mean_absolute_error(y_test, preds)
    r2 = r2_score(y_test, preds)

    mlflow.log_metrics({"test_rmse": rmse, "test_mae": mae, "test_r2": r2})
    mlflow.log_params({"train_rows": len(train_df), "test_rows": len(test_df), "student": USER_INITIALS})

    run_id = run.info.run_id
    print(f"Run: {run_id}")
    print(f"RMSE: {rmse:.3f} | MAE: {mae:.3f} | R²: {r2:.4f}")

# COMMAND ----------

dbutils.jobs.taskValues.set(key="training_run_id", value=run_id)
