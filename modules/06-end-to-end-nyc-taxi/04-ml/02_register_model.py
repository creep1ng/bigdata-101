# Databricks notebook source
# MAGIC %md
# MAGIC # Registro del modelo en Unity Catalog

# COMMAND ----------

# MAGIC %run ../00-setup/config

# COMMAND ----------

import mlflow
from mlflow.tracking import MlflowClient

spark.sql(f"USE CATALOG {CATALOG}")
mlflow.set_registry_uri("databricks-uc")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Crear el schema ML si no existe (para el modelo)

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA_ML}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Obtener run_id

# COMMAND ----------

run_id = dbutils.jobs.taskValues.get(taskKey="train", key="training_run_id", default=None, debugValue="")

if not run_id:
    client = MlflowClient()
    # Buscar en el experimento activo del notebook actual
    experiment = mlflow.get_experiment_by_name(
        f"/Users/{spark.conf.get('spark.databricks.notebook.path', '')}"
    )
    if experiment is None:
        # Fallback: usar el experimento por defecto del notebook
        experiment_id = mlflow.tracking.fluent._get_experiment_id()
    else:
        experiment_id = experiment.experiment_id
    runs = client.search_runs(
        experiment_ids=[experiment_id],
        order_by=["start_time DESC"],
        max_results=1,
    )
    run_id = runs[0].info.run_id if runs else None

assert run_id, "No se encontró run_id. Corre primero el notebook de training."
print(f"Registrando modelo del run: {run_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Registrar en UC

# COMMAND ----------

model_uri = f"runs:/{run_id}/model"
registered = mlflow.register_model(model_uri=model_uri, name=MODEL_NAME)
print(f"✓ Registrado {MODEL_NAME} versión {registered.version}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Asignar alias `challenger`

# COMMAND ----------

client = MlflowClient()
client.set_registered_model_alias(name=MODEL_NAME, alias="challenger", version=registered.version)
print(f"✓ Alias 'challenger' asignado a versión {registered.version}")
