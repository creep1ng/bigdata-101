# Databricks notebook source
# MAGIC %md
# MAGIC # Registro del modelo en el MLflow Workspace Registry
# MAGIC
# MAGIC Sin Unity Catalog registramos el modelo en el **workspace registry**
# MAGIC (el legacy). Cada estudiante tiene su propio nombre de modelo con
# MAGIC sus iniciales (`MODEL_NAME` en config).
# MAGIC
# MAGIC Usamos el patrón de stages clásico (`None` → `Staging` → `Production`
# MAGIC → `Archived`).

# COMMAND ----------

# MAGIC %run ../00-setup/config

# COMMAND ----------

import mlflow
from mlflow.tracking import MlflowClient

# IMPORTANTE: sin UC usamos el registry por defecto (workspace)
# No llamamos mlflow.set_registry_uri("databricks-uc") acá.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Obtener el run_id del notebook anterior

# COMMAND ----------

run_id = dbutils.jobs.taskValues.get(
    taskKey="train",
    key="training_run_id",
    default=None,
    debugValue="",
)

if not run_id:
    client = MlflowClient()
    active = mlflow.active_run()
    exp_id = active.info.experiment_id if active else None
    if exp_id:
        runs = client.search_runs(
            experiment_ids=[exp_id],
            order_by=["start_time DESC"],
            max_results=1,
        )
        run_id = runs[0].info.run_id

assert run_id, "No se encontró run_id. Corre primero el notebook de training."
print(f"Registrando modelo del run: {run_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Registrar el modelo

# COMMAND ----------

model_uri = f"runs:/{run_id}/model"
registered = mlflow.register_model(model_uri=model_uri, name=MODEL_NAME)

print(f"✓ Registrado {MODEL_NAME} versión {registered.version}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Promover la versión a Staging

# COMMAND ----------

client = MlflowClient()
client.transition_model_version_stage(
    name=MODEL_NAME,
    version=registered.version,
    stage="Staging",
    archive_existing_versions=False,
)
print(f"✓ Versión {registered.version} movida a Staging")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Inspección

# COMMAND ----------

for v in client.search_model_versions(f"name='{MODEL_NAME}'"):
    print(f"  v{v.version}  stage={v.current_stage}  run={v.run_id}")
