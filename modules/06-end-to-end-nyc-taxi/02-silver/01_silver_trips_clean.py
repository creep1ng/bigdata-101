# Databricks notebook source
# MAGIC %md
# MAGIC # Silver: limpieza y validación de viajes
# MAGIC
# MAGIC Leemos Bronze, aplicamos reglas de calidad y separamos los viajes válidos
# MAGIC de los rechazados. Los rechazados van a una tabla aparte — no los tiramos,
# MAGIC nos sirven para análisis de calidad.

# COMMAND ----------

# MAGIC %run ../00-setup/config

# COMMAND ----------

from pyspark.sql import functions as F

configure_storage_access(spark)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Leer Bronze y calcular duración
# MAGIC
# MAGIC Ojo con los nombres de columna: Azure Open Datasets usa camelCase
# MAGIC (`tpepPickupDateTime`, `tripDistance`, `puLocationId`).

# COMMAND ----------

bronze = spark.table(T_BRONZE_TRIPS)

trips = bronze.withColumn(
    "trip_duration_min",
    (F.col("tpepDropoffDateTime").cast("long") - F.col("tpepPickupDateTime").cast("long")) / 60.0,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Definir las reglas de calidad como expresiones booleanas

# COMMAND ----------

rules = {
    "valid_distance":   F.col("tripDistance") > 0,
    "valid_fare":       F.col("fareAmount") >= 0,
    "valid_total":      F.col("totalAmount") > 0,
    "valid_passengers": F.col("passengerCount").between(1, 8),
    "valid_dropoff":    F.col("tpepDropoffDateTime") > F.col("tpepPickupDateTime"),
    "valid_duration":   F.col("trip_duration_min").between(1, 360),  # 1 min a 6 horas
}

# Combinamos todas con AND
is_valid = None
for expr in rules.values():
    is_valid = expr if is_valid is None else (is_valid & expr)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Marcar filas con cada regla y dividir

# COMMAND ----------

tagged = trips
for name, expr in rules.items():
    tagged = tagged.withColumn(name, expr)

valid_df = tagged.filter(is_valid).drop(*rules.keys())
rejected_df = (
    tagged.filter(~is_valid)
    .withColumn("rejected_at", F.current_timestamp())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Escribir ambas tablas

# COMMAND ----------

(
    valid_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("puYear", "puMonth")
    .saveAsTable(T_SILVER_TRIPS)
)

(
    rejected_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(T_SILVER_REJECTED)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Reporte rápido

# COMMAND ----------

total_bronze = bronze.count()
total_silver = spark.table(T_SILVER_TRIPS).count()
total_rejected = spark.table(T_SILVER_REJECTED).count()

print(f"Bronze total:    {total_bronze:>12,}")
print(f"Silver válidos:  {total_silver:>12,}  ({100 * total_silver / total_bronze:.2f}%)")
print(f"Silver rechazos: {total_rejected:>12,}  ({100 * total_rejected / total_bronze:.2f}%)")
