# Databricks notebook source
# MAGIC %md
# MAGIC # Exploración del dataset crudo
# MAGIC
# MAGIC Antes de ingestar, inspeccionamos el esquema y el contenido de los
# MAGIC archivos para entender los datos. Esto es lo primero que hace un
# MAGIC ingeniero de datos frente a una fuente nueva.

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Esquema inferido por Spark

# COMMAND ----------

df = spark.read.parquet(LANDING_PATH)
df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Conteo total y rango de fechas

# COMMAND ----------

from pyspark.sql import functions as F

df.agg(
    F.count("*").alias("total_rows"),
    F.min("tpep_pickup_datetime").alias("min_pickup"),
    F.max("tpep_pickup_datetime").alias("max_pickup"),
).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Muestra de filas

# COMMAND ----------

df.limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Estadísticas de columnas numéricas clave

# COMMAND ----------

df.select(
    "passenger_count", "trip_distance", "fare_amount",
    "tip_amount", "total_amount",
).describe().display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Señales de datos sucios
# MAGIC
# MAGIC Hay cosas que no pueden ser ciertas y nos van a servir para diseñar
# MAGIC las reglas de limpieza en Silver.

# COMMAND ----------

df.select(
    F.sum(F.when(F.col("trip_distance") <= 0, 1).otherwise(0)).alias("distance_zero_or_neg"),
    F.sum(F.when(F.col("fare_amount") < 0, 1).otherwise(0)).alias("negative_fare"),
    F.sum(F.when(F.col("passenger_count") == 0, 1).otherwise(0)).alias("zero_passengers"),
    F.sum(F.when(F.col("passenger_count").isNull(), 1).otherwise(0)).alias("null_passengers"),
    F.sum(F.when(F.col("tpep_dropoff_datetime") <= F.col("tpep_pickup_datetime"), 1).otherwise(0)).alias("dropoff_before_pickup"),
).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Inspección del lookup de zonas

# COMMAND ----------

zones = (
    spark.read.option("header", True).option("inferSchema", True)
    .csv(f"{ZONES_PATH}/taxi_zone_lookup.csv")
)
zones.display()
print(f"Total zones: {zones.count()}")
