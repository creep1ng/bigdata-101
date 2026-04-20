# Databricks notebook source
# MAGIC %md
# MAGIC # Exploración del landing (Azure Open Datasets)
# MAGIC
# MAGIC Antes de ingestar, inspeccionamos el esquema y el contenido. Esto es
# MAGIC lo primero que hace un ingeniero de datos frente a una fuente nueva.
# MAGIC
# MAGIC El landing es un blob público de Azure Open Datasets. Se accede con
# MAGIC un SAS token de lectura `"r"` — ya viene configurado en `config.py`.

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

configure_landing_access(spark)
print(f"Landing: {LANDING_WASBS}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Lectura perezosa del landing completo
# MAGIC
# MAGIC Los archivos están organizados por particiones anuales. Spark hace
# MAGIC partition discovery automáticamente. Esto no carga datos aún.

# COMMAND ----------

df = spark.read.parquet(LANDING_WASBS)
df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Conteo y rango de fechas (todo el dataset)
# MAGIC
# MAGIC Atención: esto SÍ escanea la metadata de miles de archivos.
# MAGIC Con ~1.5B filas totales puede tardar 1-2 minutos.

# COMMAND ----------

from pyspark.sql import functions as F

df.agg(
    F.count("*").alias("total_rows"),
    F.min("tpepPickupDateTime").alias("min_pickup"),
    F.max("tpepPickupDateTime").alias("max_pickup"),
    F.countDistinct("puYear").alias("years_available"),
).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Muestra filtrada por año/mes
# MAGIC
# MAGIC Con el filtro sobre `puYear` y `puMonth` Spark hace partition pruning
# MAGIC y solo lee unos pocos archivos.

# COMMAND ----------

sample = (
    df
    .filter((F.col("puYear") == YEAR_FROM) & (F.col("puMonth") == MONTH_FROM))
    .limit(10)
)
sample.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Estadísticas rápidas sobre una ventana pequeña

# COMMAND ----------

window = df.filter(
    (F.col("puYear") == YEAR_FROM) & (F.col("puMonth") == MONTH_FROM)
)

window.select(
    "passengerCount", "tripDistance", "fareAmount",
    "tipAmount", "totalAmount",
).describe().display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Señales de datos sucios
# MAGIC
# MAGIC Cosas que no pueden ser ciertas y que nos servirán para diseñar las
# MAGIC reglas de limpieza en Silver.

# COMMAND ----------

window.select(
    F.sum(F.when(F.col("tripDistance") <= 0, 1).otherwise(0)).alias("distance_zero_or_neg"),
    F.sum(F.when(F.col("fareAmount") < 0, 1).otherwise(0)).alias("negative_fare"),
    F.sum(F.when(F.col("passengerCount") == 0, 1).otherwise(0)).alias("zero_passengers"),
    F.sum(F.when(F.col("passengerCount").isNull(), 1).otherwise(0)).alias("null_passengers"),
    F.sum(F.when(F.col("tpepDropoffDateTime") <= F.col("tpepPickupDateTime"), 1).otherwise(0)).alias("dropoff_before_pickup"),
).display()
