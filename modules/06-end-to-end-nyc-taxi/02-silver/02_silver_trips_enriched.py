# Databricks notebook source
# MAGIC %md
# MAGIC # Silver: viajes enriquecidos
# MAGIC
# MAGIC Tomamos la tabla limpia y la enriquecemos con:
# MAGIC - Nombres legibles de zona y borough (join con `taxi_zones`)
# MAGIC - Features temporales derivadas (hora, día de semana, fin de semana)
# MAGIC - Métricas económicas (tip_rate, cost_per_mile)

# COMMAND ----------

# MAGIC %run ../00-setup/config

# COMMAND ----------

from pyspark.sql import functions as F

configure_storage_access(spark)

# COMMAND ----------

trips = spark.table(T_SILVER_TRIPS)
zones = spark.table(T_BRONZE_ZONES)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Join con zonas
# MAGIC
# MAGIC La tabla de zonas tiene 265 filas → hint `broadcast` para que Spark la
# MAGIC distribuya a todos los executors y evite shuffle.
# MAGIC
# MAGIC OJO: en Azure Open Datasets las columnas se llaman `puLocationId`
# MAGIC y `doLocationId` (camelCase). El lookup usa `LocationID` (PascalCase).

# COMMAND ----------

zones_pickup = (
    zones.select(
        F.col("LocationID").cast("string").alias("puLocationId"),
        F.col("Borough").alias("pickup_borough"),
        F.col("Zone").alias("pickup_zone"),
        F.col("service_zone").alias("pickup_service_zone"),
    )
)

zones_dropoff = (
    zones.select(
        F.col("LocationID").cast("string").alias("doLocationId"),
        F.col("Borough").alias("dropoff_borough"),
        F.col("Zone").alias("dropoff_zone"),
        F.col("service_zone").alias("dropoff_service_zone"),
    )
)

enriched = (
    trips
    .join(F.broadcast(zones_pickup),  on="puLocationId", how="left")
    .join(F.broadcast(zones_dropoff), on="doLocationId", how="left")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Features temporales y económicas

# COMMAND ----------

enriched = (
    enriched
    .withColumn("pickup_date", F.to_date("tpepPickupDateTime"))
    .withColumn("pickup_hour", F.hour("tpepPickupDateTime"))
    .withColumn("pickup_dayofweek", F.dayofweek("tpepPickupDateTime"))
    .withColumn("is_weekend", F.col("pickup_dayofweek").isin(1, 7))
    .withColumn("is_rush_hour",
        F.col("pickup_hour").between(7, 9) | F.col("pickup_hour").between(17, 19))
    .withColumn("tip_rate",
        F.when(F.col("fareAmount") > 0, F.col("tipAmount") / F.col("fareAmount"))
        .otherwise(F.lit(0.0)))
    .withColumn("cost_per_mile",
        F.when(F.col("tripDistance") > 0, F.col("totalAmount") / F.col("tripDistance"))
        .otherwise(F.lit(None)))
    .withColumn("avg_speed_mph",
        F.when(F.col("trip_duration_min") > 0,
               F.col("tripDistance") / (F.col("trip_duration_min") / 60.0))
        .otherwise(F.lit(None)))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Escribir Silver enriched

# COMMAND ----------

(
    enriched.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("puYear", "puMonth")
    .saveAsTable(T_SILVER_TRIPS_ENRICHED)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Optimización física
# MAGIC
# MAGIC `OPTIMIZE` compacta archivos pequeños y `ZORDER` ordena por columnas
# MAGIC frecuentes en filtros.

# COMMAND ----------

spark.sql(f"OPTIMIZE {T_SILVER_TRIPS_ENRICHED} ZORDER BY (puLocationId, pickup_hour)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Validación rápida

# COMMAND ----------

display(spark.sql(f"""
    SELECT
      pickup_borough,
      COUNT(*) AS trips,
      ROUND(AVG(totalAmount), 2) AS avg_fare,
      ROUND(AVG(tip_rate) * 100, 1) AS avg_tip_pct
    FROM {T_SILVER_TRIPS_ENRICHED}
    WHERE pickup_borough IS NOT NULL
    GROUP BY pickup_borough
    ORDER BY trips DESC
"""))
