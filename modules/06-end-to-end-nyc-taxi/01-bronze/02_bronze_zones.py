# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze: tabla estática de zonas
# MAGIC
# MAGIC Lee el CSV de zonas que el profesor dejó en el landing y lo persiste
# MAGIC como Delta en Unity Catalog.

# COMMAND ----------

# MAGIC %run ../00-setup/config

# COMMAND ----------

spark.sql(f"USE CATALOG {CATALOG}")

# COMMAND ----------

zones_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(f"{LANDING_ZONES_PATH}/taxi_zone_lookup.csv")
)

zones_df.printSchema()
zones_df.display()

# COMMAND ----------

(
    zones_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(T_BRONZE_ZONES)
)

print(f"✓ Zones escritas en {T_BRONZE_ZONES}")

# COMMAND ----------

display(spark.sql(f"""
    SELECT COUNT(*) AS total_zones, COUNT(DISTINCT Borough) AS boroughs
    FROM {T_BRONZE_ZONES}
"""))
