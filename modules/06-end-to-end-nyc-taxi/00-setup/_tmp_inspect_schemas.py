# Databricks notebook source
# MAGIC %md
# MAGIC # 🔍 Inspección de esquemas Parquet por mes
# MAGIC
# MAGIC Notebook temporal para comparar los esquemas de cada archivo Parquet
# MAGIC mensual del landing y detectar inconsistencias de tipos entre meses.
# MAGIC
# MAGIC **Borrar después de usar.**

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Leer el esquema de cada archivo individual

# COMMAND ----------

landing_path = "/Volumes/nytaxi_landing/raw/files/yellow_trips"

months = ["2023-01", "2023-02", "2023-03"]
schemas = {}

for m in months:
    path = f"{landing_path}/yellow_tripdata_{m}.parquet"
    df = spark.read.parquet(path)
    schemas[m] = {field.name: str(field.dataType) for field in df.schema.fields}
    print(f"\n{'='*60}")
    print(f"  {m}  —  {len(df.schema.fields)} columnas")
    print(f"{'='*60}")
    for field in df.schema.fields:
        print(f"  {field.name:30s} {str(field.dataType):20s} nullable={field.nullable}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Comparar diferencias entre meses

# COMMAND ----------

# Recopilar todas las columnas
all_columns = sorted(set(col for s in schemas.values() for col in s))

print(f"{'Columna':30s} | {'2023-01':15s} | {'2023-02':15s} | {'2023-03':15s} | Diff?")
print("-" * 105)

diffs = []
for col in all_columns:
    types = [schemas[m].get(col, "—MISSING—") for m in months]
    has_diff = len(set(types)) > 1
    marker = " ⚠️" if has_diff else ""
    print(f"{col:30s} | {types[0]:15s} | {types[1]:15s} | {types[2]:15s} |{marker}")
    if has_diff:
        diffs.append((col, dict(zip(months, types))))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Resumen de inconsistencias

# COMMAND ----------

if diffs:
    print(f"Se encontraron {len(diffs)} columna(s) con tipos inconsistentes:\n")
    for col_name, type_map in diffs:
        print(f"  ⚠️  {col_name}:")
        for month, dtype in type_map.items():
            print(f"       {month}: {dtype}")
        print()
    print("Recomendación: usar LongType() para estas columnas en LANDING_SCHEMA")
    print("y castear a DoubleType después de la lectura.")
else:
    print("✅ Todos los archivos tienen el mismo esquema. El problema puede ser")
    print("   de nombres de columna (mayúsculas/minúsculas) — revisar arriba.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Verificar nombres de columna (case-sensitive)

# COMMAND ----------

for m in months:
    path = f"{landing_path}/yellow_tripdata_{m}.parquet"
    df = spark.read.parquet(path)
    col_names = df.columns
    print(f"{m}: {col_names}")
