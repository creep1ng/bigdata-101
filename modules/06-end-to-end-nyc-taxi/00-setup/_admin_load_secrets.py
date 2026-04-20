# Databricks notebook source
# MAGIC %md
# MAGIC # ADMIN ONLY — Carga de access keys al secret scope
# MAGIC
# MAGIC Este notebook lo corre SOLO el profesor. Crea el secret scope
# MAGIC `nytaxi-course` y carga las access keys de los storages de cada
# MAGIC estudiante.
# MAGIC
# MAGIC Los estudiantes nunca ven ni editan este archivo.
# MAGIC
# MAGIC ## Prerrequisitos
# MAGIC
# MAGIC 1. Tener el archivo local `.tmp/student-keys.txt` con formato:
# MAGIC    ```
# MAGIC    iniciales|storage_account|access_key
# MAGIC    ```
# MAGIC    (Se genera con el script `extract_keys.sh` del profesor)
# MAGIC
# MAGIC 2. Tener `databricks-cli` configurado localmente apuntando al workspace.
# MAGIC
# MAGIC ## Cómo usar este notebook
# MAGIC
# MAGIC Este notebook NO se corre dentro de Databricks — los secrets los carga
# MAGIC el profesor **desde la terminal local** usando la Databricks CLI, porque
# MAGIC `dbutils.secrets.put()` no existe.
# MAGIC
# MAGIC El notebook sirve solo como **referencia documental** y para **verificar**
# MAGIC que los secrets se cargaron bien.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parte 1 — Carga de secrets (CLI LOCAL, no en Databricks)
# MAGIC
# MAGIC ```bash
# MAGIC # Instalar CLI si no la tienes:
# MAGIC curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh
# MAGIC databricks configure --host https://adb-7405618790693465.5.azuredatabricks.net
# MAGIC
# MAGIC # Crear el secret scope (una sola vez)
# MAGIC databricks secrets create-scope nytaxi-course
# MAGIC
# MAGIC # Cargar las keys desde el archivo generado
# MAGIC while IFS='|' read -r initials account key; do
# MAGIC   [[ "$initials" =~ ^#.*$ ]] && continue
# MAGIC   [[ -z "$initials" ]] && continue
# MAGIC   echo "Cargando adls-key-$initials..."
# MAGIC   databricks secrets put-secret nytaxi-course "adls-key-$initials" --string-value "$key"
# MAGIC done < .tmp/student-keys.txt
# MAGIC
# MAGIC # Verificar
# MAGIC databricks secrets list-secrets nytaxi-course
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parte 2 — Permisos del scope
# MAGIC
# MAGIC Por defecto solo el creador del scope puede leerlo. Hay que dar
# MAGIC permiso READ al grupo de estudiantes:
# MAGIC
# MAGIC ```bash
# MAGIC databricks secrets put-acl nytaxi-course bigdata-students READ
# MAGIC databricks secrets list-acls nytaxi-course
# MAGIC ```
# MAGIC
# MAGIC **Importante:** aunque todos los estudiantes tienen READ sobre el scope,
# MAGIC cada uno solo debe USAR su propio secret (`adls-key-<sus-iniciales>`).
# MAGIC Técnicamente cualquier estudiante podría leer la key de otro — si eso
# MAGIC es un riesgo, la alternativa es crear un scope por estudiante. Para
# MAGIC un curso con profesor y confianza razonable entre alumnos, un scope
# MAGIC compartido es suficiente.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parte 3 — Verificación desde Databricks
# MAGIC
# MAGIC Esto sí se corre dentro de Databricks para confirmar que los secrets
# MAGIC están visibles.

# COMMAND ----------

# Listar los secrets del scope
secrets = dbutils.secrets.list("nytaxi-course")
print(f"Secrets disponibles en el scope 'nytaxi-course': {len(secrets)}")
for s in secrets:
    print(f"  {s.key}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parte 4 — Prueba de lectura
# MAGIC
# MAGIC Probar acceso al storage de un estudiante (cambia las iniciales).
# MAGIC NO imprime la key, solo valida que se puede leer y que funciona.

# COMMAND ----------

TEST_INITIALS = "soto"
TEST_ACCOUNT = f"dl25604{TEST_INITIALS}"

# Leer la key del scope
key = dbutils.secrets.get("nytaxi-course", f"adls-key-{TEST_INITIALS}")
print(f"Key leída para {TEST_INITIALS}: {len(key)} caracteres (no se imprime)")

# Configurar acceso al storage
spark.conf.set(
    f"fs.azure.account.key.{TEST_ACCOUNT}.dfs.core.windows.net",
    key,
)

# Probar que podemos listar el container
try:
    files = dbutils.fs.ls(f"abfss://nytaxi@{TEST_ACCOUNT}.dfs.core.windows.net/")
    print(f"✓ Acceso OK a {TEST_ACCOUNT}/nytaxi ({len(files)} items)")
except Exception as exc:
    print(f"✗ Error: {exc}")
