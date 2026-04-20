# Guía de Setup para el Profesor/Admin

Esta guía es SOLO para el profesor o administrador del workspace. Los estudiantes nunca hacen estos pasos. Si los completas bien una vez, cada estudiante solo debe cambiar sus iniciales en `config.py` y correr los notebooks.

## Qué vas a configurar

1. Una **storage account ADLS Gen2** con un container compartido
2. Un **Access Connector for Azure Databricks** que actúa como managed identity
3. Permisos del connector sobre el container
4. Un **metastore** de Unity Catalog (si no existe) asignado al workspace
5. Una **storage credential** y una **external location** en Unity Catalog
6. Un **grupo de estudiantes** con los privilegios correctos
7. Un **SQL Warehouse** compartido (opcional pero recomendado para el dashboard)

Al final, cada estudiante crea SU catálogo `<iniciales>_nytaxi` dentro del mismo container, sin colisiones.

## Prerrequisitos

- Ser **Azure Subscription Owner** (o tener `Contributor` + `User Access Administrator`) sobre la suscripción donde va el storage
- Ser **Databricks Account Admin** en el account console (portal `accounts.azuredatabricks.net`). Ser Subscription Owner NO te hace automáticamente account admin.
- Azure CLI instalada localmente (opcional pero ayuda)

## Paso 1 — Crear la storage account y el container

Desde Azure Portal o con Azure CLI:

```bash
# Variables
RG="rg-upb-bigdata"
LOCATION="eastus2"
STORAGE="upbbigdatastorage"            # debe ser único globalmente, solo minúsculas y números
CONTAINER="nyctaxi-lake"

# Resource group
az group create --name $RG --location $LOCATION

# Storage account con hierarchical namespace (ADLS Gen2)
az storage account create \
  --name $STORAGE \
  --resource-group $RG \
  --location $LOCATION \
  --sku Standard_LRS \
  --kind StorageV2 \
  --hierarchical-namespace true

# Container
az storage container create \
  --name $CONTAINER \
  --account-name $STORAGE \
  --auth-mode login
```

Apunta: `<container>=nyctaxi-lake` y `<storage-account>=upbbigdatastorage`. Estos son los valores que cada estudiante pone en `config.py`.

## Paso 2 — Crear el Access Connector

El Access Connector es el recurso que Databricks usa para autenticarse contra el storage sin secrets.

Desde Portal: **Create a resource** → buscar "Access Connector for Azure Databricks" → crearlo en el mismo resource group.

Con Azure CLI:
```bash
CONNECTOR="upb-databricks-connector"

az databricks access-connector create \
  --name $CONNECTOR \
  --resource-group $RG \
  --location $LOCATION \
  --identity-type SystemAssigned
```

Apunta el **resource ID** del connector:
```
/subscriptions/<sub-id>/resourceGroups/<rg>/providers/Microsoft.Databricks/accessConnectors/<connector-name>
```

## Paso 3 — Dar permisos al connector sobre el container

El connector tiene una managed identity; hay que darle el rol **Storage Blob Data Contributor** sobre el container.

```bash
# Obtener el principal ID del connector
PRINCIPAL_ID=$(az databricks access-connector show \
  --name $CONNECTOR --resource-group $RG \
  --query identity.principalId -o tsv)

# Scope del container
SCOPE=$(az storage account show \
  --name $STORAGE --resource-group $RG \
  --query id -o tsv)"/blobServices/default/containers/$CONTAINER"

# Asignar rol
az role assignment create \
  --assignee-object-id $PRINCIPAL_ID \
  --assignee-principal-type ServicePrincipal \
  --role "Storage Blob Data Contributor" \
  --scope $SCOPE
```

## Paso 4 — Metastore de Unity Catalog

Si tu workspace ya tiene Unity Catalog habilitado (típico para workspaces creados en los últimos 12-18 meses), salta este paso.

Para verificar, entra al workspace y ve a **Catalog**. Si ves una jerarquía de catálogos, está habilitado.

Si no:
1. Ir a `accounts.azuredatabricks.net` como Account Admin
2. **Data** → **Create metastore**
3. Seleccionar la región del workspace
4. Proveer el connector del paso 2 como storage credential inicial
5. Asignar el metastore al workspace

Referencia: [Tutorial: Unity Catalog metastore admin tasks](https://learn.microsoft.com/en-us/azure/databricks/getting-started/uc-metastore-admin-quickstart).

## Paso 5 — Storage credential y external location

Esto se hace **dentro del workspace de Databricks** (no en Azure).

### Storage Credential

Catalog Explorer → **External Data** → **Credentials** → **Create credential**:
- **Credential type**: Azure Managed Identity
- **Name**: `nyctaxi_credential`
- **Access connector ID**: pega el resource ID del paso 2

O vía SQL:
```sql
CREATE STORAGE CREDENTIAL IF NOT EXISTS nyctaxi_credential
  WITH AZURE_MANAGED_IDENTITY
  '/subscriptions/<sub-id>/resourceGroups/<rg>/providers/Microsoft.Databricks/accessConnectors/<connector-name>'
  COMMENT 'Credential para el lakehouse de NYC Taxi';
```

### External Location

Catalog Explorer → **External Data** → **External Locations** → **Create external location**:
- **Name**: `nyctaxi_lake`
- **Storage credential**: `nyctaxi_credential`
- **URL**: `abfss://nyctaxi-lake@upbbigdatastorage.dfs.core.windows.net/`

O vía SQL:
```sql
CREATE EXTERNAL LOCATION IF NOT EXISTS nyctaxi_lake
  URL 'abfss://nyctaxi-lake@upbbigdatastorage.dfs.core.windows.net/'
  WITH (STORAGE CREDENTIAL nyctaxi_credential)
  COMMENT 'Container ADLS Gen2 para el lakehouse de NYC Taxi';
```

Validar que funciona:
```sql
-- Debe listar (o devolver vacío sin error) el contenido del container
LIST 'abfss://nyctaxi-lake@upbbigdatastorage.dfs.core.windows.net/';
```

## Paso 6 — Grupo de estudiantes y permisos

### Crear el grupo en el account console

1. `accounts.azuredatabricks.net` → **User management** → **Groups** → **Add group**
2. Nombre: `bigdata-students`
3. Agregar los usuarios de los estudiantes
4. Asignar el grupo al workspace: **Workspaces** → seleccionar tu workspace → **Permissions** → agregar el grupo con acceso

### Permisos del grupo sobre el metastore

Como cada estudiante va a crear SU propio catálogo, necesitan privilegio `CREATE CATALOG` a nivel de metastore.

En el workspace, abre un notebook SQL y ejecuta:

```sql
-- Ver el metastore actual
SELECT current_metastore();

-- Permitir a los estudiantes crear catálogos
GRANT CREATE CATALOG ON METASTORE TO `bigdata-students`;

-- Permitirles usar la external location
GRANT READ FILES, WRITE FILES, CREATE EXTERNAL TABLE, CREATE MANAGED STORAGE
  ON EXTERNAL LOCATION nyctaxi_lake
  TO `bigdata-students`;

-- Permitirles usar la storage credential
GRANT READ FILES, WRITE FILES ON STORAGE CREDENTIAL nyctaxi_credential
  TO `bigdata-students`;

-- Verificar
SHOW GRANTS ON METASTORE;
SHOW GRANTS ON EXTERNAL LOCATION nyctaxi_lake;
```

## Paso 7 — Compute (cluster o SQL Warehouse)

Los estudiantes necesitan algo sobre lo cual correr sus notebooks.

### Opción A — Cluster personal (recomendado)

Crear una **Cluster Policy** compartida que limite el tamaño y auto-termine:
- **Policy family**: Personal Compute
- **Max workers**: 2
- **Auto-termination**: 30 min
- **Runtime**: 15.4 LTS o superior (con Photon)
- **Access mode**: Single User

Permitir al grupo usarla:
```sql
-- Desde Databricks CLI o UI
databricks cluster-policies create --json-file policy.json
databricks permissions update cluster-policies <policy-id> \
  --json '{"access_control_list":[{"group_name":"bigdata-students","permission_level":"CAN_USE"}]}'
```

Los estudiantes crean su cluster personal desde esta policy — no pueden saltarse los límites.

### Opción B — SQL Warehouse compartido

Si quieres que todos compartan compute (más barato pero hay colas):
1. **SQL** → **SQL Warehouses** → **Create**
2. **Cluster size**: X-Small o Small
3. **Auto-stop**: 10 min
4. **Permisos**: CAN USE para `bigdata-students`

Para el dashboard final necesitas esto sí o sí.

## Paso 8 — Git y workspace folders

Recomiendo que cada estudiante clone este repo como un **Databricks Repo** en su home:
1. **Workspace** → su carpeta home → **Add** → **Git folder**
2. Pegar la URL del repo
3. Se crea `/Users/<estudiante>/big-data/modules/06-end-to-end-nyc-taxi/...`

Cada estudiante trabaja en su propia copia, edita su `config.py` sin pisar al resto.

## Checklist final de validación

Antes de liberar a los estudiantes, prueba con una cuenta de prueba que esté en `bigdata-students`:

- [ ] Puede hacer login al workspace
- [ ] Puede ver la external location `nyctaxi_lake` en Catalog Explorer
- [ ] Puede crear un cluster personal desde la policy
- [ ] Puede correr `01_create_catalog.py` sin errores (cambiando las iniciales a algo único)
- [ ] Después de correr el notebook, ve su propio catálogo en Catalog Explorer
- [ ] Puede descargar los archivos de la TLC al volume (`02_download_raw_files.py`)
- [ ] Puede correr todo el pipeline hasta Gold

Si los 7 checks pasan, la clase está lista.

## Costos estimados (referencia)

Por semestre, para un grupo de 20 estudiantes haciendo el pipeline completo:

- **Storage ADLS Gen2**: ~2-5 USD/mes (20 catálogos × ~5 GB cada uno)
- **Compute**: depende del uso. Clusters personales Standard_DS3_v2 con 2 workers salen a ~0.80 USD/hora/estudiante. Con auto-stop de 30 min y uso de ~4h/semana por estudiante → ~60-80 USD/mes para el grupo.
- **SQL Warehouse X-Small**: ~0.38 DBU/hora. Con auto-stop agresivo → ~20-40 USD/mes.

Total aproximado: **100-150 USD/mes** para el curso completo.

Para ahorrar: usar **serverless SQL warehouses** para el dashboard (pago por segundo en vez de por hora), y pedir a los estudiantes que terminen sus clusters al acabar.

## Troubleshooting común

**`PERMISSION_DENIED: User does not have CREATE CATALOG on METASTORE`**
→ Paso 6: falta el `GRANT CREATE CATALOG ON METASTORE`.

**`[RequestId=... ErrorClass=PERMISSION_DENIED] User does not have WRITE FILES on external location`**
→ Paso 6: falta el `GRANT ... ON EXTERNAL LOCATION`.

**`AbfsRestOperationException: 403 AuthorizationPermissionMismatch`**
→ Paso 3: el connector no tiene `Storage Blob Data Contributor` sobre el container, o se lo pusiste al resource group en vez de al container.

**`Catalog 'casm_nytaxi' already exists`**
→ Dos estudiantes usaron las mismas iniciales. Pedirle a uno que cambie.

**Auto Loader falla leyendo el volume**
→ Verificar que el cluster use **Runtime 13.3 LTS o superior** y **access mode Single User o Shared** (no No Isolation Shared, que no soporta UC).

**El notebook tarda mucho en la primera corrida**
→ Normal: el cluster tarda ~5 min en arrancar. Con serverless es instantáneo pero cuesta más.
