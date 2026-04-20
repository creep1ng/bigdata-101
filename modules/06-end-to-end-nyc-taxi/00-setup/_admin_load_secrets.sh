#!/usr/bin/env bash
# ============================================================================
# ADMIN ONLY — Carga las access keys al Databricks secret scope
# ============================================================================
#
# Prerrequisitos:
#   1. Haber corrido _admin_extract_keys.sh (debe existir .tmp/student-keys.txt)
#   2. Databricks CLI instalado y configurado:
#      curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh
#      databricks configure --host https://adb-7405618790693465.5.azuredatabricks.net
# ============================================================================

set -euo pipefail

SCOPE="nytaxi-course"
STUDENT_GROUP="bigdata-students"
REPO_ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
KEYS_FILE="${REPO_ROOT}/.tmp/student-keys.txt"

if [[ ! -f "$KEYS_FILE" ]]; then
  echo "ERROR: $KEYS_FILE no existe. Corre _admin_extract_keys.sh primero."
  exit 1
fi

echo "== Secret scope: $SCOPE =="
if databricks secrets list-scopes 2>/dev/null | grep -q "^${SCOPE}\s"; then
  echo "  scope ya existe, reutilizando"
else
  echo "  creando scope..."
  databricks secrets create-scope "$SCOPE"
fi

echo ""
echo "== Cargando secrets =="
while IFS='|' read -r initials account key; do
  [[ -z "$initials" ]] && continue
  [[ "$initials" =~ ^#.*$ ]] && continue

  secret_key="adls-key-${initials}"
  echo "  ${secret_key} -> ${account}"
  databricks secrets put-secret "$SCOPE" "$secret_key" --string-value "$key"
done < "$KEYS_FILE"

echo ""
echo "== Permisos para $STUDENT_GROUP =="
databricks secrets put-acl "$SCOPE" "$STUDENT_GROUP" READ || {
  echo "  aviso: no se pudo asignar ACL al grupo (¿existe el grupo '$STUDENT_GROUP'?)"
}

echo ""
echo "== Verificación =="
databricks secrets list-secrets "$SCOPE"

echo ""
echo "Done."
