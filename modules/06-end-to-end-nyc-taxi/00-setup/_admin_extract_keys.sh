#!/usr/bin/env bash
# ============================================================================
# ADMIN ONLY — Extrae las access keys de los storage accounts de estudiantes
# y las deja en .tmp/student-keys.txt (NO commitear ese archivo)
# ============================================================================
#
# Uso:
#   1. Iniciar sesión en Azure:       az login
#   2. Setear suscripción correcta:   az account set --subscription RECURSOS_MED
#   3. Correr:                        ./_admin_extract_keys.sh
#
# Output: .tmp/student-keys.txt en la raíz del proyecto
# Formato: iniciales|storage_account|access_key
# ============================================================================

set -euo pipefail

RG="202610-big-data-analytics"
REPO_ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
OUTDIR="${REPO_ROOT}/.tmp"
OUTFILE="${OUTDIR}/student-keys.txt"

STUDENT_ACCOUNTS=(
  dl25604arbelaezm
  dl25604ariasbernal
  dl25604buitragoa
  dl25604gerena
  dl25604gomezgallego
  dl25604molina
  dl25604monsalvev
  dl25604ortegap
  dl25604ospinag
  dl25604otalvaro
  dl25604riosp
  dl25604romeror
  dl25604soto
  dl25604vasco
)

mkdir -p "$OUTDIR"

{
  echo "# Access keys for NYC Taxi course - NOT TO BE COMMITTED"
  echo "# Generated: $(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  echo "# Format: iniciales|storage_account|access_key"
  echo ""
} > "$OUTFILE"

echo "Extracting keys into $OUTFILE ..."
for account in "${STUDENT_ACCOUNTS[@]}"; do
  initials="${account#dl25604}"
  key=$(az storage account keys list \
    --account-name "$account" \
    --resource-group "$RG" \
    --query "[0].value" \
    -o tsv 2>/dev/null || true)
  if [[ -n "$key" ]]; then
    echo "${initials}|${account}|${key}" >> "$OUTFILE"
    echo "  ✓ $initials"
  else
    echo "  ✗ $initials (skipped)"
  fi
done

echo ""
echo "Done. File: $OUTFILE"
echo ""
echo "Next step: load into Databricks secret scope with _admin_load_secrets.sh"
