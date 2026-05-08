#!/usr/bin/env bash
# =============================================================================
# airflow_init.sh — Initialisation du cluster Airflow au premier démarrage
#
# Appelé par le service airflow-init (user 0:0) dans docker-compose.
# Lance uniquement des opérations idempotentes : peut être rejoué sans risque.
#
# Ordre d'exécution :
#   1. Répertoires de logs + permissions
#   2. Hardening PostgreSQL (rôles, grants)
#   3. airflow db migrate
#   4. Création / mise à jour de l'utilisateur UI Airflow
#   5. Seed des connexions et variables (bootstrap_airflow.py)
# =============================================================================
set -euo pipefail

# L'airflow CLI (shebang #!/usr/python/bin/python3.11) ne voit pas les packages user.
# On passe par le Python du user airflow qui a airflow dans son sys.path.
export PATH="/home/airflow/.local/bin:${PATH}"
PYTHON="/home/airflow/.local/bin/python3"
AIRFLOW="${PYTHON} -m airflow"

SCRIPTS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# ── 1. Répertoires de logs ────────────────────────────────────────────────────
echo "[init] Création des répertoires de logs..."
mkdir -p \
  /opt/airflow/logs \
  /opt/airflow/logs/dag_processor \
  /opt/airflow/logs/scheduler \
  /opt/airflow/logs/runtime-sync/config
chmod -R 750 /opt/airflow/logs
chown -R "${AIRFLOW_UID:-50000}:0" /opt/airflow/logs
echo "[init] Répertoires OK."

# ── 2. Hardening PostgreSQL ───────────────────────────────────────────────────
echo "[init] Hardening PostgreSQL..."
"${PYTHON}" "${SCRIPTS_DIR}/airflow_db_hardening.py"

# ── 3. Migration de la base de métadonnées Airflow ───────────────────────────
echo "[init] airflow db migrate..."
${AIRFLOW} db migrate

# ── 4. Utilisateur UI Airflow ─────────────────────────────────────────────────
AUTH_USER="${AIRFLOW_SIMPLE_AUTH_USERNAME:-admin}"
AUTH_PASS="${AIRFLOW_SIMPLE_AUTH_PASSWORD:-admin}"
echo "[init] Mise à jour de l'utilisateur UI '${AUTH_USER}'..."
${AIRFLOW} users delete --username "${AUTH_USER}" 2>/dev/null || true
${AIRFLOW} users create \
  --username "${AUTH_USER}" \
  --password "${AUTH_PASS}" \
  --firstname "${AIRFLOW_UI_FIRSTNAME:-Admin}" \
  --lastname  "${AIRFLOW_UI_LASTNAME:-User}" \
  --role      "Admin" \
  --email     "${AIRFLOW_UI_EMAIL:-admin@airflow.local}"

# ── 5. Connexions et variables Airflow ───────────────────────────────────────
echo "[init] Seed connexions et variables (bootstrap_airflow.py)..."
"${PYTHON}" "${SCRIPTS_DIR}/bootstrap_airflow.py"

echo "[init] Initialisation terminée avec succès."
