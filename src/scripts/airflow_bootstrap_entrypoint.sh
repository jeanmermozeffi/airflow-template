#!/usr/bin/env bash
set -euo pipefail

ORIGINAL_ENTRYPOINT="${AIRFLOW_ORIGINAL_ENTRYPOINT:-/entrypoint}"
BOOTSTRAP_SCRIPT="${AIRFLOW_BOOTSTRAP_SCRIPT:-/opt/airflow/scripts/bootstrap_airflow.py}"
BOOTSTRAP_ON_START="${AIRFLOW_BOOTSTRAP_ON_START:-true}"
WAIT_FOR_MIGRATIONS="${AIRFLOW_BOOTSTRAP_WAIT_FOR_MIGRATIONS:-true}"

is_enabled() {
  case "${1,,}" in
    1|true|yes|y|on) return 0 ;;
    *) return 1 ;;
  esac
}

is_airflow_runtime_subcommand() {
  case "${1:-}" in
    api-server|scheduler|dag-processor|triggerer|worker|webserver|celery)
      return 0
      ;;
  esac

  return 1
}

is_runtime_command() {
  if [[ $# -eq 0 ]]; then
    return 0
  fi

  if is_airflow_runtime_subcommand "$1"; then
    return 0
  fi

  if [[ "$1" == "airflow" || "$1" == */airflow ]] && is_airflow_runtime_subcommand "${2:-}"; then
    return 0
  fi

  if [[ "$1" == "bash" || "$1" == "sh" || "$1" == "/bin/bash" || "$1" == "/bin/sh" ]] && [[ "${2:-}" == "-c" ]]; then
    case "${3:-}" in
      *"airflow api-server"*|*"airflow scheduler"*|*"airflow dag-processor"*|*"airflow triggerer"*|*"airflow worker"*|*"airflow webserver"*|*"airflow celery"*|api-server*|scheduler*|dag-processor*|triggerer*|worker*|webserver*|celery*)
        return 0
        ;;
    esac
  fi

  return 1
}

run_bootstrap() {
  if is_enabled "${WAIT_FOR_MIGRATIONS}"; then
    if airflow db --help 2>&1 | grep -q "check-migrations"; then
      airflow db check-migrations
    else
      airflow db check
    fi
  fi

  python "${BOOTSTRAP_SCRIPT}"
}

if is_enabled "${BOOTSTRAP_ON_START}" && is_runtime_command "$@"; then
  echo "[airflow-bootstrap] Synchronisation des connexions et variables Airflow..."
  run_bootstrap
fi

if [[ -x "${ORIGINAL_ENTRYPOINT}" ]]; then
  exec "${ORIGINAL_ENTRYPOINT}" "$@"
fi

exec "$@"
