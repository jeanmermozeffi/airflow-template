#!/usr/bin/env bash

# ═════════════════════════════════════════════════════════════════════════════
# Airflow Docker Compose Manager
# ═════════════════════════════════════════════════════════════════════════════
#
# Usage:
#   ./scripts/airflow_runner.sh up          # Démarrer Airflow
#   ./scripts/airflow_runner.sh down        # Arrêter Airflow
#   ./scripts/airflow_runner.sh clean       # Nettoyer et redémarrer
#   ./scripts/airflow_runner.sh restart     # Redémarrer sans nettoyer
#   ./scripts/airflow_runner.sh logs        # Voir les logs de connexion
#   ./scripts/airflow_runner.sh logs-init   # Voir les logs d'initialisation
#   ./scripts/airflow_runner.sh logs-sched  # Voir les logs du scheduler
#   ./scripts/airflow_runner.sh status      # Afficher le statut des conteneurs
#   ./scripts/airflow_runner.sh info        # Afficher les infos de connexion
#   ./scripts/airflow_runner.sh help        # Afficher l'aide
#
# ═════════════════════════════════════════════════════════════════════════════

set -e

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────────────────────

COMPOSE_FILE="${AIRFLOW_COMPOSE_FILE:-deployment/docker-compose.yml}"
ENV_FILE="${AIRFLOW_ENV_FILE:-.env.dev}"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# ─────────────────────────────────────────────────────────────────────────────
# FONCTIONS UTILITAIRES
# ─────────────────────────────────────────────────────────────────────────────

error() {
    echo -e "${RED}ERREUR: $1${NC}" >&2
    exit 1
}

success() {
    echo -e "${GREEN}$1${NC}"
}

info() {
    echo -e "${BLUE}$1${NC}"
}

warning() {
    echo -e "${YELLOW}$1${NC}"
}

check_docker() {
    if ! command -v docker &> /dev/null; then
        error "Docker n'est pas installé."
    fi
    if ! command -v docker-compose &> /dev/null; then
        if ! docker compose version &> /dev/null; then
            error "docker-compose n'est pas disponible"
        fi
    fi
}

check_files() {
    if [ ! -f "$COMPOSE_FILE" ]; then
        error "Fichier $COMPOSE_FILE non trouvé"
    fi
    if [ ! -f "$ENV_FILE" ]; then
        error "Fichier $ENV_FILE non trouvé"
    fi
}

# ─────────────────────────────────────────────────────────────────────────────
# COMMANDES PRINCIPALES
# ─────────────────────────────────────────────────────────────────────────────

cmd_up() {
    info "Démarrage d'Airflow..."
    check_docker
    check_files
    docker compose -f "$COMPOSE_FILE" --env-file "$ENV_FILE" up -d
    success "Airflow est en train de démarrer"
    echo ""
    info "Attends ~45 secondes pour que tout soit prêt"
    info "Airflow UI: http://localhost:8080"
}

cmd_down() {
    info "Arrêt d'Airflow..."
    check_docker
    check_files
    docker compose -f "$COMPOSE_FILE" down
    success "Airflow arrêté"
}

cmd_clean() {
    info "Nettoyage complet (suppression de tous les volumes)..."
    check_docker
    check_files
    docker compose -f "$COMPOSE_FILE" down -v --remove-orphans
    success "Nettoyage terminé"
    echo ""
    info "Redémarrage d'Airflow..."
    docker compose -f "$COMPOSE_FILE" --env-file "$ENV_FILE" up -d
    success "Airflow redémarré avec une base vierge"
    echo ""
    info "Attends ~45 secondes pour que tout soit prêt"
    info "Airflow UI: http://localhost:8080"
}

cmd_restart() {
    info "Redémarrage d'Airflow (sans nettoyage)..."
    check_docker
    check_files
    docker compose -f "$COMPOSE_FILE" down
    docker compose -f "$COMPOSE_FILE" --env-file "$ENV_FILE" up -d
    success "Airflow redémarré"
}

cmd_logs() {
    check_docker
    info "Logs d'initialisation des connexions:"
    docker logs airflow-init-connections
}

cmd_logs_init() {
    check_docker
    info "Logs d'initialisation de la base de données:"
    docker logs airflow-init
}

cmd_logs_scheduler() {
    check_docker
    info "Logs du scheduler (Ctrl+C pour arrêter):"
    docker logs airflow-scheduler -f
}

cmd_status() {
    check_docker
    check_files
    echo ""
    info "Statut des conteneurs Airflow:"
    docker compose -f "$COMPOSE_FILE" ps
}

cmd_info() {
    echo ""
    echo "╔══════════════════════════════════════════════════════════════╗"
    echo "║            Informations de connexion - Airflow               ║"
    echo "╚══════════════════════════════════════════════════════════════╝"
    echo ""
    echo -e "${GREEN}Airflow UI${NC}"
    echo "   URL: http://localhost:8080"
    echo "   Utilisateur: ${AIRFLOW_SIMPLE_AUTH_USERNAME:-admin}"
    echo "   Mot de passe: (voir .env.dev)"
    echo ""
    echo -e "${GREEN}PostgreSQL Airflow metadata (Docker)${NC}"
    echo "   Host: localhost:5433"
    echo "   Commande: docker exec -it airflow-postgres psql -U airflow -d airflow"
    echo ""
}

cmd_help() {
    cat << 'EOF'
╔══════════════════════════════════════════════════════════════════════════╗
║              Airflow Docker Compose - Gestionnaire                       ║
╚══════════════════════════════════════════════════════════════════════════╝

COMMANDES DISPONIBLES:

  airflow_runner.sh up              Démarrer Airflow
  airflow_runner.sh down            Arrêter Airflow
  airflow_runner.sh clean           Nettoyer tout (volumes) et redémarrer
  airflow_runner.sh restart         Redémarrer sans nettoyer les volumes
  airflow_runner.sh logs            Afficher les logs de connexion Airflow
  airflow_runner.sh logs-init       Afficher les logs d'initialisation BD
  airflow_runner.sh logs-sched      Afficher les logs du scheduler (temps réel)
  airflow_runner.sh status          Afficher le statut des conteneurs
  airflow_runner.sh info            Afficher les infos de connexion
  airflow_runner.sh help            Afficher cette aide

VARIABLES D'ENVIRONNEMENT:

  AIRFLOW_COMPOSE_FILE   Fichier compose à utiliser (défaut: deployment/docker-compose.yml)
  AIRFLOW_ENV_FILE       Fichier .env à utiliser (défaut: .env.dev)

FLUX TYPIQUE:

  1. Démarrer:        ./scripts/airflow_runner.sh up
  2. Attendre 45s
  3. Vérifier:        ./scripts/airflow_runner.sh status
  4. Consulter logs:  ./scripts/airflow_runner.sh logs
  5. Accéder UI:      http://localhost:8080

EOF
}

# ─────────────────────────────────────────────────────────────────────────────
# POINT D'ENTRÉE
# ─────────────────────────────────────────────────────────────────────────────

main() {
    if [ $# -eq 0 ]; then
        cmd_help
        exit 0
    fi

    case "$1" in
        up)             cmd_up ;;
        down)           cmd_down ;;
        clean)          cmd_clean ;;
        restart)        cmd_restart ;;
        logs)           cmd_logs ;;
        logs-init)      cmd_logs_init ;;
        logs-sched|logs-scheduler) cmd_logs_scheduler ;;
        status)         cmd_status ;;
        info)           cmd_info ;;
        help|-h|--help) cmd_help ;;
        *)              error "Commande inconnue: $1\n\nUtilise: $0 help" ;;
    esac
}

main "$@"
