#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

PROJECT_NAME="${SMOKE_PROJECT_NAME:-velibdata-smoke}"
COMPOSE_FILE="${SMOKE_COMPOSE_FILE:-docker-compose.smoke.yml}"
MINIO_PORT="${SMOKE_MINIO_PORT:-19000}"
PROMETHEUS_PORT="${SMOKE_PROMETHEUS_PORT:-19090}"
GRAFANA_PORT="${SMOKE_GRAFANA_PORT:-13000}"
KAFKA_EXPORTER_PORT="${SMOKE_KAFKA_EXPORTER_PORT:-19308}"
KEEP_RUNNING="${SMOKE_KEEP_RUNNING:-false}"
TIMESTAMP="$(date -u +%Y%m%d-%H%M%S)"
mkdir -p evidence
REPORT="${SMOKE_REPORT:-evidence/ci-smoke-${TIMESTAMP}.txt}"
LOGS="evidence/ci-smoke-containers-${TIMESTAMP}.log"
COMPOSE=(docker compose --project-name "$PROJECT_NAME" --file "$COMPOSE_FILE")

exec > >(tee -a "$REPORT") 2>&1

log() {
  printf '[%s] %s\n' "$(date -u '+%Y-%m-%d %H:%M:%S UTC')" "$*"
}

fail() {
  log "FAIL - $*"
  return 1
}

wait_container_healthy() {
  local service="$1"
  local label="$2"
  local attempts="${3:-60}"
  local container_id status

  for ((i=1; i<=attempts; i++)); do
    container_id="$("${COMPOSE[@]}" ps -q "$service" 2>/dev/null || true)"
    if [[ -n "$container_id" ]]; then
      status="$(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "$container_id" 2>/dev/null || true)"
      if [[ "$status" == "healthy" || "$status" == "running" ]]; then
        log "PASS - $label est $status."
        return 0
      fi
    fi
    sleep 3
  done

  fail "$label n'est pas devenu sain dans le delai imparti."
}

wait_http() {
  local url="$1"
  local label="$2"
  local attempts="${3:-60}"

  for ((i=1; i<=attempts; i++)); do
    if curl --silent --show-error --fail --max-time 5 "$url" >/dev/null 2>&1; then
      log "PASS - $label repond sur $url"
      return 0
    fi
    sleep 3
  done

  fail "$label ne repond pas sur $url"
}

cleanup() {
  local exit_code=$?
  trap - EXIT

  if (( exit_code != 0 )); then
    log "Collecte des diagnostics apres echec."
    {
      echo "===== docker compose ps ====="
      "${COMPOSE[@]}" ps || true
      echo
      echo "===== docker compose logs ====="
      "${COMPOSE[@]}" logs --no-color || true
    } >"$LOGS" 2>&1
    log "Diagnostics : $ROOT_DIR/$LOGS"
  fi

  if [[ "${KEEP_RUNNING,,}" != "true" ]]; then
    log "Nettoyage de la stack de smoke test."
    "${COMPOSE[@]}" down --volumes --remove-orphans >/dev/null 2>&1 || true
  else
    log "Stack conservee car SMOKE_KEEP_RUNNING=true."
  fi

  if (( exit_code == 0 )); then
    log "RESULTAT GLOBAL : PASS"
  else
    log "RESULTAT GLOBAL : FAIL"
  fi
  log "Rapport : $ROOT_DIR/$REPORT"
  exit "$exit_code"
}
trap cleanup EXIT

log "SMOKE TEST VELIBDATA - debut"
log "Compose : $COMPOSE_FILE ; projet isole : $PROJECT_NAME"

[[ -f .env ]] || fail "Le fichier .env est absent. Creez-le a partir de .env.example."
"${COMPOSE[@]}" config --quiet
log "PASS - configuration Docker Compose valide."

"${COMPOSE[@]}" up -d kafka minio kafka-exporter prometheus grafana
wait_container_healthy kafka "Kafka"
wait_container_healthy minio "MinIO"

"${COMPOSE[@]}" run --rm minio-setup
log "PASS - initialisation des buckets terminee."

wait_http "http://127.0.0.1:${MINIO_PORT}/minio/health/ready" "MinIO"
wait_http "http://127.0.0.1:${PROMETHEUS_PORT}/-/ready" "Prometheus"
wait_http "http://127.0.0.1:${GRAFANA_PORT}/api/health" "Grafana"
wait_http "http://127.0.0.1:${KAFKA_EXPORTER_PORT}/metrics" "Kafka Exporter"

log "Verification lecture/ecriture des buckets MinIO."
"${COMPOSE[@]}" run --rm --no-deps --entrypoint /bin/sh minio-setup -c '
  set -eu
  mc alias set local http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null
  for bucket in raw clean curated; do
    mc ls "local/$bucket" >/dev/null
  done
  probe="velibdata-smoke-$(date +%s)"
  printf "%s" "$probe" >/tmp/probe.txt
  mc cp /tmp/probe.txt local/raw/ci/smoke-probe.txt >/dev/null
  actual="$(mc cat local/raw/ci/smoke-probe.txt)"
  test "$actual" = "$probe"
'
log "PASS - buckets RAW/CLEAN/CURATED accessibles et objet MinIO relu correctement."

TOPIC="velib.ci.smoke.${TIMESTAMP}"
MESSAGE="velibdata-smoke-${TIMESTAMP}"
log "Verification Kafka sur le topic $TOPIC."
"${COMPOSE[@]}" exec -T kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:9092 \
  --create --if-not-exists \
  --topic "$TOPIC" --partitions 1 --replication-factor 1 >/dev/null

"${COMPOSE[@]}" exec -T kafka /bin/bash -lc \
  "printf '%s\\n' '$MESSAGE' | /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka:9092 --topic '$TOPIC'"

CONSUMED="$("${COMPOSE[@]}" exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 \
  --topic "$TOPIC" \
  --from-beginning \
  --max-messages 1 \
  --timeout-ms 15000 2>/dev/null | tr -d '\r' | tail -n 1)"

[[ "$CONSUMED" == "$MESSAGE" ]] || fail "Message Kafka attendu '$MESSAGE', obtenu '$CONSUMED'."
log "PASS - message Kafka produit puis consomme : $MESSAGE"

EXPORTER_METRICS=""
for ((i=1; i<=30; i++)); do
  EXPORTER_METRICS="$(curl --silent --show-error --fail "http://127.0.0.1:${KAFKA_EXPORTER_PORT}/metrics" 2>/dev/null || true)"
  if grep -q '^kafka_brokers ' <<<"$EXPORTER_METRICS"; then
    break
  fi
  sleep 2
done
grep -q '^kafka_brokers ' <<<"$EXPORTER_METRICS" || fail "La metrique kafka_brokers est absente."
log "PASS - kafka-exporter expose la metrique kafka_brokers."

PROM_STATUS="$(curl --silent --show-error --fail "http://127.0.0.1:${PROMETHEUS_PORT}/api/v1/status/config")"
grep -q '"status":"success"' <<<"$PROM_STATUS" || fail "Prometheus ne valide pas sa configuration."
log "PASS - configuration Prometheus chargee."

GRAFANA_HEALTH="$(curl --silent --show-error --fail "http://127.0.0.1:${GRAFANA_PORT}/api/health")"
grep -q '"database"' <<<"$GRAFANA_HEALTH" || fail "Reponse de sante Grafana inattendue."
log "PASS - Grafana et sa base interne sont operationnels."

log "Etat final des services :"
"${COMPOSE[@]}" ps
