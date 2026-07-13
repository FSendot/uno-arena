#!/usr/bin/env bash
# EXPLICIT/NETWORKED: run the real Analytics projection-rebuilder process against
# Kafka -> context-owned Room backfill HTTP -> an ephemeral ClickHouse database.
# Kafka topics/groups, Pod, grants, and database are removed on exit.
# The chart deployment remains disabled; this harness creates one temporary Pod.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "${SCRIPT_DIR}/lib.sh"

require_cmd kubectl
require_cmd jq
assert_kind_context

TIMEOUT_S="${KIND_ANALYTICS_REBUILDER_LIVE_TIMEOUT_S:-120}"
SUFFIX="$(od -An -N6 -tx1 /dev/urandom | tr -d ' \n')"
POD="analytics-rebuilder-live-${SUFFIX}"
DB="unoarena_analytics_rebuild_live_${SUFFIX}"
JOB_ID="analytics-rebuild-${SUFFIX}"
GROUP="analytics-rebuilder-live-${SUFFIX}"
REQUEST_TOPIC="analytics.rebuild.live.${SUFFIX}"
REQUEST_DLQ="analytics.rebuild.live.${SUFFIX}.dlq"
POD_CREATED=0
DB_CREATED=0
TOPICS_CREATED=0

kafka() {
  kubectl -n "${KIND_NAMESPACE}" exec deploy/kafka -- /opt/kafka/bin/"$@"
}

ch_admin() {
  kubectl -n "${KIND_NAMESPACE}" exec -i deploy/clickhouse -- \
    clickhouse-client --user clickhouse_admin --password local-only-ch-admin --multiquery "$@"
}

ch_runtime() {
  kubectl -n "${KIND_NAMESPACE}" exec -i deploy/clickhouse -- \
    clickhouse-client --user analytics_runtime --password local-only-analytics-runtime \
      --database "${DB}" "$@"
}

cleanup() {
  local rc=$?
  set +e
  if [[ "${POD_CREATED}" -eq 1 ]]; then
    kubectl -n "${KIND_NAMESPACE}" delete pod "${POD}" --ignore-not-found --wait=false >/dev/null
  fi
  if [[ "${TOPICS_CREATED}" -eq 1 ]]; then
    for topic in "${REQUEST_TOPIC}" "${REQUEST_DLQ}"; do
      kafka kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic "${topic}" >/dev/null 2>&1
    done
    kafka kafka-consumer-groups.sh --bootstrap-server localhost:9092 --delete \
      --group "${GROUP}" >/dev/null 2>&1
  fi
  if [[ "${DB_CREATED}" -eq 1 ]]; then
    ch_admin --query "REVOKE ALL ON ${DB}.* FROM analytics_runtime" >/dev/null
    ch_admin --query "DROP DATABASE IF EXISTS ${DB}" >/dev/null
  fi
  return "${rc}"
}
trap cleanup EXIT

[[ "${DB}" =~ ^unoarena_analytics_rebuild_live_[a-f0-9]{12}$ ]] \
  || die "unsafe generated ClickHouse database name: ${DB}"
for deploy in kafka clickhouse analytics room-gameplay tournament-orchestration ranking; do
  kubectl -n "${KIND_NAMESPACE}" get "deploy/${deploy}" >/dev/null 2>&1 \
    || die "deploy/${deploy} not found; deploy the complete kind stack first"
done
secret_json="$(kubectl -n "${KIND_NAMESPACE}" get secret uno-arena-local-credentials -o json)"
for key in analytics-runtime-user analytics-runtime-password analytics-room-credential \
  analytics-tournament-credential analytics-ranking-credential; do
  [[ "$(jq -r --arg key "${key}" '.data[$key] // empty' <<<"${secret_json}")" != "" ]] \
    || die "uno-arena-local-credentials is missing ${key}; reapply the local secret manifest"
done

TOPICS_CREATED=1
for topic in "${REQUEST_TOPIC}" "${REQUEST_DLQ}"; do
  kafka kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists \
    --topic "${topic}" --partitions 1 --replication-factor 1 >/dev/null
done

tables=(active_generation gameplay_metrics ingestion_conflicts processed_events projection_generations \
  rating_statistics recovery_jobs recovery_leases recovery_page_checkpoints \
  recovery_request_idempotency schema_migrations tournament_statistics)
ch_admin --query "CREATE DATABASE ${DB}" >/dev/null
DB_CREATED=1
sql=""
for table in "${tables[@]}"; do
  sql+=" CREATE TABLE ${DB}.${table} AS analytics.${table};"
done
sql+=" INSERT INTO ${DB}.schema_migrations (version) VALUES ('001_init');"
sql+=" GRANT SELECT, INSERT ON ${DB}.* TO analytics_runtime;"
ch_admin --query "${sql}" >/dev/null

POD_CREATED=1
kubectl -n "${KIND_NAMESPACE}" apply -f - >/dev/null <<YAML
apiVersion: v1
kind: Pod
metadata:
  name: ${POD}
  labels:
    app: analytics-projection-rebuilder-live-test
    uno-arena.local/live-test: "true"
spec:
  restartPolicy: Never
  terminationGracePeriodSeconds: 5
  containers:
    - name: projection-rebuilder
      image: uno-arena/analytics:local
      imagePullPolicy: IfNotPresent
      env:
        - {name: WORKER_ROLE, value: analytics-projection-rebuilder}
        - {name: CLICKHOUSE_URL, value: http://clickhouse.uno-arena.svc.cluster.local:8123}
        - {name: CLICKHOUSE_DB, value: "${DB}"}
        - {name: KAFKA_BROKERS, value: kafka.uno-arena.svc.cluster.local:9092}
        - {name: KAFKA_PROJECTION_REBUILD_GROUP, value: "${GROUP}"}
        - {name: KAFKA_PROJECTION_REBUILD_TOPIC, value: "${REQUEST_TOPIC}"}
        - {name: KAFKA_PROJECTION_REBUILD_DLQ_TOPIC, value: "${REQUEST_DLQ}"}
        - {name: ROOM_GAMEPLAY_URL, value: http://room-gameplay.uno-arena.svc.cluster.local:8080}
        - {name: TOURNAMENT_ORCHESTRATION_URL, value: http://tournament-orchestration.uno-arena.svc.cluster.local:8080}
        - {name: RANKING_URL, value: http://ranking.uno-arena.svc.cluster.local:8080}
        - name: CLICKHOUSE_USER
          valueFrom: {secretKeyRef: {name: uno-arena-local-credentials, key: analytics-runtime-user}}
        - name: CLICKHOUSE_PASSWORD
          valueFrom: {secretKeyRef: {name: uno-arena-local-credentials, key: analytics-runtime-password}}
        - name: ANALYTICS_ROOM_CREDENTIAL
          valueFrom: {secretKeyRef: {name: uno-arena-local-credentials, key: analytics-room-credential}}
        - name: ANALYTICS_TOURNAMENT_CREDENTIAL
          valueFrom: {secretKeyRef: {name: uno-arena-local-credentials, key: analytics-tournament-credential}}
        - name: ANALYTICS_RANKING_CREDENTIAL
          valueFrom: {secretKeyRef: {name: uno-arena-local-credentials, key: analytics-ranking-credential}}
YAML

kubectl -n "${KIND_NAMESPACE}" wait --for=jsonpath='{.status.phase}'=Running "pod/${POD}" --timeout=60s >/dev/null \
  || { kubectl -n "${KIND_NAMESPACE}" describe "pod/${POD}"; die "rebuilder Pod did not start"; }
deadline=$((SECONDS + 60))
until kubectl -n "${KIND_NAMESPACE}" logs "${POD}" 2>/dev/null | grep -q projection_rebuilder_startup; do
  (( SECONDS < deadline )) || { kubectl -n "${KIND_NAMESPACE}" logs "${POD}" >&2; die "rebuilder did not report startup"; }
  sleep 1
done

# A far-future bounded range deliberately yields one empty terminal page from the
# real Room backfill API. That still exercises job registration, clone, lease,
# page checkpoint, activation, idempotency, and Kafka offset commit.
payload="$(jq -cn --arg eid "analytics-rebuild-event-${SUFFIX}" --arg corr "corr-${SUFFIX}" \
  --arg job "${JOB_ID}" --arg at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  '{eventId:$eid,eventType:"AnalyticsProjectionRebuildRequested",schemaVersion:1,correlationId:$corr,occurredAt:$at,recoveryJobId:$job,sourceContext:"room",expectedSourceTopic:"room.gameplay.metrics",fromOccurredAt:"2099-01-01T00:00:00Z",toOccurredAt:"2099-01-01T00:00:15Z"}')"
produce() {
  printf '%s\t%s\n' "${JOB_ID}" "${payload}" | kubectl -n "${KIND_NAMESPACE}" exec -i deploy/kafka -- \
    /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 \
      --topic "${REQUEST_TOPIC}" --property parse.key=true --property key.separator=$'\t' >/dev/null
}

produce
deadline=$((SECONDS + TIMEOUT_S))
status=""
while (( SECONDS < deadline )); do
  status="$(ch_runtime --query "SELECT status FROM recovery_jobs FINAL WHERE recovery_job_id='${JOB_ID}'" 2>/dev/null | tr -d '[:space:]')"
  [[ "${status}" == "complete" ]] && break
  if ! kubectl -n "${KIND_NAMESPACE}" get "pod/${POD}" -o jsonpath='{.status.phase}' 2>/dev/null | grep -q Running; then
    kubectl -n "${KIND_NAMESPACE}" logs "${POD}" >&2
    die "rebuilder Pod stopped before recovery completed"
  fi
  sleep 2
done
[[ "${status}" == "complete" ]] \
  || { kubectl -n "${KIND_NAMESPACE}" logs "${POD}" >&2; die "Analytics recovery did not complete"; }

job_row="$(ch_runtime --format TSVRaw --query "SELECT generation_id,pages_completed,accepted_count FROM recovery_jobs FINAL WHERE recovery_job_id='${JOB_ID}'")"
IFS=$'\t' read -r generation pages accepted <<<"${job_row}"
[[ -n "${generation}" && "${pages}" == "1" && "${accepted}" == "0" ]] \
  || die "unexpected recovery job row: ${job_row}"
active="$(ch_runtime --query 'SELECT generation_id FROM active_generation FINAL WHERE singleton=1' | tr -d '[:space:]')"
[[ "${active}" == "${generation}" ]] || die "recovery generation was not activated"
idemp_count="$(ch_runtime --query "SELECT count() FROM recovery_request_idempotency FINAL WHERE recovery_job_id='${JOB_ID}' AND disposition='finalized'")"
[[ "${idemp_count//[[:space:]]/}" == "1" ]] || die "finalized idempotency marker missing"
lease_epoch="$(ch_runtime --query "SELECT lease_epoch FROM recovery_leases FINAL WHERE recovery_job_id='${JOB_ID}'" | tr -d '[:space:]')"
[[ "${lease_epoch}" =~ ^[1-9][0-9]*$ ]] || die "durable lease/fence row missing"

# Exact redelivery must short-circuit on the durable request marker: page/job and
# lease epoch stay unchanged, proving idempotency without another activation.
produce
sleep 5
job_row_after="$(ch_runtime --format TSVRaw --query "SELECT generation_id,pages_completed,accepted_count FROM recovery_jobs FINAL WHERE recovery_job_id='${JOB_ID}'")"
lease_epoch_after="$(ch_runtime --query "SELECT lease_epoch FROM recovery_leases FINAL WHERE recovery_job_id='${JOB_ID}'" | tr -d '[:space:]')"
[[ "${job_row_after}" == "${job_row}" && "${lease_epoch_after}" == "${lease_epoch}" ]] \
  || die "idempotent redelivery mutated job or lease fence"

echo "ok kind-test-analytics-projection-rebuilder-live job=${JOB_ID} generation=${generation} pages=1 leaseEpoch=${lease_epoch} idempotent=true"
