#!/usr/bin/env bash
# EXPLICIT/NETWORKED: run the real Spectator projection-rebuilder process against
# Kafka -> Room recovery HTTP -> Redis. All Kafka topics/groups and Redis keys are
# per-run. The one authoritative Room fixture is deleted by room_id on cleanup.
# The chart deployment remains disabled; this harness creates one temporary Pod.
set -euo pipefail
set -m

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "${SCRIPT_DIR}/lib.sh"

require_cmd kubectl
require_cmd curl
require_cmd jq
require_cmd nc
assert_kind_context

TIMEOUT_S="${KIND_SPECTATOR_REBUILDER_LIVE_TIMEOUT_S:-120}"
SUFFIX="$(od -An -N6 -tx1 /dev/urandom | tr -d ' \n')"
POD="spectator-rebuilder-live-${SUFFIX}"
ROOM_ID="room_rebuild_${SUFFIX}"
USERNAME="${SPECTATOR_REBUILD_TEST_USERNAME:-rebuild_${SUFFIX}}"
PASSWORD="${SPECTATOR_REBUILD_TEST_PASSWORD:-rebuild-${SUFFIX}-pass}"
JOB_ID="spectator-rebuild-${SUFFIX}"
COMMAND_ID="create-rebuild-${SUFFIX}"
GROUP="spectator-rebuilder-live-${SUFFIX}"
REQUEST_TOPIC="spectator.rebuild.live.${SUFFIX}"
REQUEST_DLQ="spectator.rebuild.live.${SUFFIX}.dlq"
HELD_DLQ="spectator.held.live.${SUFFIX}.dlq"
REDIS_PREFIX="spectator-live-${SUFFIX}:"
REDIS_DB=5

PF_PID=""
PF_LOG=""
CREATE_RESPONSE_FILE=""
ROOM_LOCAL_PORT=""
ROOM_CREATED=0
IDENTITY_PLAYER_ID=""
POD_CREATED=0
TOPICS_CREATED=0

kafka() {
  kubectl -n "${KIND_NAMESPACE}" exec deploy/kafka -- /opt/kafka/bin/"$@"
}

redis() {
  kubectl -n "${KIND_NAMESPACE}" exec deploy/redis -- redis-cli -n "${REDIS_DB}" "$@"
}

room_psql() {
  kubectl -n "${KIND_NAMESPACE}" exec -i deploy/postgres-room-gameplay -- \
    env PGPASSWORD=local-only-room-runtime psql -U room_runtime -d room_gameplay -v ON_ERROR_STOP=1 "$@"
}

identity_psql() {
  kubectl -n "${KIND_NAMESPACE}" exec -i deploy/postgres-identity -- \
    psql -U identity_admin -d identity -v ON_ERROR_STOP=1 "$@"
}

cleanup() {
  local rc=$?
  set +e
  if [[ "${POD_CREATED}" -eq 1 ]]; then
    kubectl -n "${KIND_NAMESPACE}" delete pod "${POD}" --ignore-not-found --wait=false >/dev/null
  fi
  if [[ "${ROOM_CREATED}" -eq 1 ]]; then
    runtime_pod="$(room_psql -Atc "SELECT pod_name FROM room_runtime_assignments WHERE room_id = '${ROOM_ID}'" 2>/dev/null || true)"
    [[ -z "${runtime_pod}" ]] || kubectl -n "${KIND_NAMESPACE}" delete pod "${runtime_pod}" --ignore-not-found --wait=false >/dev/null
    # Outboxes/idempotency are intentionally not FK-owned; delete only this run's rows.
    room_psql <<SQL >/dev/null
BEGIN;
DELETE FROM integration_outbox_events WHERE room_id = '${ROOM_ID}';
DELETE FROM realtime_outbox_events WHERE room_id = '${ROOM_ID}';
DELETE FROM processed_reconciliation_offsets WHERE room_id = '${ROOM_ID}';
DELETE FROM pending_integrity_reconciliations WHERE room_id = '${ROOM_ID}';
DELETE FROM command_idempotency WHERE room_id = '${ROOM_ID}' OR command_id = '${COMMAND_ID}';
DELETE FROM rooms WHERE room_id = '${ROOM_ID}';
COMMIT;
SQL
  fi
  if [[ -n "${IDENTITY_PLAYER_ID}" ]]; then
    identity_psql <<SQL >/dev/null
BEGIN;
DELETE FROM outbox_events WHERE player_id = '${IDENTITY_PLAYER_ID}';
DELETE FROM sessions WHERE player_id = '${IDENTITY_PLAYER_ID}';
DELETE FROM external_identities WHERE player_id = '${IDENTITY_PLAYER_ID}';
DELETE FROM player_acls WHERE player_id = '${IDENTITY_PLAYER_ID}';
DELETE FROM players WHERE player_id = '${IDENTITY_PLAYER_ID}';
COMMIT;
SQL
  fi
  while IFS= read -r redis_key; do
    [[ -n "${redis_key}" ]] && redis DEL "${redis_key}" >/dev/null
  done < <(redis --scan --pattern "${REDIS_PREFIX}*" 2>/dev/null)
  if [[ "${TOPICS_CREATED}" -eq 1 ]]; then
    for topic in "${REQUEST_TOPIC}" "${REQUEST_DLQ}" "${HELD_DLQ}"; do
      kafka kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic "${topic}" >/dev/null 2>&1
    done
    kafka kafka-consumer-groups.sh --bootstrap-server localhost:9092 --delete \
      --group "${GROUP}" >/dev/null 2>&1
  fi
  if [[ -n "${PF_PID}" ]]; then
    kill "${PF_PID}" 2>/dev/null
    wait "${PF_PID}" 2>/dev/null
  fi
  [[ -z "${PF_LOG}" ]] || rm -f "${PF_LOG}"
  [[ -z "${CREATE_RESPONSE_FILE}" ]] || rm -f "${CREATE_RESPONSE_FILE}"
  return "${rc}"
}
trap cleanup EXIT

for deploy in gateway identity postgres-identity kafka redis room-gameplay postgres-room-gameplay spectator-view; do
  kubectl -n "${KIND_NAMESPACE}" get "deploy/${deploy}" >/dev/null 2>&1 \
    || die "deploy/${deploy} not found; deploy the complete kind stack first"
done
secret_json="$(kubectl -n "${KIND_NAMESPACE}" get secret uno-arena-local-credentials -o json)"
for key in room-spectator-recovery-service-credential; do
  [[ "$(jq -r --arg key "${key}" '.data[$key] // empty' <<<"${secret_json}")" != "" ]] \
    || die "uno-arena-local-credentials is missing ${key}; reapply the local secret manifest"
done

TOPICS_CREATED=1
for topic in "${REQUEST_TOPIC}" "${REQUEST_DLQ}" "${HELD_DLQ}"; do
  kafka kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists \
    --topic "${topic}" --partitions 1 --replication-factor 1 >/dev/null
done

PF_LOG="$(mktemp "${TMPDIR:-/tmp}/spectator-rebuilder-gateway-pf.XXXXXX")"
kubectl -n "${KIND_NAMESPACE}" port-forward --address=127.0.0.1 svc/gateway 0:8080 >"${PF_LOG}" 2>&1 &
PF_PID=$!
for _ in $(seq 1 40); do
  ROOM_LOCAL_PORT="$(sed -nE 's/.*Forwarding from 127\.0\.0\.1:([0-9]+) -> 8080.*/\1/p' "${PF_LOG}" | head -n1)"
  if [[ -n "${ROOM_LOCAL_PORT}" ]] && nc -z 127.0.0.1 "${ROOM_LOCAL_PORT}" 2>/dev/null; then
    break
  fi
  kill -0 "${PF_PID}" 2>/dev/null || die "Gateway port-forward exited early (see ${PF_LOG})"
  sleep 0.25
done
[[ -n "${ROOM_LOCAL_PORT}" ]] || die "Gateway port-forward did not become ready (see ${PF_LOG})"

register_response="$(curl -fsS -X POST "http://127.0.0.1:${ROOM_LOCAL_PORT}/v1/auth/register" \
  -H 'Content-Type: application/json' -H "X-Correlation-Id: corr-register-${SUFFIX}" \
  -d "$(jq -cn --arg user "${USERNAME}" --arg pass "${PASSWORD}" '{username:$user,password:$pass}')")" \
  || die "failed to register recovery fixture identity"
[[ "$(jq -r '.status // empty' <<<"${register_response}")" == "ok" ]] \
  || die "Identity fixture registration rejected: ${register_response}"
login_response="$(curl -fsS -X POST "http://127.0.0.1:${ROOM_LOCAL_PORT}/v1/auth/login" \
  -H 'Content-Type: application/json' -H "X-Correlation-Id: corr-login-${SUFFIX}" \
  -d "$(jq -cn --arg user "${USERNAME}" --arg pass "${PASSWORD}" '{username:$user,password:$pass}')")" \
  || die "failed to login recovery fixture identity"
token="$(jq -r '.token // empty' <<<"${login_response}")"
IDENTITY_PLAYER_ID="$(jq -r '.playerId // empty' <<<"${login_response}")"
[[ -n "${token}" && -n "${IDENTITY_PLAYER_ID}" ]] || die "Identity fixture login rejected: ${login_response}"

create_body="$(jq -cn \
  --arg cid "${COMMAND_ID}" --arg room "${ROOM_ID}" \
  '{commandId:$cid,type:"CreateRoom",schemaVersion:1,payload:{roomId:$room,visibility:"public"}}')"
ROOM_CREATED=1
CREATE_RESPONSE_FILE="$(mktemp "${TMPDIR:-/tmp}/spectator-rebuilder-create.XXXXXX")"
create_code=""
create_response=""
# The command ID makes CreateRoom safe to retry when the Gateway temporarily
# loses a Room endpoint during local rollout convergence. Retry only transport
# and gateway-availability statuses; application responses remain fail-closed.
for _ in $(seq 1 10); do
  create_code="$(curl -sS -o "${CREATE_RESPONSE_FILE}" -w '%{http_code}' -X POST \
    "http://127.0.0.1:${ROOM_LOCAL_PORT}/v1/commands" \
    -H 'Content-Type: application/json' -H "Authorization: Bearer ${token}" \
    -H "X-Command-Id: ${COMMAND_ID}" -H "X-Correlation-Id: corr-${SUFFIX}" -d "${create_body}" || true)"
  create_response="$(cat "${CREATE_RESPONSE_FILE}")"
  [[ "${create_code}" =~ ^2[0-9][0-9]$ ]] && break
  case "${create_code}" in
    000|502|503|504) sleep 2 ;;
    *) die "Room recovery fixture failed status=${create_code} body=${create_response}" ;;
  esac
done
[[ "${create_code}" =~ ^2[0-9][0-9]$ ]] \
  || die "failed to create Room recovery fixture after bounded retries status=${create_code} body=${create_response}"
[[ "$(jq -r '.status // empty' <<<"${create_response}")" == "accepted" ]] \
  || die "Room fixture rejected: ${create_response}"

# Dedicated Room runtimes are admitted asynchronously. Do not publish the
# rebuild request until the authoritative assignment is Ready and routable.
deadline=$((SECONDS + 60))
runtime_state=""
until [[ "${runtime_state}" == "ready" ]]; do
  (( SECONDS < deadline )) || die "Room runtime did not become ready for recovery fixture"
  runtime_state="$(room_psql -Atc "SELECT observed_state FROM room_runtime_assignments WHERE room_id = '${ROOM_ID}'" 2>/dev/null || true)"
  [[ "${runtime_state}" == "ready" ]] || sleep 1
done

QUARANTINE_KEY="${REDIS_PREFIX}v1:room:{${ROOM_ID}}:kafka_quarantine"
redis HSET "${QUARANTINE_KEY}" active 1 consumer_group spectator-view \
  source_topic room.spectator-safe.events classification application_error \
  reason live_test_gap quarantined_at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" >/dev/null

POD_CREATED=1
kubectl -n "${KIND_NAMESPACE}" apply -f - >/dev/null <<YAML
apiVersion: v1
kind: Pod
metadata:
  name: ${POD}
  labels:
    app: spectator-projection-rebuilder-live-test
    uno-arena.local/live-test: "true"
    app.kubernetes.io/name: spectator-view
    unoarena.io/component: projection-rebuilder
    unoarena.io/metrics-exposed: "true"
    unoarena.io/metrics-scrape: pod
spec:
  serviceAccountName: spectator-view-projection-rebuilder
  automountServiceAccountToken: false
  restartPolicy: Never
  terminationGracePeriodSeconds: 5
  containers:
    - name: projection-rebuilder
      image: uno-arena/spectator-view:local
      imagePullPolicy: IfNotPresent
      ports: [{name: metrics, containerPort: 9090}]
      env:
        - {name: WORKER_ROLE, value: spectator-projection-rebuilder}
        - {name: TELEMETRY_MODE, value: required}
        - {name: SERVICE_NAME, value: spectator-view}
        - {name: DEPLOYMENT_ENV, value: kind}
        - {name: SERVICE_VERSION, value: local}
        - {name: UNOARENA_COMPONENT, value: projection-rebuilder}
        - {name: METRICS_ADDR, value: ":9090"}
        - {name: OTEL_EXPORTER_OTLP_ENDPOINT, value: http://alloy-otlp.observability.svc.cluster.local:4317}
        - {name: OTEL_EXPORTER_OTLP_PROTOCOL, value: grpc}
        - {name: OTEL_TRACES_SAMPLER, value: parentbased_always_on}
        - {name: OTEL_GO_X_OBSERVABILITY, value: "true"}
        - name: POD_UID
          valueFrom: {fieldRef: {fieldPath: metadata.uid}}
        - {name: REDIS_URL, value: redis://redis.uno-arena.svc.cluster.local:6379/${REDIS_DB}}
        - {name: SPECTATOR_REDIS_KEY_PREFIX, value: "${REDIS_PREFIX}"}
        - {name: KAFKA_BROKERS, value: kafka.uno-arena.svc.cluster.local:9092}
        - {name: KAFKA_CONSUMER_GROUP, value: spectator-view}
        - {name: KAFKA_SPECTATOR_SAFE_TOPIC, value: room.spectator-safe.events}
        - {name: KAFKA_SPECTATOR_SAFE_DLQ_TOPIC, value: "${HELD_DLQ}"}
        - {name: KAFKA_PROJECTION_REBUILD_GROUP, value: "${GROUP}"}
        - {name: KAFKA_PROJECTION_REBUILD_TOPIC, value: "${REQUEST_TOPIC}"}
        - {name: KAFKA_PROJECTION_REBUILD_DLQ_TOPIC, value: "${REQUEST_DLQ}"}
        - {name: KAFKA_HELD_DLQ_MAX_POLL_CYCLES, value: "8"}
        - {name: ROOM_GAMEPLAY_URL, value: http://room-gameplay.uno-arena.svc.cluster.local:8080}
        - name: ROOM_SPECTATOR_RECOVERY_SERVICE_CREDENTIAL
          valueFrom: {secretKeyRef: {name: uno-arena-local-credentials, key: room-spectator-recovery-service-credential}}
YAML

kubectl -n "${KIND_NAMESPACE}" wait --for=jsonpath='{.status.phase}'=Running "pod/${POD}" --timeout=60s >/dev/null \
  || { kubectl -n "${KIND_NAMESPACE}" describe "pod/${POD}"; die "rebuilder Pod did not start"; }
deadline=$((SECONDS + 60))
until kubectl -n "${KIND_NAMESPACE}" logs "${POD}" 2>/dev/null | grep -q projection_rebuilder_started; do
  (( SECONDS < deadline )) || { kubectl -n "${KIND_NAMESPACE}" logs "${POD}" >&2; die "rebuilder did not report startup"; }
  sleep 1
done

payload="$(jq -cn --arg eid "spectator-rebuild-event-${SUFFIX}" --arg corr "corr-${SUFFIX}" \
  --arg job "${JOB_ID}" --arg room "${ROOM_ID}" --arg at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  '{eventId:$eid,eventType:"SpectatorProjectionRebuildRequested",schemaVersion:1,correlationId:$corr,occurredAt:$at,recoveryJobId:$job,roomId:$room,failedCheckpoint:1,expectedSourceTopic:"room.spectator-safe.events"}')"
produce() {
  printf '%s\t%s\n' "${ROOM_ID}" "${payload}" | kubectl -n "${KIND_NAMESPACE}" exec -i deploy/kafka -- \
    /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 \
      --topic "${REQUEST_TOPIC}" --property parse.key=true --property key.separator=$'\t' >/dev/null
}

produce
GEN_KEY="${REDIS_PREFIX}v1:room:{${ROOM_ID}}:generation"
META_KEY="${REDIS_PREFIX}v1:room:{${ROOM_ID}}:meta"
deadline=$((SECONDS + TIMEOUT_S))
while (( SECONDS < deadline )); do
  generation="$(redis GET "${GEN_KEY}" 2>/dev/null | tr -d '\r')"
  sequence="$(redis HGET "${META_KEY}" sequence 2>/dev/null | tr -d '\r')"
  released="$(redis HGET "${QUARANTINE_KEY}" active 2>/dev/null | tr -d '\r')"
  if [[ "${generation}" == "2" && "${sequence}" == "1" && "${released}" == "0" ]]; then
    break
  fi
  if ! kubectl -n "${KIND_NAMESPACE}" get "pod/${POD}" -o jsonpath='{.status.phase}' 2>/dev/null | grep -q Running; then
    kubectl -n "${KIND_NAMESPACE}" logs "${POD}" >&2
    die "rebuilder Pod stopped before recovery completed"
  fi
  sleep 2
done
[[ "${generation:-}" == "2" && "${sequence:-}" == "1" && "${released:-}" == "0" ]] \
  || { kubectl -n "${KIND_NAMESPACE}" logs "${POD}" >&2; die "fenced Redis swap/quarantine release not observed"; }
[[ "$(redis HGET "${QUARANTINE_KEY}" release_note | tr -d '\r')" == "recovery_continuity_proven" ]] \
  || die "quarantine release audit note missing"

# Exact redelivery must hit the atomic done marker: no second generation swap.
produce
sleep 5
[[ "$(redis GET "${GEN_KEY}" | tr -d '\r')" == "2" ]] \
  || die "idempotent redelivery changed the active generation"

echo "ok kind-test-spectator-projection-rebuilder-live room=${ROOM_ID} generation=2 sequence=1 idempotent=true"
