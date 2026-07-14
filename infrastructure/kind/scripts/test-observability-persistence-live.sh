#!/usr/bin/env bash
# Force current Loki/Tempo data to object storage, replace both backends and
# MinIO, recreate Tempo with an empty WAL PVC, then prove the same log and trace
# remain queryable from object storage.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "${SCRIPT_DIR}/lib.sh"
# shellcheck source=observability-acceptance-lib.sh
source "${SCRIPT_DIR}/observability-acceptance-lib.sh"
require_cmd curl
require_cmd helm
require_cmd kubectl
require_cmd ruby
assert_kind_context
report_error() {
  local rc=$? line="$1"
  echo "error: observability persistence probe failed at line ${line} (exit ${rc})" >&2
  return "${rc}"
}
trap 'report_error ${LINENO}' ERR
"${SCRIPT_DIR}/wait-observability.sh"

state="${OBS_ACCEPTANCE_STATE_FILE:-${TMPDIR:-/tmp}/uno-arena-observability-acceptance.state}"
[[ -f "${state}" ]] || die "trace acceptance state missing: ${state}; run trace acceptance first"
# shellcheck disable=SC1090
source "${state}"
[[ "${TRACE_ID:-}" =~ ^[[:xdigit:]]{32}$ ]] || die "invalid TRACE_ID in ${state}"
[[ -n "${CORRELATION_ID:-}" ]] || die "missing CORRELATION_ID in ${state}"
echo "persistence precondition: trace acceptance state loaded"

LOKI_PORT="${LOKI_LOCAL_PORT:-13100}"
TEMPO_PORT="${TEMPO_LOCAL_PORT:-13200}"
GATEWAY_PORT="${GATEWAY_LOCAL_PORT:-18081}"
obs_forward observability service/loki "${LOKI_PORT}" 3100
obs_forward observability service/tempo "${TEMPO_PORT}" 3200
obs_forward "${KIND_NAMESPACE}" service/gateway "${GATEWAY_PORT}" 8080
trap obs_cleanup_forwards EXIT INT TERM
LOKI="http://127.0.0.1:${LOKI_PORT}"
TEMPO="http://127.0.0.1:${TEMPO_PORT}"
export UNOARENA_API_URL="http://127.0.0.1:${GATEWAY_PORT}"
obs_wait_http "${LOKI}/ready"
obs_wait_http "${TEMPO}/ready"
obs_wait_http "${UNOARENA_API_URL}/ready"
echo "persistence precondition: Loki, Tempo, and Gateway endpoints ready"

# Use a fresh, low-volume application correlation marker independent of the
# canonical high-volume gameplay trace. The mutation still goes through the
# public CLI, and both Gateway and Identity record the correlation ID.
CLI="${REPO_ROOT}/client-checkpoint/bin/unoarena"
[[ -x "${CLI}" ]] || die "client CLI is not executable: ${CLI}"
marker_prefix="obspersist$(date +%s)$$"
LOG_MARKER="$(ruby -rsecurerandom -e 'puts SecureRandom.uuid')"
echo "persistence marker: registering one fresh player through Gateway"
marker_error="$(mktemp)"
if ! marker_json="$(CORRELATION_ID="${LOG_MARKER}" "${CLI}" seed --count 1 --prefix "${marker_prefix}" 2>"${marker_error}")"; then
  cat "${marker_error}" >&2
  rm -f "${marker_error}"
  die "persistence marker registration failed; outcome may be unknown and was not replayed"
fi
rm -f "${marker_error}"
if ! marker_player_id="$(printf '%s\n' "${marker_json}" | obs_json_field playerId)"; then
  die "persistence marker registration returned invalid JSON/playerId"
fi
if ! marker_token="$(printf '%s\n' "${marker_json}" | obs_json_field token)"; then
  die "persistence marker registration returned invalid JSON/token"
fi
[[ -n "${marker_player_id}" ]] || die "persistence marker registration returned no playerId"
[[ -n "${marker_token}" ]] || die "persistence marker registration returned no token"
echo "persistence marker: registration accepted"

loki_query() {
  curl -fsS --get \
    --data-urlencode "query={namespace=\"uno-arena\"} |= \`${LOG_MARKER}\`" \
    --data-urlencode "start=$(( ($(date +%s) - 7200) * 1000000000 ))" \
    --data-urlencode "end=$(( $(date +%s) * 1000000000 ))" \
    --data-urlencode 'limit=100' "${LOKI}/loki/api/v1/query_range"
}
assert_loki_result() {
  loki_query | ruby -rjson -e 'b=JSON.parse(STDIN.read); abort "persisted Loki result missing" if (b.dig("data","result") || []).empty?'
}
assert_tempo_result() {
  local body
  body="$(curl -fsS -H 'Accept: application/json' "${TEMPO}/api/traces/${TRACE_ID}")"
  grep -q 'resource' <<<"${body}" || die "persisted Tempo trace missing: ${TRACE_ID}"
}
assert_tempo_block_result() {
  local body
  body="$(curl -fsS -H 'Accept: application/json' \
    "${TEMPO}/querier/api/traces/${TRACE_ID}?mode=blocks")"
  grep -q 'resource' <<<"${body}" || die "Tempo object-store block missing: ${TRACE_ID}"
}

marker_deadline=$((SECONDS + ${OBS_LOG_MARKER_TIMEOUT_SECONDS:-90}))
while ! assert_loki_result >/dev/null 2>&1; do
  (( SECONDS < marker_deadline )) || die "fresh application log marker did not reach Loki: ${LOG_MARKER}"
  # Registration is a one-shot mutation and is never replayed. If its stdout
  # log crossed the brief post-outage recovery window, safely repeat an
  # authenticated read with the same correlation marker until Alloy/Loki
  # proves the application log path is live.
  curl -fsS --max-time 10 \
    -H "Authorization: Bearer ${marker_token}" \
    -H "X-Correlation-Id: ${LOG_MARKER}" \
    "${UNOARENA_API_URL}/v1/auth/whoami" >/dev/null 2>&1 || true
  sleep 2
done
echo "persistence marker: application log observed in Loki"
assert_tempo_result
echo "persistence trace: canonical trace observed in Tempo"
# Tempo 3 has no public /flush endpoint. Wait until its supported querier
# debug path can retrieve this trace from blocks alone, proving that the trace
# reached S3 before the WAL is destroyed.
flush_deadline=$((SECONDS + ${OBS_OBJECT_FLUSH_TIMEOUT_SECONDS:-180}))
while ! assert_tempo_block_result >/dev/null 2>&1; do
  (( SECONDS < flush_deadline )) || die "Tempo trace did not reach object storage: ${TRACE_ID}"
  sleep 5
done
echo "persistence trace: canonical trace observed through Tempo block storage"
sleep "${OBS_OBJECT_FLUSH_SETTLE_SECONDS:-15}"

wait_replacement_uid() {
  local workload="$1" old_uid="$2" deadline=$((SECONDS + 180)) payload new_uid
  while (( SECONDS < deadline )); do
    payload="$(kubectl -n observability get pod -l "app.kubernetes.io/name=${workload}" -o json)"
    new_uid="$(printf '%s' "${payload}" | ruby -rjson -e '
      old=ARGV.fetch(0); pods=JSON.parse(STDIN.read).fetch("items", [])
      pod=pods.find { |p| p.dig("metadata","uid") != old && (p.dig("status","conditions") || []).any? { |c| c["type"]=="Ready" && c["status"]=="True" } }
      puts pod.dig("metadata","uid") if pod
    ' "${old_uid}")"
    if [[ -n "${new_uid}" ]]; then printf '%s\n' "${new_uid}"; return 0; fi
    sleep 2
  done
  die "${workload} did not produce a distinct Ready replacement pod"
}

ready_pod_name() {
  local workload="$1"
  kubectl -n observability get pod -l "app.kubernetes.io/name=${workload}" -o json | ruby -rjson -e '
    pods=JSON.parse(STDIN.read).fetch("items", [])
    pod=pods.find { |p| (p.dig("status","conditions") || []).any? { |c| c["type"]=="Ready" && c["status"]=="True" } }
    abort "no Ready pod for #{ARGV.fetch(0)}" unless pod
    puts pod.dig("metadata","name")
  ' "${workload}"
}

wait_loki_graceful_restart() {
  local pod_name="$1" pod_uid="$2" old_restart_count="$3"
  local deadline=$((SECONDS + 180)) payload
  while (( SECONDS < deadline )); do
    payload="$(kubectl -n observability get pod "${pod_name}" -o json 2>/dev/null || true)"
    if [[ -n "${payload}" ]] && printf '%s' "${payload}" | ruby -rjson -e '
      pod=JSON.parse(STDIN.read)
      same_uid=pod.dig("metadata","uid") == ARGV.fetch(0)
      restart=(pod.dig("status","containerStatuses",0,"restartCount") || 0).to_i
      ready=(pod.dig("status","conditions") || []).any? { |c| c["type"]=="Ready" && c["status"]=="True" }
      exit(same_uid && restart > ARGV.fetch(1).to_i && ready ? 0 : 1)
    ' "${pod_uid}" "${old_restart_count}"; then
      return 0
    fi
    sleep 2
  done
  die "Loki did not complete a graceful full-process restart for TSDB index shipping"
}

# Kubernetes may commit a mutation even when the API client sees an etcd or
# transport timeout. Retry only idempotent desired-state mutations, and delete
# exact pod names so a retry can never target the replacement selected by a
# workload label.
kubectl_mutation_retry() {
  local retries="${OBS_KUBECTL_MUTATION_RETRIES:-5}" attempt output rc=1
  [[ "${retries}" =~ ^[1-9][0-9]*$ ]] && (( retries <= 20 )) \
    || die "OBS_KUBECTL_MUTATION_RETRIES must be an integer from 1 to 20"
  for ((attempt=1; attempt<=retries; attempt++)); do
    if output="$("$@" 2>&1)"; then
      [[ -z "${output}" ]] || printf '%s\n' "${output}"
      return 0
    else
      rc=$?
    fi
    if (( attempt < retries )); then
      echo "warning: Kubernetes mutation attempt ${attempt}/${retries} failed; retrying exact desired state" >&2
      sleep 2
    fi
  done
  [[ -z "${output:-}" ]] || printf '%s\n' "${output}" >&2
  return "${rc}"
}

old_loki_name="$(ready_pod_name loki)"
old_tempo_name="$(ready_pod_name tempo)"
old_minio_name="$(ready_pod_name minio)"
old_loki_uid="$(kubectl -n observability get pod "${old_loki_name}" -o jsonpath='{.metadata.uid}')"
old_loki_restart_count="$(kubectl -n observability get pod "${old_loki_name}" -o jsonpath='{.status.containerStatuses[0].restartCount}')"
old_tempo_uid="$(kubectl -n observability get pod "${old_tempo_name}" -o jsonpath='{.metadata.uid}')"
old_minio_uid="$(kubectl -n observability get pod "${old_minio_name}" -o jsonpath='{.metadata.uid}')"
old_tempo_wal_uid="$(kubectl -n observability get pvc tempo-wal -o jsonpath='{.metadata.uid}')"

# Stop Loki synchronously with chunk flushing and full-process termination. The
# process shutdown builds the active 15-minute TSDB head and the shipper uploads
# it before exit. Wait for kubelet to restart that same pod, then delete its exact
# identity so the distinct replacement has no emptyDir index/WAL to fall back to.
shutdown_status=""
shutdown_rc=0
shutdown_status="$(curl -sS -o /dev/null -w '%{http_code}' -X POST \
  "${LOKI}/ingester/shutdown?flush=true&delete_ring_tokens=false&terminate=true")" \
  || shutdown_rc=$?
# Loki closes the request connection after the ingester module stops in
# monolithic mode. curl 52 is therefore accepted here; recovery from S3 below
# remains the authoritative proof that the flush actually completed.
if (( shutdown_rc == 0 )); then
  [[ "${shutdown_status}" == "200" || "${shutdown_status}" == "204" ]] \
    || die "Loki ingester shutdown/flush failed with HTTP ${shutdown_status}"
elif (( shutdown_rc != 52 )); then
  die "Loki ingester shutdown/flush request failed with curl exit ${shutdown_rc}"
fi
wait_loki_graceful_restart "${old_loki_name}" "${old_loki_uid}" "${old_loki_restart_count}"
kubectl_mutation_retry kubectl -n observability delete pod "${old_loki_name}" --ignore-not-found --wait=false
new_loki_uid="$(wait_replacement_uid loki "${old_loki_uid}")"
obs_cleanup_forwards
obs_forward observability service/loki "${LOKI_PORT}" 3100
obs_forward observability service/tempo "${TEMPO_PORT}" 3200
obs_wait_http "${LOKI}/ready" 90
obs_wait_http "${TEMPO}/ready" 90
loki_recovery_deadline=$((SECONDS + ${OBS_PERSISTENCE_TIMEOUT_SECONDS:-180}))
while ! assert_loki_result >/dev/null 2>&1; do
  (( SECONDS < loki_recovery_deadline )) \
    || die "Loki replacement could not recover accepted log from S3: ${LOG_MARKER}"
  sleep 5
done

kubectl_mutation_retry kubectl -n observability scale deployment/tempo --replicas=0
kubectl -n observability wait --for=delete pod -l app.kubernetes.io/name=tempo --timeout=180s
kubectl_mutation_retry kubectl -n observability delete pvc tempo-wal --ignore-not-found --wait=true
kubectl_mutation_retry kubectl -n observability delete pod "${old_minio_name}" --ignore-not-found --wait=false
kubectl -n observability rollout status deployment/minio --timeout=180s
chart="${REPO_ROOT}/infrastructure/observability/helm/uno-arena-observability"
helm upgrade --install uno-arena-observability "${chart}" -n observability \
  -f "${chart}/values.kind.yaml" --wait --timeout 180s
kubectl -n observability rollout status deployment/tempo --timeout=180s
new_tempo_wal_uid="$(kubectl -n observability get pvc tempo-wal -o jsonpath='{.metadata.uid}')"
[[ -n "${new_tempo_wal_uid}" && "${new_tempo_wal_uid}" != "${old_tempo_wal_uid}" ]] \
  || die "Tempo WAL PVC was not recreated; object-store recovery was not isolated"

new_minio_uid="$(wait_replacement_uid minio "${old_minio_uid}")"
new_tempo_uid="$(wait_replacement_uid tempo "${old_tempo_uid}")"

obs_cleanup_forwards
obs_forward observability service/loki "${LOKI_PORT}" 3100
obs_forward observability service/tempo "${TEMPO_PORT}" 3200
obs_wait_http "${LOKI}/ready" 90
obs_wait_http "${TEMPO}/ready" 90
deadline=$((SECONDS + ${OBS_PERSISTENCE_TIMEOUT_SECONDS:-180}))
while (( SECONDS < deadline )); do
  if assert_loki_result >/dev/null 2>&1 && assert_tempo_result >/dev/null 2>&1; then
    rm -f "${state}"
    echo "ok kind-observability-persistence trace_id=${TRACE_ID} log_marker=${LOG_MARKER} correlationId=${CORRELATION_ID} backends=loki,tempo,minio"
    exit 0
  fi
  sleep 5
done
die "Loki/Tempo evidence did not recover after backend and MinIO pod replacement"
