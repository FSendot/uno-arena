#!/usr/bin/env bash
# Prove gameplay remains available while the OTLP gateway is absent and that a
# newly created trace is exported after the gateway recovers.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
# shellcheck source=lib.sh
source "${SCRIPT_DIR}/lib.sh"
# shellcheck source=observability-acceptance-lib.sh
source "${SCRIPT_DIR}/observability-acceptance-lib.sh"
require_cmd curl
require_cmd kubectl
require_cmd ruby
assert_kind_context
CLI="${ROOT}/client-checkpoint/bin/unoarena"
GATEWAY_PORT="${GATEWAY_LOCAL_PORT:-18080}"
TEMPO_PORT="${TEMPO_LOCAL_PORT:-13200}"
obs_forward "${KIND_NAMESPACE}" service/gateway "${GATEWAY_PORT}" 8080
obs_forward observability service/tempo "${TEMPO_PORT}" 3200
export UNOARENA_API_URL="http://127.0.0.1:${GATEWAY_PORT}"
TEMPO="http://127.0.0.1:${TEMPO_PORT}"
obs_wait_http "${UNOARENA_API_URL}/ready"
obs_wait_http "${TEMPO}/ready"

original_replicas="$(kubectl -n observability get deployment alloy-otlp -o jsonpath='{.spec.replicas}')"
[[ "${original_replicas}" =~ ^[1-9][0-9]*$ ]] || die "alloy-otlp must start with at least one replica"
restore_alloy() {
  kubectl -n observability scale deployment/alloy-otlp --replicas="${original_replicas}" >/dev/null 2>&1 || true
  kubectl -n observability rollout status deployment/alloy-otlp --timeout=180s >/dev/null 2>&1 || true
  obs_cleanup_forwards
}
trap restore_alloy EXIT INT TERM

prefix="obso$(date +%s)$$"
seed="$(${CLI} seed --count 1 --prefix "${prefix}")"
token="$(printf '%s\n' "${seed}" | obs_json_field token)"
kubectl -n observability scale deployment/alloy-otlp --replicas=0
deadline=$((SECONDS + 180))
while (( SECONDS < deadline )); do
  [[ "$(kubectl -n observability get pod -l app.kubernetes.io/name=alloy-otlp --no-headers 2>/dev/null | wc -l | tr -d ' ')" == "0" ]] && break
  sleep 2
done
[[ "$(kubectl -n observability get pod -l app.kubernetes.io/name=alloy-otlp --no-headers 2>/dev/null | wc -l | tr -d ' ')" == "0" ]] || \
  die "alloy-otlp pods did not terminate"
down_room="$(obs_unique obs-otlp-down | tr -d '-')"
payload="$(ruby -rjson -e 'puts JSON.generate({roomId:ARGV[0],maxSeats:2})' "${down_room}")"
down_result="$(${CLI} room create --token "${token}" --payload "${payload}" \
  --command-id "create-${down_room}" --correlation-id "corr-${down_room}")"
[[ "$(printf '%s' "${down_result}" | obs_json_field status)" == "accepted" ]] || die "gameplay failed while alloy-otlp was unavailable"
curl -fsS "${UNOARENA_API_URL}/ready" >/dev/null || die "Gateway readiness coupled to exporter availability"

kubectl -n observability scale deployment/alloy-otlp --replicas="${original_replicas}"
kubectl -n observability rollout status deployment/alloy-otlp --timeout=180s
# Restoring the collector releases all application exporters at once. On the
# single-node acceptance cluster, wait for that bounded reconnect burst to drain
# and for every backend to regain readiness before issuing the first post-outage
# mutation. Gameplay availability was already proven above while Alloy was down.
KIND_OBSERVABILITY_TIMEOUT="${OBS_OUTAGE_RECOVERY_TIMEOUT:-300s}" \
  "${SCRIPT_DIR}/wait-observability.sh"

trace_has_recovered_http_path() {
  ruby -rjson -e '
    body = JSON.parse(STDIN.read)
    scalar = ->(value) { value.is_a?(Hash) ? (value["stringValue"] || value["string_value"] || value.values.first) : value }
    services = {}
    (body["batches"] || body["resourceSpans"] || body["resource_spans"] || []).each do |batch|
      resource = (batch.dig("resource", "attributes") || []).each_with_object({}) do |attribute, out|
        out[attribute["key"]] = scalar.call(attribute["value"])
      end
      service = resource["service.name"].to_s
      (batch["scopeSpans"] || batch["scope_spans"] || batch["instrumentationLibrarySpans"] || []).each do |scope|
        (scope["spans"] || []).each do |span|
          attributes = (span["attributes"] || []).each_with_object({}) do |attribute, out|
            out[attribute["key"]] = scalar.call(attribute["value"])
          end
          services[service] = true if span["kind"] == "SPAN_KIND_SERVER" &&
            !attributes["http.request.method"].to_s.empty? && !attributes["url.path"].to_s.empty?
        end
      end
    end
    exit(%w[gateway room-gameplay].all? { |service| services[service] } ? 0 : 1)
  ' 2>/dev/null
}

# A batch already in an application exporter when Alloy disappears may be
# dropped by policy through its stale gRPC connection after Alloy becomes
# Ready. Issue fresh commands until one complete new trace proves reconnection.
recovery_deadline=$((SECONDS + ${OBS_TRACE_TIMEOUT_SECONDS:-180}))
attempt=0
last_trace_id=""
while (( SECONDS < recovery_deadline )); do
  attempt=$((attempt + 1))
  up_room="$(obs_unique "obs-otlp-up-${attempt}" | tr -d '-')"
  up_corr="corr-${up_room}"
  payload="$(ruby -rjson -e 'puts JSON.generate({roomId:ARGV[0],maxSeats:2})' "${up_room}")"
  up_result="$(${CLI} room create --token "${token}" --payload "${payload}" \
    --command-id "create-${up_room}" --correlation-id "${up_corr}")"
  [[ "$(printf '%s' "${up_result}" | obs_json_field status)" == "accepted" ]] || die "gameplay failed after alloy-otlp recovery"

  row=""
  row_deadline=$((SECONDS + 30))
  (( row_deadline > recovery_deadline )) && row_deadline=${recovery_deadline}
  while (( SECONDS < row_deadline )); do
    row="$(kubectl -n "${KIND_NAMESPACE}" exec deploy/postgres-room-gameplay -- \
      psql -U room_admin -d room_gameplay -tA -F '|' -c \
      "SELECT correlation_id, traceparent FROM realtime_outbox_events WHERE correlation_id='${up_corr}' AND traceparent IS NOT NULL AND traceparent <> '' ORDER BY outbox_id DESC LIMIT 1" 2>/dev/null | tr -d '\r')"
    [[ -n "${row}" ]] && break
    sleep 2
  done
  [[ -n "${row}" ]] || continue
  last_trace_id="$(obs_trace_id_from_traceparent "${row#*|}")"

  trace_deadline=$((SECONDS + 30))
  (( trace_deadline > recovery_deadline )) && trace_deadline=${recovery_deadline}
  while (( SECONDS < trace_deadline )); do
    trace="$(curl -fsS -H 'Accept: application/json' "${TEMPO}/api/traces/${last_trace_id}" 2>/dev/null || true)"
    if printf '%s' "${trace}" | trace_has_recovered_http_path; then
      echo "ok kind-observability-outage gameplay_continued=${down_room} recovered_trace=${last_trace_id} attempts=${attempt}"
      exit 0
    fi
    sleep 3
  done
done
die "no complete new trace resumed after alloy-otlp recovery; last_trace=${last_trace_id:-none} attempts=${attempt}"
