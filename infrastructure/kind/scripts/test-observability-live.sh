#!/usr/bin/env bash
# Live acceptance for provisioned observability and deployed application targets.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "${SCRIPT_DIR}/lib.sh"
require_cmd curl
require_cmd ruby
assert_kind_context

"${SCRIPT_DIR}/wait-observability.sh"

observability_identities=(alloy-logs alloy-otlp loki tempo prometheus alertmanager redis-exporter alert-webhook-sink kube-state-metrics grafana minio minio-bucket-bootstrap)
for identity in "${observability_identities[@]}"; do
  kubectl -n observability get "serviceaccount/${identity}" >/dev/null \
    || die "observability service account missing: ${identity}"
  if kubectl auth can-i get secrets -n uno-arena \
      --as="system:serviceaccount:observability:${identity}" | grep -qx yes; then
    die "observability service account must not read application Secrets: ${identity}"
  fi
done

pids=()
cleanup() {
  for pid in "${pids[@]}"; do kill "${pid}" >/dev/null 2>&1 || true; done
}
trap cleanup EXIT

forward() {
  local namespace="$1" service="$2" local_port="$3" remote_port="$4"
  kubectl -n "${namespace}" port-forward --address=127.0.0.1 "service/${service}" "${local_port}:${remote_port}" >/dev/null 2>&1 &
  pids+=("$!")
}

forward observability prometheus "${PROMETHEUS_LOCAL_PORT:-19090}" 9090
forward observability alertmanager "${ALERTMANAGER_LOCAL_PORT:-19093}" 9093
forward observability loki "${LOKI_LOCAL_PORT:-13100}" 3100
forward observability tempo "${TEMPO_LOCAL_PORT:-13200}" 3200
forward observability grafana "${GRAFANA_LOCAL_PORT:-13000}" 3000
forward "${KIND_NAMESPACE}" gateway "${GATEWAY_LOCAL_PORT:-18080}" 8080

wait_http() {
  local url="$1"
  for _ in {1..30}; do curl -fsS "${url}" >/dev/null 2>&1 && return 0; sleep 1; done
  die "endpoint not ready: ${url}"
}

wait_http "http://127.0.0.1:${PROMETHEUS_LOCAL_PORT:-19090}/-/ready"
wait_http "http://127.0.0.1:${ALERTMANAGER_LOCAL_PORT:-19093}/-/ready"
wait_http "http://127.0.0.1:${LOKI_LOCAL_PORT:-13100}/ready"
wait_http "http://127.0.0.1:${TEMPO_LOCAL_PORT:-13200}/ready"
wait_http "http://127.0.0.1:${GRAFANA_LOCAL_PORT:-13000}/api/health"
wait_http "http://127.0.0.1:${GATEWAY_LOCAL_PORT:-18080}/ready"

secret_value() {
  kubectl -n observability get secret uno-arena-local-observability -o "jsonpath={.data.$1}" | ruby -rbase64 -e 'print Base64.decode64(STDIN.read)'
}
grafana_user="$(secret_value admin-user)"
grafana_password="$(secret_value admin-password)"
datasources="$(curl -fsS -u "${grafana_user}:${grafana_password}" "http://127.0.0.1:${GRAFANA_LOCAL_PORT:-13000}/api/datasources")"
dashboards="$(curl -fsS -u "${grafana_user}:${grafana_password}" "http://127.0.0.1:${GRAFANA_LOCAL_PORT:-13000}/api/search?type=dash-db")"
for uid in prometheus loki tempo; do grep -Fq '"uid":"'"${uid}"'"' <<<"${datasources}" || die "Grafana data source missing: ${uid}"; done
for uid in uno-arena-business uno-arena-cluster uno-arena-red; do grep -Fq '"uid":"'"${uid}"'"' <<<"${dashboards}" || die "Grafana dashboard missing: ${uid}"; done
printf '%s' "${datasources}" | ruby -rjson -e '
  sources=JSON.parse(STDIN.read)
  tempo=sources.find { |source| source["uid"] == "tempo" }
  abort "Grafana Tempo data source missing" unless tempo
  mapping=tempo.dig("jsonData", "tracesToLogsV2") || {}
  abort "Tempo trace-to-logs must target Loki" unless mapping["datasourceUid"] == "loki"
  abort "Tempo trace-to-logs must filter trace IDs" unless mapping["filterByTraceID"] == true
  abort "Tempo trace-to-logs must filter span IDs" unless mapping["filterBySpanID"] == true
  tags=(mapping["tags"] || []).map { |tag| [tag["key"], tag["value"]] }
  expected=[["service.name", "service"], ["unoarena.component", "component"]]
  abort "Tempo trace-to-logs label mapping mismatch: #{tags.inspect}" unless tags == expected
'

targets="$(curl -fsS "http://127.0.0.1:${PROMETHEUS_LOCAL_PORT:-19090}/api/v1/targets")"
printf '%s' "${targets}" | ruby -rjson -e '
  body=JSON.parse(STDIN.read)
  abort "Prometheus targets query failed" unless body["status"] == "success"
  active=body.dig("data", "activeTargets") || []
  %w[prometheus alertmanager redis-exporter alloy-logs alloy-otlp loki tempo grafana kube-state-metrics kubelet-cadvisor uno-arena-services uno-arena-workers].each do |job|
    matches=active.select { |target| target.dig("labels", "job") == job }
    abort "Prometheus target missing: #{job}" if matches.empty?
    bad=matches.reject { |target| target["health"] == "up" }
    abort "Prometheus target not healthy: #{job}" unless bad.empty?
  end
'
for query in 'count(kube_pod_status_ready{condition="true"})' 'count(container_cpu_usage_seconds_total{container!=""})'; do
  curl -fsS --get --data-urlencode "query=${query}" \
    "http://127.0.0.1:${PROMETHEUS_LOCAL_PORT:-19090}/api/v1/query" | ruby -rjson -e '
      body=JSON.parse(STDIN.read)
      abort "Prometheus metric query failed" unless body["status"] == "success"
      result=body.dig("data", "result") || []
      abort "representative cluster metric absent" if result.empty? || result[0].dig("value", 1).to_f <= 0
    '
done

# Exercise a representative API route only after both API and worker scrape
# jobs exist and are healthy. Worker health here is scrape availability, not an
# HTTP work metric. This probe must fail before application deployment instead
# of treating observability-only readiness as end-to-end API RED evidence.
for _ in {1..3}; do
  curl -fsS "http://127.0.0.1:${GATEWAY_LOCAL_PORT:-18080}/v1/rooms?limit=1" >/dev/null
done

prometheus_query_positive() {
  local query="$1"
  curl -fsS --get --data-urlencode "query=${query}" \
    "http://127.0.0.1:${PROMETHEUS_LOCAL_PORT:-19090}/api/v1/query" | ruby -rjson -e '
      body=JSON.parse(STDIN.read)
      exit 1 unless body["status"] == "success"
      result=body.dig("data", "result") || []
      exit(result.any? { |series| series.dig("value", 1).to_f > 0 } ? 0 : 1)
    '
}

wait_for_red_series() {
  local name="$1" query="$2"
  for _ in $(seq 1 "${OBS_RED_WAIT_ATTEMPTS:-45}"); do
    prometheus_query_positive "${query}" && return 0
    sleep "${OBS_RED_WAIT_INTERVAL_SECONDS:-2}"
  done
  die "RED metric missing or non-positive after real Gateway request: ${name}"
}

wait_for_red_series raw-count 'sum(http_server_request_duration_seconds_count{job="uno-arena-services",service="gateway"})'
wait_for_red_series raw-bucket 'sum(http_server_request_duration_seconds_bucket{job="uno-arena-services",service="gateway"})'
wait_for_red_series recorded-rate 'sum(unoarena:http_requests:rate5m{service="gateway"})'
wait_for_red_series recorded-p95 'sum(unoarena:http_request_duration_seconds:p95_5m{service="gateway"})'

rules="$(curl -fsS "http://127.0.0.1:${PROMETHEUS_LOCAL_PORT:-19090}/api/v1/rules")"
printf '%s' "${rules}" | ruby -rjson -e '
  body=JSON.parse(STDIN.read)
  abort "Prometheus rules query failed" unless body["status"] == "success"
  groups=body.dig("data", "groups") || []
  names=groups.map { |group| group["name"] }
  expected=%w[uno-arena-red-recording uno-arena-service-alerts uno-arena-observability-alerts]
  missing=expected-names
  abort "Prometheus rule groups missing: #{missing.inspect}" unless missing.empty?
'

probe_since="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
curl -fsS -X POST -H 'Content-Type: application/json' \
  --data '[{"labels":{"alertname":"UnoArenaWebhookDeliveryProbe","severity":"info"},"annotations":{"summary":"kind delivery probe"}}]' \
  "http://127.0.0.1:${ALERTMANAGER_LOCAL_PORT:-19093}/api/v2/alerts" >/dev/null
delivered=false
for _ in {1..30}; do
  if kubectl -n observability logs deployment/alert-webhook-sink --since-time="${probe_since}" 2>/dev/null | \
      grep -Fq 'POST /alerts?receiver=kind HTTP/1.1" 204'; then
    delivered=true
    break
  fi
  sleep 1
done
[[ "${delivered}" == true ]] || die "Alertmanager did not deliver the probe to the in-cluster webhook sink"
resolved_at="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
curl -fsS -X POST -H 'Content-Type: application/json' \
  --data "[{\"labels\":{\"alertname\":\"UnoArenaWebhookDeliveryProbe\",\"severity\":\"info\"},\"annotations\":{\"summary\":\"kind delivery probe\"},\"endsAt\":\"${resolved_at}\"}]" \
  "http://127.0.0.1:${ALERTMANAGER_LOCAL_PORT:-19093}/api/v2/alerts" >/dev/null

echo "ok kind-observability-live datasources=3 dashboards=3 api-http-red=gateway worker-scrape=up alert-delivery=204"
