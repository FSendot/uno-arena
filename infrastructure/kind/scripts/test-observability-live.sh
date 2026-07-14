#!/usr/bin/env bash
# Read-only live acceptance for provisioned observability infrastructure.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "${SCRIPT_DIR}/lib.sh"
require_cmd curl
require_cmd ruby
assert_kind_context

"${SCRIPT_DIR}/wait-observability.sh"

observability_identities=(alloy-logs alloy-otlp loki tempo prometheus kube-state-metrics grafana minio minio-bucket-bootstrap)
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
  local service="$1" local_port="$2" remote_port="$3"
  kubectl -n observability port-forward --address=127.0.0.1 "service/${service}" "${local_port}:${remote_port}" >/dev/null 2>&1 &
  pids+=("$!")
}

forward prometheus "${PROMETHEUS_LOCAL_PORT:-19090}" 9090
forward loki "${LOKI_LOCAL_PORT:-13100}" 3100
forward tempo "${TEMPO_LOCAL_PORT:-13200}" 3200
forward grafana "${GRAFANA_LOCAL_PORT:-13000}" 3000

wait_http() {
  local url="$1"
  for _ in {1..30}; do curl -fsS "${url}" >/dev/null 2>&1 && return 0; sleep 1; done
  die "endpoint not ready: ${url}"
}

wait_http "http://127.0.0.1:${PROMETHEUS_LOCAL_PORT:-19090}/-/ready"
wait_http "http://127.0.0.1:${LOKI_LOCAL_PORT:-13100}/ready"
wait_http "http://127.0.0.1:${TEMPO_LOCAL_PORT:-13200}/ready"
wait_http "http://127.0.0.1:${GRAFANA_LOCAL_PORT:-13000}/api/health"

secret_value() {
  kubectl -n observability get secret uno-arena-local-observability -o "jsonpath={.data.$1}" | ruby -rbase64 -e 'print Base64.decode64(STDIN.read)'
}
grafana_user="$(secret_value admin-user)"
grafana_password="$(secret_value admin-password)"
datasources="$(curl -fsS -u "${grafana_user}:${grafana_password}" "http://127.0.0.1:${GRAFANA_LOCAL_PORT:-13000}/api/datasources")"
dashboards="$(curl -fsS -u "${grafana_user}:${grafana_password}" "http://127.0.0.1:${GRAFANA_LOCAL_PORT:-13000}/api/search?type=dash-db")"
for uid in prometheus loki tempo; do grep -Fq '"uid":"'"${uid}"'"' <<<"${datasources}" || die "Grafana data source missing: ${uid}"; done
for uid in uno-arena-business uno-arena-cluster; do grep -Fq '"uid":"'"${uid}"'"' <<<"${dashboards}" || die "Grafana dashboard missing: ${uid}"; done
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
  %w[kube-state-metrics kubelet-cadvisor].each do |job|
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

echo "ok kind-observability-live datasources=3 dashboards=2"
