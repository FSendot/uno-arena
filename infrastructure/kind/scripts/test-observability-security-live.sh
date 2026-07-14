#!/usr/bin/env bash
# Explicit allow/deny proof for application metrics and OTLP ingress.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "${SCRIPT_DIR}/lib.sh"
# shellcheck source=observability-acceptance-lib.sh
source "${SCRIPT_DIR}/observability-acceptance-lib.sh"
require_cmd curl
require_cmd kubectl
require_cmd ruby
assert_kind_context
"${SCRIPT_DIR}/wait-observability.sh"

PROM_PORT="${PROMETHEUS_LOCAL_PORT:-19090}"
obs_forward observability service/prometheus "${PROM_PORT}" 9090
trap obs_cleanup_forwards EXIT INT TERM
PROM="http://127.0.0.1:${PROM_PORT}"
obs_wait_http "${PROM}/-/ready"

targets="$(curl -fsS "${PROM}/api/v1/targets?state=active")"
printf '%s' "${targets}" | ruby -rjson -e '
  body=JSON.parse(STDIN.read); targets=body.dig("data","activeTargets") || []
  apps=targets.select { |t| %w[uno-arena-services uno-arena-workers].include?(t.dig("labels","job")) }
  abort "no application metrics targets discovered" if apps.empty?
  down=apps.reject { |t| t["health"] == "up" }
  abort "application metrics scrape is not authorized/up: #{down}" unless down.empty?
  abort "identity metrics target missing" unless apps.any? { |t| t.dig("labels","service") == "identity" }
'

deny_from_postgres() {
  local url="$1" label="$2"
  if kubectl -n "${KIND_NAMESPACE}" exec deploy/postgres-identity -- sh -c \
      "wget -q -T 4 -O /dev/null '${url}'" >/dev/null 2>&1; then
    die "${label} unexpectedly succeeded"
  fi
}
# The same pod can reach an ordinary application port. This prevents a broken
# source pod or DNS path from being mistaken for a policy denial below.
kubectl -n "${KIND_NAMESPACE}" exec deploy/postgres-identity -- sh -c \
  "wget -q -T 4 -O /dev/null 'http://identity:8080/health'" >/dev/null 2>&1 || \
  die "unauthorized-source control path to Identity health is not reachable"
deny_from_postgres 'http://identity:9090/metrics' 'unauthorized application metrics'
if kubectl -n "${KIND_NAMESPACE}" exec deploy/postgres-identity -- sh -c \
    "wget -q -T 4 --header='Content-Type: application/json' --post-data='{\"resourceSpans\":[]}' -O /dev/null 'http://alloy-otlp.observability.svc.cluster.local:4318/v1/traces'" \
    >/dev/null 2>&1; then
  die "unauthorized OTLP export unexpectedly succeeded"
fi

echo "ok kind-observability-security authorized=prometheus unauthorized=postgres-identity/default"
