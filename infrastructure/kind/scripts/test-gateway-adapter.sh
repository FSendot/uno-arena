#!/usr/bin/env bash
# Explicit live Gateway durable adapter acceptance against kind (/ready + rate-limit ping path).
# Kafka SessionInvalidated consumer and Debezium player-feed sink remain PENDING.
# Never applies/resets unrelated resources.
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

# shellcheck source=port-forward-gateway.sh
source "${SCRIPT_DIR}/port-forward-gateway.sh"

echo "Gateway base: ${GATEWAY_BASE_URL}"
echo "note: Kafka SessionInvalidated + Debezium player-feed sink remain PENDING"

ready=0
for _ in $(seq 1 90); do
  code="$(curl -sS -o /tmp/gateway-ready.json -w '%{http_code}' "${GATEWAY_BASE_URL}/ready" || true)"
  if [[ "${code}" == "200" ]]; then
    ready=1
    break
  fi
  sleep 1
done
[[ "${ready}" == "1" ]] || die "/ready never became 200 (last=$(cat /tmp/gateway-ready.json 2>/dev/null || true))"

# Health must stay up independently of upstream probe set.
health="$(curl -sS -o /tmp/gateway-health.json -w '%{http_code}' "${GATEWAY_BASE_URL}/health" || true)"
[[ "${health}" == "200" ]] || die "/health want 200 got ${health}"

echo "ok kind-test-gateway-adapter (Kafka/Debezium PENDING)"
