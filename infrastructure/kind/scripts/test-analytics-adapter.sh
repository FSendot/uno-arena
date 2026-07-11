#!/usr/bin/env bash
# Explicit live Analytics durable adapter acceptance against kind (structure + ready + ingest + dedupe).
# Kafka ingestion remains later. Never applies/resets unrelated resources.
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

# shellcheck source=port-forward-analytics.sh
source "${SCRIPT_DIR}/port-forward-analytics.sh"

ROOM_CRED="${ANALYTICS_ROOM_CREDENTIAL:-local-analytics-room}"
GID="kind-adapter-$(date +%s)"

echo "Analytics base: ${ANALYTICS_BASE_URL}"

ready=0
for _ in $(seq 1 60); do
  code="$(curl -sS -o /tmp/analytics-ready.json -w '%{http_code}' "${ANALYTICS_BASE_URL}/ready" || true)"
  if [[ "${code}" == "200" ]]; then
    ready=1
    break
  fi
  sleep 1
done
[[ "${ready}" == "1" ]] || die "/ready never became 200 (last=$(cat /tmp/analytics-ready.json 2>/dev/null || true))"

body="$(cat <<EOF
{"eventId":"evt-${GID}","eventType":"GameplayMetric","schemaVersion":1,"correlationId":"corr-${GID}","occurredAt":"2026-07-10T12:00:00Z","payload":{"visibility":"anonymized_adhoc","metricType":"turn_advanced","roomId":"r-${GID}"}}
EOF
)"

create="$(curl -sS -X POST "${ANALYTICS_BASE_URL}/internal/v1/analytics/room/events" \
  -H "Content-Type: application/json" \
  -H "X-Service-Credential: ${ROOM_CRED}" \
  -H "X-Correlation-Id: corr-${GID}" \
  -d "${body}")"
kind="$(jq -r '.kind // empty' <<<"${create}")"
[[ "${kind}" == "accepted" ]] || die "room ingest failed: ${create}"

# Exact eventId replay must stay duplicate without new visible rows.
replay="$(curl -sS -X POST "${ANALYTICS_BASE_URL}/internal/v1/analytics/room/events" \
  -H "Content-Type: application/json" \
  -H "X-Service-Credential: ${ROOM_CRED}" \
  -d "${body}")"
[[ "$(jq -r '.kind // empty' <<<"${replay}")" == "duplicate" ]] || die "replay failed: ${replay}"

snap="$(curl -sS "${ANALYTICS_BASE_URL}/v1/analytics/public")"
count="$(jq -r --arg eid "evt-${GID}" '[.gameplayMetrics[]? | select(.eventId==$eid)] | length' <<<"${snap}")"
[[ "${count}" == "1" ]] || die "want 1 visible gameplay row for evt-${GID}, got ${count}: ${snap}"

# Confirm durable processed_events marker exists in ClickHouse analytics DB.
proc_count="$(kubectl -n "${KIND_NAMESPACE}" exec deploy/clickhouse -- \
  clickhouse-client --user analytics_runtime --password local-only-analytics-runtime \
  --query "SELECT count() FROM analytics.processed_events FINAL WHERE event_id='evt-${GID}'" 2>/dev/null || echo 0)"
proc_count="$(echo "${proc_count}" | tr -d '[:space:]')"
[[ "${proc_count}" =~ ^[0-9]+$ ]] || proc_count=0
if (( proc_count < 1 )); then
  die "expected >=1 processed_events row for evt-${GID}, got ${proc_count}"
fi

echo "ok kind-test-analytics-adapter event=evt-${GID} processed=${proc_count}"
