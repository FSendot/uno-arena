#!/usr/bin/env bash
# EXPLICIT/NETWORKED: produce one GameplayMetric to Kafka and observe ClickHouse ingest.
# Proves Analytics Kafka→ClickHouse durable path only. Does not claim SSE or broader E2E.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "${SCRIPT_DIR}/lib.sh"

require_cmd kubectl
assert_kind_context

TOPIC="room.gameplay.metrics"
EVENT_ID="kind-analytics-kafka-$(date +%s)-$$"
ROOM_ID="room-${EVENT_ID}"
CORR_ID="corr-${EVENT_ID}"
OCCURRED_AT="2026-07-11T15:00:00Z"
TIMEOUT_S="${KIND_ANALYTICS_KAFKA_LIVE_TIMEOUT_S:-90}"

echo "precondition: analytics + kafka + clickhouse deployments exist"
kubectl -n "${KIND_NAMESPACE}" get deploy/analytics >/dev/null 2>&1 || die "deploy/analytics not found; deploy analytics first"
kubectl -n "${KIND_NAMESPACE}" get deploy/kafka >/dev/null 2>&1 || die "deploy/kafka not found; kind cluster incomplete"
kubectl -n "${KIND_NAMESPACE}" get deploy/clickhouse >/dev/null 2>&1 || die "deploy/clickhouse not found; kind cluster incomplete"

PAYLOAD="$(cat <<EOF
{"schemaVersion":1,"eventId":"${EVENT_ID}","eventType":"GameplayMetric","correlationId":"${CORR_ID}","occurredAt":"${OCCURRED_AT}","roomId":"${ROOM_ID}","visibility":"anonymized_adhoc","metricType":"turn_advanced"}
EOF
)"

echo "produce ${EVENT_ID} to ${TOPIC} key=${ROOM_ID}"
printf '%s\t%s\n' "${ROOM_ID}" "${PAYLOAD}" | kubectl -n "${KIND_NAMESPACE}" exec -i deploy/kafka -- \
  /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server localhost:9092 \
    --topic "${TOPIC}" \
    --property parse.key=true \
    --property key.separator=$'\t' \
  >/dev/null

echo "poll ClickHouse processed_events for event_id=${EVENT_ID} (timeout=${TIMEOUT_S}s)"
deadline=$((SECONDS + TIMEOUT_S))
proc_count=0
while (( SECONDS < deadline )); do
  proc_count="$(kubectl -n "${KIND_NAMESPACE}" exec deploy/clickhouse -- \
    clickhouse-client --user analytics_runtime --password local-only-analytics-runtime \
    --query "SELECT count() FROM analytics.processed_events FINAL WHERE event_id='${EVENT_ID}'" 2>/dev/null || echo 0)"
  proc_count="$(echo "${proc_count}" | tr -d '[:space:]')"
  [[ "${proc_count}" =~ ^[0-9]+$ ]] || proc_count=0
  if (( proc_count >= 1 )); then
    break
  fi
  sleep 2
done

(( proc_count >= 1 )) || die "event ${EVENT_ID} not observed in analytics.processed_events within ${TIMEOUT_S}s (count=${proc_count})"

gameplay_count="$(kubectl -n "${KIND_NAMESPACE}" exec deploy/clickhouse -- \
  clickhouse-client --user analytics_runtime --password local-only-analytics-runtime \
  --query "SELECT count() FROM analytics.gameplay_metrics FINAL WHERE event_id='${EVENT_ID}'" 2>/dev/null || echo 0)"
gameplay_count="$(echo "${gameplay_count}" | tr -d '[:space:]')"
[[ "${gameplay_count}" =~ ^[0-9]+$ ]] || gameplay_count=0
(( gameplay_count >= 1 )) || die "event ${EVENT_ID} missing from gameplay_metrics (count=${gameplay_count})"

echo "ok kind-test-analytics-kafka-to-clickhouse-live event=${EVENT_ID} processed=${proc_count} gameplay=${gameplay_count}"
