#!/usr/bin/env bash
# Offline structure checks for Analytics Kafka consumer + Kafka→ClickHouse live harness.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "${SCRIPT_DIR}/lib.sh"

fail=0
check() {
  local file="$1"
  local needle="$2"
  if ! grep -qF "${needle}" "${file}"; then
    echo "FAIL: ${file} missing ${needle}" >&2
    fail=1
  fi
}

SRC="${REPO_ROOT}/services/analytics/src"
LIVE="${SCRIPT_DIR}/test-analytics-kafka-to-clickhouse-live.sh"
STRUCT="${SCRIPT_DIR}/test-analytics-kafka-consumer-structure.sh"

for f in \
  "${SRC}/kafka_config.go" \
  "${SRC}/kafka_parse.go" \
  "${SRC}/kafka_consumer.go" \
  "${SRC}/kafka_franz.go" \
  "${SRC}/kafka_franz_test.go" \
  "${SRC}/kafka_parse_test.go" \
  "${LIVE}" \
  "${STRUCT}"; do
  [[ -f "${f}" ]] || { echo "FAIL: missing ${f}" >&2; fail=1; }
done

check "${SRC}/kafka_config.go" 'DefaultAnalyticsKafkaGroup'
check "${SRC}/kafka_config.go" 'analytics'
check "${SRC}/kafka_config.go" 'room.gameplay.metrics'
check "${SRC}/kafka_config.go" 'ranking.leaderboard_snapshot_published'
check "${SRC}/kafka_config.go" '.analytics.dlq'
check "${SRC}/kafka_consumer.go" 'topicPartition'
check "${SRC}/kafka_consumer.go" 'OutcomeIgnored'
check "${SRC}/kafka_franz.go" 'franz-go v1.21.5'
check "${SRC}/kafka_franz.go" 'DisableAutoCommit'
check "${SRC}/kafka_franz.go" 'ReadCommitted'
check "${SRC}/kafka_parse.go" 'ParseAnalyticsRecord'
check "${SRC}/kafka_parse.go" 'DurableIgnore'

check "${LIVE}" "room.gameplay.metrics"
check "${LIVE}" "processed_events"
check "${LIVE}" "assert_kind_context"
check "${LIVE}" "kafka-console-producer"
check "${LIVE}" "Kafka→ClickHouse"
# Refuse over-claim wording that implies HTTP/SSE product E2E.
if grep -qiE 'SSE stream|end-to-end product|full e2e' "${LIVE}"; then
  echo "FAIL: live script must not claim SSE/product E2E beyond Kafka→CH" >&2
  fail=1
fi

KIND_VALUES="${REPO_ROOT}/services/analytics/helm/analytics/values.kind.yaml"
check "${KIND_VALUES}" "KAFKA_BROKERS"
check "${KIND_VALUES}" "kafka.uno-arena.svc.cluster.local:9092"
check "${KIND_VALUES}" "KAFKA_CONSUMER_GROUP"
check "${KIND_VALUES}" "KAFKA_TOPICS"
if grep -qiE 'kafka.*(pending|intentionally omitted)|pending.*kafka' "${KIND_VALUES}"; then
  echo "FAIL: values.kind.yaml must not say Kafka pending/omitted" >&2
  fail=1
fi

[[ "${fail}" -eq 0 ]] || exit 1
echo "ok analytics-kafka-consumer-structure"
