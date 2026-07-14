#!/usr/bin/env bash
# EXPLICIT/NETWORKED live probe: insert one Identity outbox row and wait for the
# AsyncAPI topic identity.session.invalidated. Fail-closed. Disposable kind only.
# Does not prove consumer processing — only Connect Outbox Router → Kafka path.
# Asserts Kafka key == partition_key and canonical envelope fields on the matched record.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "${SCRIPT_DIR}/lib.sh"

require_cmd kubectl
require_cmd python3
assert_kind_context

die() {
  echo "FAIL: $*" >&2
  exit 1
}

EVENT_ID="kind-live-kafka-$(date +%s)-$$"
PARTITION_KEY="player-kind-live-${EVENT_ID}"
PLAYER_ID="${PARTITION_KEY}"
SESSION_ID="sess-kind-live-${EVENT_ID}"
CORR_ID="corr-kind-live-${EVENT_ID}"
OCCURRED_AT="2026-07-11T15:00:00Z"
TOPIC="identity.session.invalidated"
TIMEOUT_S="${KIND_CDC_LIVE_TIMEOUT_S:-90}"

echo "precondition: Connect connectors RUNNING"
"${SCRIPT_DIR}/test-debezium-connectors.sh" >/dev/null

echo "insert Identity outbox row event_id=${EVENT_ID} partition_key=${PARTITION_KEY}"
# -i forwards the heredoc to psql stdin; do not allocate a tty (-t).
kubectl -n "${KIND_NAMESPACE}" exec -i deploy/postgres-identity -- \
  env PGPASSWORD=local-only-identity-runtime \
  psql -v ON_ERROR_STOP=1 -U identity_runtime -d identity <<SQL
INSERT INTO outbox_events (
  event_id, event_type, topic, partition_key, schema_version, player_id, payload
) VALUES (
  '${EVENT_ID}',
  'SessionInvalidated',
  '${TOPIC}',
  '${PARTITION_KEY}',
  1,
  '${PLAYER_ID}',
  '{"schemaVersion":1,"eventId":"${EVENT_ID}","eventType":"SessionInvalidated","correlationId":"${CORR_ID}","occurredAt":"${OCCURRED_AT}","playerId":"${PLAYER_ID}","sessionId":"${SESSION_ID}","reason":"kind_live_probe"}'::jsonb
);
SQL

echo "poll Kafka topic ${TOPIC} for eventId=${EVENT_ID} (timeout=${TIMEOUT_S}s)"
deadline=$((SECONDS + TIMEOUT_S))
matched_raw=""
while (( SECONDS < deadline )); do
  # Print key+value so the matched record can assert partition_key as Kafka key.
  raw="$(
    kubectl -n "${KIND_NAMESPACE}" exec deploy/kafka -- \
      /opt/kafka/bin/kafka-console-consumer.sh \
        --bootstrap-server localhost:9092 \
        --topic "${TOPIC}" \
        --from-beginning \
        --timeout-ms 5000 \
        --max-messages 100 \
        --property print.key=true \
        --property key.separator=$'\t' \
        2>/dev/null || true
  )"
  if printf '%s' "${raw}" | grep -qF "${EVENT_ID}"; then
    matched_raw="${raw}"
    break
  fi
  sleep 2
done

[[ -n "${matched_raw}" ]] || die "event ${EVENT_ID} not observed on ${TOPIC} within ${TIMEOUT_S}s"

# Program on fd 3; captured Kafka lines on stdin (do not use python3 - + heredoc).
printf '%s' "${matched_raw}" | python3 /dev/fd/3 \
  "${EVENT_ID}" "${PARTITION_KEY}" "${CORR_ID}" "${OCCURRED_AT}" "${PLAYER_ID}" "${SESSION_ID}" \
  3<<'PY'
import json, sys

event_id, partition_key, corr_id, occurred_at, player_id, session_id = sys.argv[1:7]
lines = sys.stdin.read().splitlines()
matched = None
for line in lines:
    if event_id not in line:
        continue
    if "\t" not in line:
        raise SystemExit(f"matched line missing key/value separator: {line!r}")
    key, value = line.split("\t", 1)
    try:
        body = json.loads(value)
    except json.JSONDecodeError as exc:
        raise SystemExit(f"value is not JSON: {exc}: {value[:200]!r}") from exc
    matched = (key, body)
    break

if matched is None:
    raise SystemExit(f"no record containing eventId={event_id}")

key, body = matched
if key != partition_key:
    raise SystemExit(f"Kafka key={key!r} want partition_key={partition_key!r}")

required = {
    "schemaVersion": 1,
    "eventId": event_id,
    "eventType": "SessionInvalidated",
    "correlationId": corr_id,
    "occurredAt": occurred_at,
    "playerId": player_id,
    "sessionId": session_id,
}
for field, want in required.items():
    got = body.get(field)
    if got != want:
        raise SystemExit(f"field {field}={got!r} want {want!r} in {body}")

print("ok matched Kafka key + canonical envelope fields")
PY

echo "ok test-debezium-postgres-to-kafka-live (Postgres outbox → Kafka observed; consumer delivery is outside this producer-path probe)"
