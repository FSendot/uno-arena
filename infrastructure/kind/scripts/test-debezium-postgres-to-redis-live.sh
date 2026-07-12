#!/usr/bin/env bash
# EXPLICIT/NETWORKED live probe: insert one Room realtime_outbox_events row and
# wait for the Redis stream entry (DB 2, extended format). Fail-closed.
# Disposable kind only. Does not prove Gateway SSE end-to-end — only
# Debezium Server Outbox Router → Redis path.
# Uses a unique room/stream + sequence per run; validates extended entry JSON;
# deletes only that unique test stream on success.
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

RUN_ID="$(date +%s)-$$"
EVENT_ID="kind-live-redis-${RUN_ID}"
ROOM_ID="room-kind-live-${RUN_ID}"
STREAM="room:${ROOM_ID}:player"
PLAYER_ID="player-kind-live-${RUN_ID}"
SESSION_ID="sess-kind-live-${RUN_ID}"
SEQ="$((100000 + $$ % 899999))"
CORR_ID="corr-kind-live-${RUN_ID}"
CAUSATION_ID="cmd-kind-live-${RUN_ID}"
TIMEOUT_S="${KIND_CDC_LIVE_TIMEOUT_S:-90}"

cleanup() {
  kubectl -n "${KIND_NAMESPACE}" exec deploy/redis -- \
    redis-cli -n 2 DEL "${STREAM}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

echo "precondition: Debezium Server deployment ready"
kubectl -n "${KIND_NAMESPACE}" rollout status deployment/debezium-server-room-realtime --timeout=120s
"${SCRIPT_DIR}/status-debezium-server.sh" >/dev/null

echo "insert Room realtime outbox row event_id=${EVENT_ID} stream=${STREAM} seq=${SEQ}"
# -i forwards the heredoc to psql stdin; do not allocate a tty (-t).
kubectl -n "${KIND_NAMESPACE}" exec -i deploy/postgres-room-gameplay -- \
  env PGPASSWORD=local-only-room-runtime \
  psql -v ON_ERROR_STOP=1 -U room_runtime -d room_gameplay <<SQL
INSERT INTO realtime_outbox_events (
  event_id, event_type, topic, target_stream, partition_key, schema_version,
  room_id, player_id, session_id, sequence_number, payload, correlation_id, causation_id, occurred_at
) VALUES (
  '${EVENT_ID}',
  'CardPlayed',
  '',
  '${STREAM}',
  '${ROOM_ID}',
  1,
  '${ROOM_ID}',
  '${PLAYER_ID}',
  '${SESSION_ID}',
  ${SEQ},
  '{"roomId":"${ROOM_ID}","fact":"CardPlayed","sequence":${SEQ},"data":{"card":"public"}}'::jsonb,
  '${CORR_ID}',
  '${CAUSATION_ID}',
  now()
);
SQL

echo "poll Redis DB2 stream ${STREAM} for eventId=${EVENT_ID} (timeout=${TIMEOUT_S}s)"
deadline=$((SECONDS + TIMEOUT_S))
matched_raw=""
while (( SECONDS < deadline )); do
  raw="$(
    kubectl -n "${KIND_NAMESPACE}" exec deploy/redis -- \
      redis-cli -n 2 XRANGE "${STREAM}" - + COUNT 50 2>/dev/null || true
  )"
  if printf '%s' "${raw}" | grep -qF "${EVENT_ID}"; then
    matched_raw="${raw}"
    break
  fi
  sleep 2
done

[[ -n "${matched_raw}" ]] || die "event ${EVENT_ID} not observed on Redis stream ${STREAM} within ${TIMEOUT_S}s"

# Program on fd 3; captured Redis XRANGE on stdin (do not use python3 - + heredoc).
printf '%s' "${matched_raw}" | python3 /dev/fd/3 \
  "${EVENT_ID}" "${STREAM}" "${ROOM_ID}" "${PLAYER_ID}" "${SESSION_ID}" "${SEQ}" \
  3<<'PY'
import json, sys

event_id, stream, room_id, player_id, session_id, seq_s = sys.argv[1:7]
seq = int(seq_s)
text = sys.stdin.read()
if event_id not in text:
    raise SystemExit(f"eventId {event_id} missing from XRANGE output")

# Extended Redis Streams entries expose a JSON value field. Locate the first
# JSON object that contains our eventId (envelope aliases + default payload).
candidates = []
decoder = json.JSONDecoder()
i = 0
while i < len(text):
    if text[i] != "{":
        i += 1
        continue
    try:
        obj, end = decoder.raw_decode(text, i)
    except json.JSONDecodeError:
        i += 1
        continue
    if isinstance(obj, dict) and event_id in json.dumps(obj):
        candidates.append(obj)
    i = end

if not candidates:
    raise SystemExit(f"no JSON value containing eventId={event_id} in extended entry")

matched = candidates[0]
required_aliases = {
    "eventId": event_id,
    "event": "CardPlayed",
    "schemaVersion": 1,
    "sequence": seq,
    "playerId": player_id,
    "sessionId": session_id,
}
for field, want in required_aliases.items():
    if field not in matched:
        raise SystemExit(f"missing envelope alias {field} in {matched}")
    got = matched[field]
    if field == "schemaVersion" and got in (1, "1"):
        continue
    if field == "sequence" and got in (seq, str(seq)):
        continue
    if got != want:
        raise SystemExit(f"field {field}={got!r} want {want!r}")

# Default routed payload must remain available as payload (not only aliases).
payload = matched.get("payload")
if payload is None:
    raise SystemExit(f"default routed payload missing in extended value: {matched}")
if isinstance(payload, str):
    try:
        payload = json.loads(payload)
    except json.JSONDecodeError as exc:
        raise SystemExit(f"payload string is not JSON: {exc}") from exc
if not isinstance(payload, dict):
    raise SystemExit(f"payload must be object, got {type(payload).__name__}: {payload!r}")
if payload.get("roomId") != room_id:
    raise SystemExit(f"payload.roomId={payload.get('roomId')!r} want {room_id!r}")
if payload.get("fact") != "CardPlayed":
    raise SystemExit(f"payload.fact={payload.get('fact')!r}")
if payload.get("sequence") not in (seq, str(seq)):
    raise SystemExit(f"payload.sequence={payload.get('sequence')!r} want {seq}")

print(f"ok matched Redis extended entry on {stream}")
PY

echo "ok test-debezium-postgres-to-redis-live (Postgres realtime outbox → Redis observed; Gateway SSE E2E not claimed)"
