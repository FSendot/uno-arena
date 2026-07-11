#!/usr/bin/env bash
# Explicit live Spectator durable adapter acceptance against kind (ready + ingest + replay).
# Kafka consumer remains PENDING. HTTP internal ingest is the test/ops bridge.
# Spectator pod replacement can retain Redis projection while Redis stays up; that does
# not prove Redis process durability. Same-pod Redis PID 1 restart durability is covered
# by kind Redis AOF (`make kind-test-redis-aof`), not by this adapter. Redis remains
# disposable/not authoritative (pod replace / kind reset may require Kafka rebuild).
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

# shellcheck source=port-forward-spectator-view.sh
source "${SCRIPT_DIR}/port-forward-spectator-view.sh"

CRED="${SPECTATOR_VIEW_INTERNAL_CREDENTIAL:-local-room-to-spectator}"
GID="kind-adapter-$(date +%s)"
ROOM="room-${GID}"

echo "Spectator base: ${SPECTATOR_BASE_URL}"
echo "note: Kafka consumer for room.spectator-safe.events remains PENDING"

ready=0
for _ in $(seq 1 60); do
  code="$(curl -sS -o /tmp/spectator-ready.json -w '%{http_code}' "${SPECTATOR_BASE_URL}/ready" || true)"
  if [[ "${code}" == "200" ]]; then
    ready=1
    break
  fi
  sleep 1
done
[[ "${ready}" == "1" ]] || die "/ready never became 200 (last=$(cat /tmp/spectator-ready.json 2>/dev/null || true))"

mode="$(jq -r '.mode // empty' </tmp/spectator-ready.json)"
[[ "${mode}" == "durable" ]] || die "expected mode=durable, got ${mode}"

body="$(cat <<EOF
{"schemaVersion":1,"eventId":"evt-${GID}","roomId":"${ROOM}","sequence":1,"event":"RoomCreated","data":{"visibility":"public","status":"waiting","seats":[{"seatIndex":0,"playerId":"p1","displayName":"Alice","cardCount":0}]}}
EOF
)"

create="$(curl -sS -X POST "${SPECTATOR_BASE_URL}/internal/v1/spectator/rooms/${ROOM}/events" \
  -H "Content-Type: application/json" \
  -H "X-Service-Credential: ${CRED}" \
  -H "X-Correlation-Id: corr-${GID}" \
  -d "${body}")"
kind="$(jq -r '.kind // empty' <<<"${create}")"
[[ "${kind}" == "accepted" ]] || die "ingest failed: ${create}"

replay="$(curl -sS -X POST "${SPECTATOR_BASE_URL}/internal/v1/spectator/rooms/${ROOM}/events" \
  -H "Content-Type: application/json" \
  -H "X-Service-Credential: ${CRED}" \
  -d "${body}")"
[[ "$(jq -r '.kind // empty' <<<"${replay}")" == "duplicate" ]] || die "replay failed: ${replay}"

snap="$(curl -sS "${SPECTATOR_BASE_URL}/v1/spectator/rooms/${ROOM}/snapshot")"
seq="$(jq -r '.sequence // empty' <<<"${snap}")"
[[ "${seq}" == "1" ]] || die "snapshot sequence want 1 got ${seq}: ${snap}"

echo "ok kind-test-spectator-adapter room=${ROOM} (Kafka PENDING)"
