#!/usr/bin/env bash
# Explicit live Tournament durable adapter acceptance against kind (ready + create + idempotency).
# CreateTournament is internal-only and intentionally emits no Kafka/outbox row.
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

# shellcheck source=port-forward-tournament-orchestration.sh
source "${SCRIPT_DIR}/port-forward-tournament-orchestration.sh"
trap cleanup EXIT

CRED="${TOURNAMENT_INTERNAL_CREDENTIAL:-local-room-to-tournament}"
TID="kind-adapter-$(date +%s)"

echo "Tournament base: ${TOURNAMENT_BASE_URL}"

ready=0
for _ in $(seq 1 60); do
  code="$(curl -sS -o /tmp/tournament-ready.json -w '%{http_code}' "${TOURNAMENT_BASE_URL}/ready" || true)"
  if [[ "${code}" == "200" ]]; then
    ready=1
    break
  fi
  sleep 1
done
[[ "${ready}" == "1" ]] || die "/ready never became 200 (last=$(cat /tmp/tournament-ready.json 2>/dev/null || true))"

create="$(curl -sS -X POST "${TOURNAMENT_BASE_URL}/internal/v1/commands" \
  -H "Content-Type: application/json" \
  -H "X-Service-Credential: ${CRED}" \
  -H "X-Correlation-Id: corr-${TID}" \
  -d "{\"commandId\":\"cmd-${TID}\",\"type\":\"CreateTournament\",\"schemaVersion\":1,\"payload\":{\"tournamentId\":\"${TID}\",\"capacity\":4},\"playerId\":\"op\",\"sessionId\":\"s\"}")"
status="$(jq -r '.status // empty' <<<"${create}")"
[[ "${status}" == "accepted" ]] || die "create tournament failed: ${create}"

# Durable aggregate and command outcome committed atomically. The internal-only
# create fact intentionally has no AsyncAPI topic and therefore no outbox row.
tournament_count="$(kubectl -n "${KIND_NAMESPACE}" exec deploy/postgres-tournament -- \
  psql -U tournament_admin -d tournament -tAc \
  "SELECT count(*) FROM tournaments WHERE tournament_id='${TID}' AND phase='registration' AND capacity=4" 2>/dev/null || echo 0)"
tournament_count="$(echo "${tournament_count}" | tr -d '[:space:]')"
[[ "${tournament_count}" == "1" ]] || die "expected one durable tournament ${TID}, got ${tournament_count}"
outbox_count="$(kubectl -n "${KIND_NAMESPACE}" exec deploy/postgres-tournament -- \
  psql -U tournament_admin -d tournament -tAc \
  "SELECT count(*) FROM outbox_events WHERE tournament_id='${TID}'" 2>/dev/null || echo 1)"
outbox_count="$(echo "${outbox_count}" | tr -d '[:space:]')"
[[ "${outbox_count}" == "0" ]] || die "CreateTournament must emit zero outbox rows, got ${outbox_count}"

# Exact replay must stay accepted without duplicating aggregate/idempotency rows.
replay="$(curl -sS -X POST "${TOURNAMENT_BASE_URL}/internal/v1/commands" \
  -H "Content-Type: application/json" \
  -H "X-Service-Credential: ${CRED}" \
  -d "{\"commandId\":\"cmd-${TID}\",\"type\":\"CreateTournament\",\"schemaVersion\":1,\"payload\":{\"tournamentId\":\"${TID}\",\"capacity\":4},\"playerId\":\"op\",\"sessionId\":\"s\"}")"
[[ "$(jq -r '.status // empty' <<<"${replay}")" == "accepted" ]] || die "replay failed: ${replay}"
idempotency_count="$(kubectl -n "${KIND_NAMESPACE}" exec deploy/postgres-tournament -- \
  psql -U tournament_admin -d tournament -tAc \
  "SELECT count(*) FROM command_idempotency WHERE command_id='cmd-${TID}' AND tournament_id='${TID}'" 2>/dev/null || echo 0)"
idempotency_count="$(echo "${idempotency_count}" | tr -d '[:space:]')"
[[ "${idempotency_count}" == "1" ]] || die "replay must keep one idempotency row, got ${idempotency_count}"

echo "ok kind-test-tournament-adapter tournament=${TID} idempotency=1 outbox=0"
