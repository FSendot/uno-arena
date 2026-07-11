#!/usr/bin/env bash
# Explicit live Tournament durable adapter acceptance against kind (structure + ready + create + outbox row).
# CDC Kafka delivery remains later. Never applies/resets unrelated resources.
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

# Outbox committed for create (CDC delivery later).
outbox_count="$(kubectl -n "${KIND_NAMESPACE}" exec deploy/postgres-tournament -- \
  psql -U tournament_admin -d tournament -tAc \
  "SELECT count(*) FROM outbox_events WHERE tournament_id='${TID}'" 2>/dev/null || echo 0)"
outbox_count="$(echo "${outbox_count}" | tr -d '[:space:]')"
[[ "${outbox_count}" =~ ^[0-9]+$ ]] || outbox_count=0
if (( outbox_count < 1 )); then
  die "expected >=1 outbox row for tournament ${TID}, got ${outbox_count}"
fi

# Exact replay must stay accepted without duplicating outbox.
replay="$(curl -sS -X POST "${TOURNAMENT_BASE_URL}/internal/v1/commands" \
  -H "Content-Type: application/json" \
  -H "X-Service-Credential: ${CRED}" \
  -d "{\"commandId\":\"cmd-${TID}\",\"type\":\"CreateTournament\",\"schemaVersion\":1,\"payload\":{\"tournamentId\":\"${TID}\",\"capacity\":4},\"playerId\":\"op\",\"sessionId\":\"s\"}")"
[[ "$(jq -r '.status // empty' <<<"${replay}")" == "accepted" ]] || die "replay failed: ${replay}"
outbox_count2="$(kubectl -n "${KIND_NAMESPACE}" exec deploy/postgres-tournament -- \
  psql -U tournament_admin -d tournament -tAc \
  "SELECT count(*) FROM outbox_events WHERE tournament_id='${TID}'" 2>/dev/null || echo 0)"
outbox_count2="$(echo "${outbox_count2}" | tr -d '[:space:]')"
[[ "${outbox_count2}" == "${outbox_count}" ]] || die "replay must not duplicate outbox (${outbox_count} -> ${outbox_count2})"

echo "ok kind-test-tournament-adapter tournament=${TID} outbox=${outbox_count}"
