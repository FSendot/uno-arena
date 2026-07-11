#!/usr/bin/env bash
# Explicit live Ranking durable adapter acceptance against kind (structure + ready + games-results + outbox).
# CDC Kafka delivery and Redis projection remain later. Never applies/resets unrelated resources.
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

# shellcheck source=port-forward-ranking.sh
source "${SCRIPT_DIR}/port-forward-ranking.sh"

CRED="${RANKING_INTERNAL_CREDENTIAL:-local-room-to-ranking}"
GID="kind-adapter-$(date +%s)"

echo "Ranking base: ${RANKING_BASE_URL}"

ready=0
for _ in $(seq 1 60); do
  code="$(curl -sS -o /tmp/ranking-ready.json -w '%{http_code}' "${RANKING_BASE_URL}/ready" || true)"
  if [[ "${code}" == "200" ]]; then
    ready=1
    break
  fi
  sleep 1
done
[[ "${ready}" == "1" ]] || die "/ready never became 200 (last=$(cat /tmp/ranking-ready.json 2>/dev/null || true))"

body="$(cat <<EOF
{"commandId":"cmd-${GID}","eventId":"evt-${GID}","gameId":"${GID}","roomId":"r-${GID}","roomType":"ad_hoc","isAbandoned":false,"authoritative":true,"completed":true,"participants":[{"playerId":"p1-${GID}","placement":1},{"playerId":"p2-${GID}","placement":2}]}
EOF
)"

create="$(curl -sS -X POST "${RANKING_BASE_URL}/internal/v1/rankings/games-results" \
  -H "Content-Type: application/json" \
  -H "X-Service-Credential: ${CRED}" \
  -H "X-Correlation-Id: corr-${GID}" \
  -d "${body}")"
kind="$(jq -r '.kind // empty' <<<"${create}")"
[[ "${kind}" == "accepted" ]] || die "games-results failed: ${create}"

outbox_count="$(kubectl -n "${KIND_NAMESPACE}" exec deploy/postgres-ranking -- \
  psql -U ranking_admin -d ranking -tAc \
  "SELECT count(*) FROM outbox_events WHERE payload->>'gameId'='${GID}' OR partition_key LIKE 'p1-${GID}'" 2>/dev/null || echo 0)"
outbox_count="$(echo "${outbox_count}" | tr -d '[:space:]')"
[[ "${outbox_count}" =~ ^[0-9]+$ ]] || outbox_count=0
if (( outbox_count < 1 )); then
  die "expected >=1 outbox row for game ${GID}, got ${outbox_count}"
fi

# Exact eventId replay must stay duplicate without duplicating outbox.
replay="$(curl -sS -X POST "${RANKING_BASE_URL}/internal/v1/rankings/games-results" \
  -H "Content-Type: application/json" \
  -H "X-Service-Credential: ${CRED}" \
  -d "${body}")"
[[ "$(jq -r '.kind // empty' <<<"${replay}")" == "duplicate" ]] || die "replay failed: ${replay}"
outbox_count2="$(kubectl -n "${KIND_NAMESPACE}" exec deploy/postgres-ranking -- \
  psql -U ranking_admin -d ranking -tAc \
  "SELECT count(*) FROM outbox_events WHERE payload->>'gameId'='${GID}' OR partition_key LIKE 'p1-${GID}'" 2>/dev/null || echo 0)"
outbox_count2="$(echo "${outbox_count2}" | tr -d '[:space:]')"
[[ "${outbox_count2}" == "${outbox_count}" ]] || die "replay must not duplicate outbox (${outbox_count} -> ${outbox_count2})"

echo "ok kind-test-ranking-adapter game=${GID} outbox=${outbox_count}"
