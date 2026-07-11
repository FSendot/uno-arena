#!/usr/bin/env bash
# Explicit live Identity durable adapter acceptance against kind.
# Proves: login -> authoritative validate -> second login invalidates first + outbox row;
# optional pod restart persistence. CDC Kafka delivery remains later.
# Never applies/resets unrelated resources. Uses isolated port-forward (local port 0).
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

# shellcheck source=port-forward-identity.sh
# Parent owns cleanup of PF child.
source "${SCRIPT_DIR}/port-forward-identity.sh"

CRED="${IDENTITY_INTERNAL_CREDENTIAL:-local-gw-backend}"
USER="${IDENTITY_TEST_USER:-test-player}"
PASS="${IDENTITY_TEST_PASSWORD:-local-only-test-password}"

echo "Identity base: ${IDENTITY_BASE_URL}"

ready=0
for _ in $(seq 1 60); do
  code="$(curl -sS -o /tmp/identity-ready.json -w '%{http_code}' "${IDENTITY_BASE_URL}/ready" || true)"
  if [[ "${code}" == "200" ]]; then
    ready=1
    break
  fi
  sleep 1
done
[[ "${ready}" == "1" ]] || die "/ready never became 200 (last=$(cat /tmp/identity-ready.json 2>/dev/null || true))"

login1="$(curl -sS -X POST "${IDENTITY_BASE_URL}/v1/auth/login" \
  -H 'Content-Type: application/json' \
  -d "{\"username\":\"${USER}\",\"password\":\"${PASS}\"}")"
token1="$(jq -r '.token // empty' <<<"${login1}")"
sid1="$(jq -r '.sessionId // empty' <<<"${login1}")"
pid1="$(jq -r '.playerId // empty' <<<"${login1}")"
[[ -n "${token1}" && -n "${sid1}" && -n "${pid1}" ]] || die "first login failed: ${login1}"

val1="$(curl -sS -o /tmp/identity-val1.json -w '%{http_code}' -X POST "${IDENTITY_BASE_URL}/internal/v1/sessions/validate" \
  -H "X-Service-Credential: ${CRED}" \
  -H "Authorization: Bearer ${token1}")"
[[ "${val1}" == "200" ]] || die "authoritative validate expected 200, got ${val1} $(cat /tmp/identity-val1.json)"

login2="$(curl -sS -X POST "${IDENTITY_BASE_URL}/v1/auth/login" \
  -H 'Content-Type: application/json' \
  -d "{\"username\":\"${USER}\",\"password\":\"${PASS}\"}")"
token2="$(jq -r '.token // empty' <<<"${login2}")"
sid2="$(jq -r '.sessionId // empty' <<<"${login2}")"
[[ -n "${token2}" && -n "${sid2}" ]] || die "second login failed: ${login2}"
[[ "${sid2}" != "${sid1}" ]] || die "second login must create a new sessionId"

val_stale="$(curl -sS -o /tmp/identity-val-stale.json -w '%{http_code}' -X POST "${IDENTITY_BASE_URL}/internal/v1/sessions/validate" \
  -H "X-Service-Credential: ${CRED}" \
  -H "Authorization: Bearer ${token1}")"
[[ "${val_stale}" == "401" ]] || die "stale token after takeover expected 401, got ${val_stale}"

val2="$(curl -sS -o /tmp/identity-val2.json -w '%{http_code}' -X POST "${IDENTITY_BASE_URL}/internal/v1/sessions/validate" \
  -H "X-Service-Credential: ${CRED}" \
  -H "Authorization: Bearer ${token2}")"
[[ "${val2}" == "200" ]] || die "second token validate expected 200, got ${val2}"

# Outbox committed for first session invalidation (CDC delivery later).
# Use the Postgres pod's admin role (peer auth / container env).
outbox_count="$(kubectl -n "${KIND_NAMESPACE}" exec deploy/postgres-identity -- \
  psql -U identity_admin -d identity -tAc \
  "SELECT count(*) FROM outbox_events WHERE event_type='SessionInvalidated' AND partition_key='${pid1}'" 2>/dev/null || echo 0)"
outbox_count="$(echo "${outbox_count}" | tr -d '[:space:]')"
[[ "${outbox_count}" =~ ^[0-9]+$ ]] || outbox_count=0
if (( outbox_count < 1 )); then
  die "expected >=1 SessionInvalidated outbox row for player ${pid1}, got ${outbox_count}"
fi
echo "outbox SessionInvalidated rows for player=${pid1}: ${outbox_count}"

# Process/pod restart persistence: restart Identity, re-validate second token.
kubectl -n "${KIND_NAMESPACE}" rollout restart "deployment/identity"
kubectl -n "${KIND_NAMESPACE}" rollout status "deployment/identity" --timeout=180s

# Re-establish port-forward after restart (old PF may die).
cleanup
PF_PID=""
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/port-forward-identity.sh"

ready=0
for _ in $(seq 1 60); do
  code="$(curl -sS -o /dev/null -w '%{http_code}' "${IDENTITY_BASE_URL}/ready" || true)"
  if [[ "${code}" == "200" ]]; then ready=1; break; fi
  sleep 1
done
[[ "${ready}" == "1" ]] || die "/ready failed after restart"

val_after="$(curl -sS -o /tmp/identity-val-after.json -w '%{http_code}' -X POST "${IDENTITY_BASE_URL}/internal/v1/sessions/validate" \
  -H "X-Service-Credential: ${CRED}" \
  -H "Authorization: Bearer ${token2}")"
[[ "${val_after}" == "200" ]] || die "session must persist across restart, got ${val_after}"

echo "ok kind-test-identity-adapter"
