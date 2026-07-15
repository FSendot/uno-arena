#!/usr/bin/env bash
# Offline contract for the kind-native live client parity acceptance lane.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
RUNNER="${REPO_ROOT}/client-checkpoint/tests/run-live-client-parity.sh"
WRAPPER="${SCRIPT_DIR}/test-client-parity-live.sh"
PROBES="${SCRIPT_DIR}/run-live-probes.sh"

die() { echo "FAIL: $*" >&2; exit 1; }

for file in "${RUNNER}" "${WRAPPER}" "${PROBES}"; do
  [[ -f "${file}" ]] || die "missing ${file}"
  bash -n "${file}"
done

grep -Fq 'UNOARENA_API_URL is required' "${RUNNER}" || die "external runner must require caller Gateway URL"
if grep -Eq 'capability-compose|docker compose|CAP_SKIP_UP|CAP_TEARDOWN_EXTERNAL' "${RUNNER}"; then
  die "external runner must not contain Docker Compose lifecycle assumptions"
fi

grep -Fq 'CAP_ROOM_READY_TIMEOUT_S' "${RUNNER}" || die "first Room runtime readiness gate must be bounded"
grep -Fq 'CAP_SPECTATOR_PROJECTION_TIMEOUT_S' "${RUNNER}" || die "spectator projection wait must be bounded"
grep -Fq 'wait_spectator_snapshot_status' "${RUNNER}" || die "live parity must wait for asynchronous spectator progress"
grep -Fq 'wait_http_status "term spectator snapshot 403"' "${RUNNER}" || \
  die "terminal spectator denial must wait for asynchronous projection closure"
grep -Fq 'int(data.get("sequence", -1)) >= min_sequence' "${RUNNER}" || \
  die "spectator projection wait must gate on committed Room sequence"
grep -Fq 'never retry the mutation' "${RUNNER}" || \
  die "spectator projection wait must preserve mutation single-attempt semantics"
grep -Fq 'capability_http_code -H "Authorization: Bearer ${token}"' "${RUNNER}" || \
  die "Room readiness gate must use an authenticated safe read"
create_line="$(grep -n -m1 'capability_assert_accepted "CreateRoom accepted seq1"' "${RUNNER}" | cut -d: -f1)"
gate_line="$(grep -n -m1 '"host player snapshot readiness before first JoinRoom"' "${RUNNER}" | cut -d: -f1)"
join_line="$(grep -n -m1 'must_cli_json "JoinRoom"' "${RUNNER}" | cut -d: -f1)"
[[ -n "${create_line}" && -n "${gate_line}" && -n "${join_line}" ]] || \
  die "missing CreateRoom/readiness/JoinRoom ordering evidence"
(( create_line < gate_line && gate_line < join_line )) || \
  die "authenticated safe-read gate must run after CreateRoom and before first JoinRoom"
grep -Fq 'This mutation is attempted exactly once after the safe-read readiness gate.' "${RUNNER}" || \
  die "first JoinRoom must remain a single mutation attempt"
cancel_create_line="$(grep -n -m1 'capability_assert_accepted "CreateRoom term"' "${RUNNER}" | cut -d: -f1)"
cancel_gate_line="$(grep -n -m1 '"host player snapshot readiness before CancelRoom"' "${RUNNER}" | cut -d: -f1)"
cancel_line="$(grep -n -m1 'must_cli_json "CancelRoom"' "${RUNNER}" | cut -d: -f1)"
[[ -n "${cancel_create_line}" && -n "${cancel_gate_line}" && -n "${cancel_line}" ]] || \
  die "missing terminal CreateRoom/readiness/CancelRoom ordering evidence"
(( cancel_create_line < cancel_gate_line && cancel_gate_line < cancel_line )) || \
  die "authenticated safe-read gate must run after terminal CreateRoom and before CancelRoom"

for evidence in \
  'session_invalidated' \
  'projection_updated' \
  'CloseRegistration idempotent replay equal' \
  'leaderboard+analytics vs fixture' \
  'snapshot_required' \
  'term spectator snapshot 403' \
  'term spectator stream HTTP 403'; do
  grep -Fq "${evidence}" "${RUNNER}" || die "client parity evidence missing: ${evidence}"
done

grep -Fq 'source "${SCRIPT_DIR}/port-forward-gateway.sh"' "${WRAPPER}" || die "kind wrapper must use isolated Gateway port-forward"
grep -Fq 'export UNOARENA_API_URL="${GATEWAY_BASE_URL}"' "${WRAPPER}" || die "kind wrapper must pass port-forward URL to external runner"
grep -Fq 'bash "${PARITY_RUNNER}"' "${WRAPPER}" || die "kind wrapper must invoke external runner"
grep -Fq 'test-client-parity-live.sh' "${PROBES}" || die "clean live-probe lane must run client parity"

echo "ok kind-test-client-parity-structure"
