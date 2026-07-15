#!/usr/bin/env bash
# Live client parity acceptance against a caller-provided external Gateway URL.
# Speaks only to the CLI/BFF external edge. Requires bash, curl, and python3.
#
# Usage:
#   UNOARENA_API_URL=http://127.0.0.1:8080 \
#     ./client-checkpoint/tests/run-live-client-parity.sh
#
# Env:
#   UNOARENA_API_URL         required external Gateway base URL; never overwritten
#   CAP_READY_TIMEOUT_S      readiness wait (default 180)
#   CAP_ROOM_READY_TIMEOUT_S first dedicated Room runtime wait (defaults to readiness wait)
#   CAP_SPECTATOR_PROJECTION_TIMEOUT_S async spectator projection wait (default 60)
set -euo pipefail

TESTS_DIR="$(cd "$(dirname "$0")" && pwd)"
CLIENT_DIR="$(cd "$TESTS_DIR/.." && pwd)"
CLI="${CLIENT_DIR}/bin/unoarena"

# shellcheck source=lib/capability-assert.sh
. "$TESTS_DIR/lib/capability-assert.sh"
# shellcheck source=lib/capability-fixtures.sh
. "$TESTS_DIR/lib/capability-fixtures.sh"

capability_require_cmds
chmod +x "$CLI"

if [ -z "${UNOARENA_API_URL:-}" ]; then
  echo "FAIL: UNOARENA_API_URL is required for live client parity acceptance" >&2
  exit 1
fi
# Preserve the caller's environment exactly; normalize only this local join base.
BASE="${UNOARENA_API_URL%/}"

CAP_TMPDIR="$(mktemp -d "${TMPDIR:-/tmp}/uno-cap.XXXXXX")"
CAP_HTTP_BODY_FILE="$CAP_TMPDIR/body.json"
RUN_ID="$(capability_unique_run_id)"
READY_TIMEOUT_S="${CAP_READY_TIMEOUT_S:-180}"
ROOM_READY_TIMEOUT_S="${CAP_ROOM_READY_TIMEOUT_S:-$READY_TIMEOUT_S}"
SPECTATOR_PROJECTION_TIMEOUT_S="${CAP_SPECTATOR_PROJECTION_TIMEOUT_S:-60}"
export CAP_TMPDIR

CAP_CLEANUP_DONE=0
CAP_EXIT_CODE=0

HOST_USER="cap-host-${RUN_ID}"
GUEST_USER="cap-guest-${RUN_ID}"
PASS="CapStack1!"
ROOM_MAIN="cap-room-main-${RUN_ID}"
ROOM_TERM="cap-room-term-${RUN_ID}"
TOUR_ID="cap-tour-${RUN_ID}"
GAME_ID="cap-game-${RUN_ID}"
SCHEMA_FIXTURE="$TESTS_DIR/fixtures/public-read-schema.json"

cleanup() {
  # Idempotent: INT/TERM then EXIT must not double-down or double-reap.
  if [ "${CAP_CLEANUP_DONE:-0}" = "1" ]; then
    return 0
  fi
  CAP_CLEANUP_DONE=1
  trap - EXIT INT TERM
  # Always reap background streams and bounded commands before temp cleanup.
  capability_kill_bg_pids
  rm -rf "$CAP_TMPDIR"
  exit "${CAP_EXIT_CODE}"
}
trap 'CAP_EXIT_CODE=$?; cleanup' EXIT
trap 'CAP_EXIT_CODE=130; cleanup' INT
trap 'CAP_EXIT_CODE=143; cleanup' TERM

phase() {
  echo
  echo "==> phase: $*"
}

# A CreateRoom acceptance precedes the dedicated runtime becoming Ready. Gate the
# first mutation on a safe, authenticated player-snapshot read; never retry the
# JoinRoom command whose outcome would be ambiguous after an HTTP failure.
wait_authenticated_player_snapshot() {
  local label="$1"
  local url="$2"
  local token="$3"
  local body_file="$4"
  local timeout_s="$5"
  local probe_max="${CAP_WAIT_HTTP_PROBE_MAX_TIME:-2}"
  local start=$SECONDS
  local code=""
  local probe_rc=0
  local elapsed=0

  while [ "$((SECONDS - start))" -lt "$timeout_s" ]; do
    probe_rc=0
    code="$(
      CAP_HTTP_BODY_FILE="$body_file" CAP_HTTP_MAX_TIME="$probe_max" \
        capability_http_code -H "Authorization: Bearer ${token}" "$url" 2>/dev/null
    )" || probe_rc=$?
    elapsed=$((SECONDS - start))
    if [ "$probe_rc" -eq 0 ] && [ "$code" = "200" ]; then
      echo "ok - $label (${elapsed}s)"
      return 0
    fi
    sleep 2
  done

  elapsed=$((SECONDS - start))
  echo "not ok - $label timed out after ${timeout_s}s (last=$code rc=$probe_rc)" >&2
  if [ -f "$body_file" ]; then
    echo "  body: $(head -c 1000 "$body_file")" >&2
  fi
  return 1
}

# Spectator View is an asynchronous Kafka projection. Poll only its safe GET
# until it has reached the committed Room sequence; never retry the mutation
# that produced the fact.
wait_spectator_snapshot_status() {
  local label="$1"
  local url="$2"
  local status="$3"
  local min_sequence="$4"
  local body_file="$5"
  local timeout_s="$6"
  local probe_max="${CAP_WAIT_HTTP_PROBE_MAX_TIME:-2}"
  local start=$SECONDS
  local code=""
  local probe_rc=0
  local elapsed=0

  while [ "$((SECONDS - start))" -lt "$timeout_s" ]; do
    probe_rc=0
    code="$(
      CAP_HTTP_BODY_FILE="$body_file" CAP_HTTP_MAX_TIME="$probe_max" \
        capability_http_code "$url" 2>/dev/null
    )" || probe_rc=$?
    elapsed=$((SECONDS - start))
    if [ "$probe_rc" -eq 0 ] && [ "$code" = "200" ] && \
        python3 - "$body_file" "$status" "$min_sequence" >/dev/null 2>&1 <<'PY'
import json
import sys

path, status, min_sequence = sys.argv[1], sys.argv[2], int(sys.argv[3])
with open(path, encoding="utf-8") as source:
    data = json.load(source)
assert data.get("status") == status, data
assert int(data.get("sequence", -1)) >= min_sequence, data
assert data.get("streamClosed") is False, data
PY
    then
      echo "ok - $label status=${status} sequence>=${min_sequence} (${elapsed}s)"
      return 0
    fi
    sleep 2
  done

  elapsed=$((SECONDS - start))
  echo "not ok - $label timed out after ${timeout_s}s (last=$code rc=$probe_rc)" >&2
  if [ -f "$body_file" ]; then
    echo "  body: $(head -c 1000 "$body_file")" >&2
  fi
  return 1
}

# Terminal denial is projected asynchronously too. Poll only the safe snapshot
# GET until Spectator View closes admission; never replay the terminal mutation.
wait_http_status() {
  local label="$1"
  local url="$2"
  local expected="$3"
  local body_file="$4"
  local timeout_s="$5"
  local probe_max="${CAP_WAIT_HTTP_PROBE_MAX_TIME:-2}"
  local start=$SECONDS
  local code=""
  local probe_rc=0
  local elapsed=0

  while [ "$((SECONDS - start))" -lt "$timeout_s" ]; do
    probe_rc=0
    code="$(
      CAP_HTTP_BODY_FILE="$body_file" CAP_HTTP_MAX_TIME="$probe_max" \
        capability_http_code "$url" 2>/dev/null
    )" || probe_rc=$?
    elapsed=$((SECONDS - start))
    if [ "$probe_rc" -eq 0 ] && [ "$code" = "$expected" ]; then
      echo "ok - $label HTTP ${expected} (${elapsed}s)"
      return 0
    fi
    sleep 2
  done

  elapsed=$((SECONDS - start))
  echo "not ok - $label timed out after ${timeout_s}s (last=$code rc=$probe_rc)" >&2
  if [ -f "$body_file" ]; then
    echo "  body: $(head -c 1000 "$body_file")" >&2
  fi
  return 1
}

# Login + assert only. Do not command-substitute this: must_cli_json and
# capability_json_assert print "ok - …" on stdout; capture the token with
# capability_json_get alone (same pattern as TOKEN_A / TOKEN_B above).
login_token() {
  local user="$1"
  local out="$CAP_TMPDIR/login-${user}.json"
  must_cli_json "login $user" "$out" login --user "$user" --pass "$PASS"
  capability_json_assert "login $user has token" "$out" \
    "assert data.get('token') and data.get('playerId') and data.get('sessionId'), data"
}

echo "live client parity run_id=$RUN_ID url=$UNOARENA_API_URL"
echo "tmp=$CAP_TMPDIR"

# --- 1 readiness (raw /health before first CLI use) ---
phase "1 health / ready"
capability_wait_http "$BASE/health" 200 "$READY_TIMEOUT_S" "$CAP_TMPDIR/raw-health.json"
capability_json_assert "raw /health status ok" "$CAP_TMPDIR/raw-health.json" \
  "assert data.get('status')=='ok', data"

capability_wait_http "$BASE/ready" 200 "$READY_TIMEOUT_S" "$CAP_TMPDIR/ready.json"
capability_json_assert "ready body" "$CAP_TMPDIR/ready.json" \
  "assert data.get('status')=='ready', data; assert data.get('service')=='gateway', data"

must_cli_json "health" "$CAP_TMPDIR/health.json" health
capability_json_assert "health status ok" "$CAP_TMPDIR/health.json" \
  "assert data.get('status')=='ok', data"

# --- 2 auth + session takeover ---
phase "2 register / login / session takeover"
# Unique RUN_ID usernames: registration must succeed (same strictness as guest).
must_cli_json "register host" "$CAP_TMPDIR/reg-host.json" \
  register --user "$HOST_USER" --pass "$PASS" --correlation-id "corr-reg-host-$RUN_ID"
capability_json_assert "register host ok" "$CAP_TMPDIR/reg-host.json" "assert data.get('status')=='ok', data"

must_cli_json "register guest" "$CAP_TMPDIR/reg-guest.json" \
  register --user "$GUEST_USER" --pass "$PASS" --correlation-id "corr-reg-guest-$RUN_ID"
capability_json_assert "register guest ok" "$CAP_TMPDIR/reg-guest.json" "assert data.get('status')=='ok', data"

must_cli_json "login host A" "$CAP_TMPDIR/login-a.json" login --user "$HOST_USER" --pass "$PASS"
TOKEN_A="$(capability_json_get "$CAP_TMPDIR/login-a.json" token)"
HOST_PLAYER="$(capability_json_get "$CAP_TMPDIR/login-a.json" playerId)"
capability_assert_eq "token A non-empty" "1" "$([ -n "$TOKEN_A" ] && echo 1 || echo 0)"

must_cli_json "whoami A" "$CAP_TMPDIR/whoami-a.json" whoami --token "$TOKEN_A"
capability_assert_eq "whoami A playerId" "$HOST_PLAYER" "$(capability_json_get "$CAP_TMPDIR/whoami-a.json" playerId)"

CTRL_OUT="$CAP_TMPDIR/control.sse"
CAP_SPAWN_ERR="$CTRL_OUT.err"
capability_spawn_pg "$CTRL_OUT" -- \
  "$CLI" stream control --token "$TOKEN_A" --correlation-id "corr-ctrl-$RUN_ID"
CTRL_PID="$CAP_SPAWN_PID"
capability_track_pid "$CTRL_PID"
# Subscription must precede takeover: settle, then require the stream still live.
sleep 1
if ! kill -0 "$CTRL_PID" 2>/dev/null; then
  echo "not ok - control SSE exited before takeover (subscription failed)" >&2
  cat "$CTRL_OUT" >&2 || true
  cat "$CTRL_OUT.err" >&2 || true
  exit 1
fi
echo "ok - control SSE subscribed before takeover"

must_cli_json "login host B (takeover)" "$CAP_TMPDIR/login-b.json" login --user "$HOST_USER" --pass "$PASS"
TOKEN_B="$(capability_json_get "$CAP_TMPDIR/login-b.json" token)"
HOST_PLAYER_B="$(capability_json_get "$CAP_TMPDIR/login-b.json" playerId)"
capability_assert_eq "takeover same playerId" "$HOST_PLAYER" "$HOST_PLAYER_B"
if [ "$TOKEN_A" = "$TOKEN_B" ]; then
  echo "not ok - takeover must issue a new token" >&2
  exit 1
fi
echo "ok - takeover token differs"

# Exact SSE framing (event: + data:) — not a 401 JSON substring match.
if capability_wait_sse_event "$CTRL_OUT" "session_invalidated" 15; then
  echo "ok - control SSE event: session_invalidated + data"
else
  echo "not ok - control SSE missing exact event: session_invalidated + data frame" >&2
  echo "--- control stdout ---" >&2
  cat "$CTRL_OUT" >&2 || true
  echo "--- control stderr ---" >&2
  cat "$CTRL_OUT.err" >&2 || true
  exit 1
fi
capability_kill_pid_group "$CTRL_PID"
wait "$CTRL_PID" 2>/dev/null || true
capability_untrack_pid "$CTRL_PID"

code="$(CAP_HTTP_BODY_FILE="$CAP_TMPDIR/whoami-stale.json" capability_http_code \
  -H "Authorization: Bearer ${TOKEN_A}" "$BASE/v1/auth/whoami")"
capability_assert_http "stale token whoami 401" "401" "$code"

must_cli_json "whoami B" "$CAP_TMPDIR/whoami-b.json" whoami --token "$TOKEN_B"
capability_assert_eq "whoami B playerId" "$HOST_PLAYER" "$(capability_json_get "$CAP_TMPDIR/whoami-b.json" playerId)"

login_token "$GUEST_USER"
GUEST_TOKEN="$(capability_json_get "$CAP_TMPDIR/login-${GUEST_USER}.json" token)"
GUEST_PLAYER="$(capability_json_get "$CAP_TMPDIR/login-${GUEST_USER}.json" playerId)"

# --- 3 room lifecycle ---
phase "3 create / join / lock / start public room"
CREATE_PAYLOAD="$(capability_payload_create_room "$ROOM_MAIN" 2)"
must_cli_json "CreateRoom" "$CAP_TMPDIR/create-main.json" \
  room create --token "$TOKEN_B" --command-id "cmd-cr-main-$RUN_ID" \
  --payload "$CREATE_PAYLOAD" --correlation-id "corr-cr-main-$RUN_ID"
capability_assert_accepted "CreateRoom accepted seq1" "$CAP_TMPDIR/create-main.json" 1
capability_json_assert "CreateRoom type" "$CAP_TMPDIR/create-main.json" \
  "assert data.get('type')=='CreateRoom', data"

PLAYER_SNAP_URL="$BASE/v1/rooms/$(capability_urlencode "$ROOM_MAIN")/snapshot"
wait_authenticated_player_snapshot \
  "host player snapshot readiness before first JoinRoom" \
  "$PLAYER_SNAP_URL" "$TOKEN_B" "$CAP_TMPDIR/host-snap-ready.json" \
  "$ROOM_READY_TIMEOUT_S"

# spectator waiting
SPEC_URL="$BASE/v1/spectator/rooms/$(capability_urlencode "$ROOM_MAIN")/snapshot"
wait_spectator_snapshot_status "spectator snapshot waiting" "$SPEC_URL" waiting 1 \
  "$CAP_TMPDIR/spec-waiting.json" "$SPECTATOR_PROJECTION_TIMEOUT_S"

# This mutation is attempted exactly once after the safe-read readiness gate.
must_cli_json "JoinRoom" "$CAP_TMPDIR/join-main.json" \
  command --token "$GUEST_TOKEN" --type JoinRoom --room-id "$ROOM_MAIN" \
  --command-id "cmd-join-main-$RUN_ID" --expected-sequence 1 --payload '{}' \
  --correlation-id "corr-join-main-$RUN_ID"
capability_assert_accepted "JoinRoom accepted seq2" "$CAP_TMPDIR/join-main.json" 2

must_cli_json "LockRoom" "$CAP_TMPDIR/lock-main.json" \
  command --token "$TOKEN_B" --type LockRoom --room-id "$ROOM_MAIN" \
  --command-id "cmd-lock-main-$RUN_ID" --expected-sequence 2 --payload '{}' \
  --correlation-id "corr-lock-main-$RUN_ID"
capability_assert_accepted "LockRoom accepted seq3" "$CAP_TMPDIR/lock-main.json" 3

wait_spectator_snapshot_status "spectator snapshot locked" "$SPEC_URL" locked 3 \
  "$CAP_TMPDIR/spec-locked.json" "$SPECTATOR_PROJECTION_TIMEOUT_S"

START_PAYLOAD="$(capability_payload_start_match "$GAME_ID")"
must_cli_json "StartMatch" "$CAP_TMPDIR/start-main.json" \
  command --token "$TOKEN_B" --type StartMatch --room-id "$ROOM_MAIN" \
  --command-id "cmd-start-main-$RUN_ID" --expected-sequence 3 --payload "$START_PAYLOAD" \
  --correlation-id "corr-start-main-$RUN_ID"
capability_assert_accepted "StartMatch accepted seq4" "$CAP_TMPDIR/start-main.json" 4

wait_spectator_snapshot_status "spectator snapshot in_progress" "$SPEC_URL" in_progress 4 \
  "$CAP_TMPDIR/spec-progress.json" "$SPECTATOR_PROJECTION_TIMEOUT_S"

# stale sequence reject (after start; expectedSequence 1 is stale). The CLI
# exits nonzero for HTTP 409 while preserving the rejected CommandResult in its
# diagnostic stream, so recover that JSON for the same contract assertion.
if cli_json "$CAP_TMPDIR/stale-join.json" \
  command --token "$GUEST_TOKEN" --type JoinRoom --room-id "$ROOM_MAIN" \
  --command-id "cmd-join-oldseq-$RUN_ID" --expected-sequence 1 --payload '{}' \
  --correlation-id "corr-join-oldseq-$RUN_ID"; then
  die "stale JoinRoom unexpectedly exited zero"
fi
python3 - "$CAP_TMPDIR/stale-join.json.err" "$CAP_TMPDIR/stale-join.json" <<'PY'
import json, pathlib, sys

source, destination = map(pathlib.Path, sys.argv[1:])
for line in reversed(source.read_text(encoding="utf-8").splitlines()):
    try:
        result = json.loads(line)
    except json.JSONDecodeError:
        continue
    if isinstance(result, dict) and result.get("status") == "rejected":
        destination.write_text(json.dumps(result, separators=(",", ":")) + "\n", encoding="utf-8")
        break
else:
    raise SystemExit("stale JoinRoom diagnostic omitted rejected CommandResult")
PY
echo "ok - stale JoinRoom rejected envelope (cli exit nonzero)"
capability_assert_rejected "stale sequence rejected" "$CAP_TMPDIR/stale-join.json" "stale"

# --- 4 DrawCard via player snapshot currentPlayer + spectator SSE after mutation ---
phase "4 deterministic DrawCard + spectator SSE"
# Spectator streams have no initial body: subscribe before the mutation, then
# require Spectator View's public projection_updated event with non-empty data.
# SnapshotSanitized is the upstream Room-to-Spectator Kafka fact, not the public
# projection stream event owned by Spectator View.
SPEC_LIVE="$CAP_TMPDIR/spec-live.sse"
CAP_SPAWN_ERR="$SPEC_LIVE.err"
capability_spawn_pg "$SPEC_LIVE" -- \
  "$CLI" stream spectator --room-id "$ROOM_MAIN" --correlation-id "corr-spec-live-$RUN_ID"
SPEC_PID="$CAP_SPAWN_PID"
capability_track_pid "$SPEC_PID"
sleep 1
if ! kill -0 "$SPEC_PID" 2>/dev/null; then
  echo "not ok - spectator stream exited before DrawCard (subscription failed)" >&2
  cat "$SPEC_LIVE" >&2 || true
  cat "$SPEC_LIVE.err" >&2 || true
  exit 1
fi
echo "ok - spectator SSE subscribed before DrawCard"

code="$(CAP_HTTP_BODY_FILE="$CAP_TMPDIR/host-snap.json" capability_http_code \
  -H "Authorization: Bearer ${TOKEN_B}" "$PLAYER_SNAP_URL")"
capability_assert_http "host player snapshot" "200" "$code"

# Prefer host snapshot; if currentPlayer is guest, fetch guest snapshot for sequence.
CURRENT="$(capability_json_get "$CAP_TMPDIR/host-snap.json" game.currentPlayer)"
SEQ="$(capability_json_get "$CAP_TMPDIR/host-snap.json" sequenceNumber)"
ACTOR_TOKEN="$TOKEN_B"
if [ "$CURRENT" = "$GUEST_PLAYER" ]; then
  ACTOR_TOKEN="$GUEST_TOKEN"
  code="$(CAP_HTTP_BODY_FILE="$CAP_TMPDIR/guest-snap.json" capability_http_code \
    -H "Authorization: Bearer ${GUEST_TOKEN}" "$PLAYER_SNAP_URL")"
  capability_assert_http "guest player snapshot" "200" "$code"
  SEQ="$(capability_json_get "$CAP_TMPDIR/guest-snap.json" sequenceNumber)"
  CURRENT="$(capability_json_get "$CAP_TMPDIR/guest-snap.json" game.currentPlayer)"
fi
capability_assert_eq "currentPlayer known" "1" \
  "$([ "$CURRENT" = "$HOST_PLAYER" ] || [ "$CURRENT" = "$GUEST_PLAYER" ] && echo 1 || echo 0)"
NEXT_SEQ=$((SEQ + 1))

must_cli_json "DrawCard" "$CAP_TMPDIR/draw.json" \
  command --token "$ACTOR_TOKEN" --type DrawCard --room-id "$ROOM_MAIN" \
  --command-id "cmd-draw-1-$RUN_ID" --expected-sequence "$SEQ" --payload '{}' \
  --correlation-id "corr-draw-$RUN_ID"
capability_assert_accepted "DrawCard accepted" "$CAP_TMPDIR/draw.json" "$NEXT_SEQ"

if capability_wait_sse_event "$SPEC_LIVE" "projection_updated" 20; then
  echo "ok - spectator SSE event: projection_updated + data after DrawCard"
else
  echo "not ok - spectator SSE missing exact event: projection_updated + data after DrawCard" >&2
  echo "--- spectator stdout ---" >&2
  cat "$SPEC_LIVE" >&2 || true
  echo "--- spectator stderr ---" >&2
  cat "$SPEC_LIVE.err" >&2 || true
  exit 1
fi
capability_kill_pid_group "$SPEC_PID"
wait "$SPEC_PID" 2>/dev/null || true
capability_untrack_pid "$SPEC_PID"

# --- 5 tournament ---
phase "5 tournament create / register / close / idempotent replay"
TOUR_CREATE="$(capability_payload_tournament_create "$TOUR_ID" 8)"
must_cli_json "CreateTournament" "$CAP_TMPDIR/tour-create.json" \
  tournament create --token "$TOKEN_B" --command-id "cmd-tour-create-$RUN_ID" \
  --payload "$TOUR_CREATE" --correlation-id "corr-tour-create-$RUN_ID"
capability_assert_accepted "CreateTournament" "$CAP_TMPDIR/tour-create.json"
capability_json_assert "CreateTournament type" "$CAP_TMPDIR/tour-create.json" \
  "assert data.get('type')=='CreateTournament', data"

TOUR_ID_PAYLOAD="$(capability_payload_tournament_id "$TOUR_ID")"
must_cli_json "RegisterPlayer host" "$CAP_TMPDIR/tour-reg-host.json" \
  tournament register --token "$TOKEN_B" --command-id "cmd-tour-reg-host-$RUN_ID" \
  --payload "$TOUR_ID_PAYLOAD"
capability_assert_accepted "RegisterPlayer host" "$CAP_TMPDIR/tour-reg-host.json"

must_cli_json "RegisterPlayer guest" "$CAP_TMPDIR/tour-reg-guest.json" \
  tournament register --token "$GUEST_TOKEN" --command-id "cmd-tour-reg-guest-$RUN_ID" \
  --payload "$TOUR_ID_PAYLOAD"
capability_assert_accepted "RegisterPlayer guest" "$CAP_TMPDIR/tour-reg-guest.json"

must_cli_json "CloseRegistration" "$CAP_TMPDIR/tour-close.json" \
  tournament close-registration --token "$TOKEN_B" --command-id "cmd-tour-close-$RUN_ID" \
  --payload "$TOUR_ID_PAYLOAD"
capability_assert_accepted "CloseRegistration" "$CAP_TMPDIR/tour-close.json"
capability_json_assert "CloseRegistration type" "$CAP_TMPDIR/tour-close.json" \
  "assert data.get('type')=='CloseRegistration', data"

must_cli_json "CloseRegistration idempotent" "$CAP_TMPDIR/tour-close-2.json" \
  tournament close-registration --token "$TOKEN_B" --command-id "cmd-tour-close-$RUN_ID" \
  --payload "$TOUR_ID_PAYLOAD"
capability_assert_accepted "CloseRegistration replay" "$CAP_TMPDIR/tour-close-2.json"
# Same command-id must return the same accepted outcome (not a second distinct accept).
capability_assert_command_replay_equal "CloseRegistration idempotent replay equal" \
  "$CAP_TMPDIR/tour-close.json" "$CAP_TMPDIR/tour-close-2.json"

# --- 6 leaderboard + analytics schema ---
phase "6 leaderboard + analytics schema reads"
must_cli_json "leaderboard" "$CAP_TMPDIR/lb.json" leaderboard --correlation-id "corr-lb-$RUN_ID"
must_cli_json "analytics" "$CAP_TMPDIR/an.json" analytics --correlation-id "corr-an-$RUN_ID"
# Fixture drives response validation (not an on-disk presence check).
capability_assert_public_read_schema "leaderboard+analytics vs fixture" \
  "$SCHEMA_FIXTURE" "$CAP_TMPDIR/lb.json" "$CAP_TMPDIR/an.json"

# --- 7 unknown Last-Event-ID => snapshot_required ---
# Unknown Last-Event-ID must refuse immediately with HTTP 409 (not a hung open SSE).
# Bound curl/CLI the same way as phase 8 so a regression that opens SSE cannot hang forever.
phase "7 unknown Last-Event-ID snapshot_required"
SSE_409_STREAM_URL="$BASE/v1/streams/spectator?roomId=$(capability_urlencode "$ROOM_MAIN")"
set +e
code="$(CAP_HTTP_MAX_TIME=3 CAP_HTTP_BODY_FILE="$CAP_TMPDIR/sse-409.json" \
  capability_http_code \
  -H "Last-Event-ID: cap-missing-event-id-$RUN_ID" \
  "$SSE_409_STREAM_URL" 2>"$CAP_TMPDIR/sse-409-http.err")"
sse_http_rc=$?
set -e
if [ "$sse_http_rc" -eq 28 ]; then
  echo "not ok - spectator stream unknown Last-Event-ID HTTP timed out (expected immediate 409)" >&2
  cat "$CAP_TMPDIR/sse-409-http.err" >&2 || true
  head -c 1000 "$CAP_TMPDIR/sse-409.json" >&2 || true
  exit 1
fi
if [ "$sse_http_rc" -ne 0 ]; then
  echo "not ok - spectator stream unknown Last-Event-ID HTTP curl failed (rc=$sse_http_rc)" >&2
  cat "$CAP_TMPDIR/sse-409-http.err" >&2 || true
  exit 1
fi
capability_assert_http "spectator stream HTTP 409" "409" "$code"
capability_json_assert "snapshot_required code" "$CAP_TMPDIR/sse-409.json" \
  "assert data.get('code')=='snapshot_required', data"

# Bounded CLI: snapshot_required is immediate; hang/timeout is a failure.
set +e
capability_run_bounded 5 "$CAP_TMPDIR/sse-409.out" -- \
  "$CLI" stream spectator --room-id "$ROOM_MAIN" \
  --last-event-id "cap-missing-event-id-$RUN_ID" --correlation-id "corr-sse-409-$RUN_ID"
sse_rc=$?
set -e
if [ "${CAP_RUN_BOUNDED_TIMED_OUT:-0}" -eq 1 ]; then
  echo "not ok - spectator stream unknown Last-Event-ID hung (expected immediate snapshot_required)" >&2
  cat "$CAP_TMPDIR/sse-409.out" >&2 || true
  exit 1
fi
if [ "$sse_rc" -eq 0 ]; then
  echo "not ok - spectator stream with unknown Last-Event-ID should fail" >&2
  cat "$CAP_TMPDIR/sse-409.out" >&2 || true
  exit 1
fi
echo "ok - spectator stream unknown Last-Event-ID exits nonzero without hang"
capability_assert_contains "CLI mentions snapshot_required" "snapshot_required" \
  "$(cat "$CAP_TMPDIR/sse-409.out" 2>/dev/null || true)"

code="$(CAP_HTTP_BODY_FILE="$CAP_TMPDIR/spec-resync.json" capability_http_code "$SPEC_URL")"
capability_assert_http "spectator resync snapshot" "200" "$code"

# --- 8 terminal room CancelRoom + spectator denied ---
# CancelRoom is the deterministic live representative of terminal spectator denial.
# RoomCompleted denial is covered by domain/service tests (room-gameplay + spectator-view);
# this harness does not invent a nondeterministic full-game completion path.
phase "8 CancelRoom then spectator denied"
TERM_PAYLOAD="$(capability_payload_create_room "$ROOM_TERM" 2)"
must_cli_json "CreateRoom term" "$CAP_TMPDIR/create-term.json" \
  room create --token "$TOKEN_B" --command-id "cmd-cr-term-$RUN_ID" \
  --payload "$TERM_PAYLOAD"
capability_assert_accepted "CreateRoom term" "$CAP_TMPDIR/create-term.json" 1

TERM_SPEC_URL="$BASE/v1/spectator/rooms/$(capability_urlencode "$ROOM_TERM")/snapshot"
TERM_PLAYER_SNAP_URL="$BASE/v1/rooms/$(capability_urlencode "$ROOM_TERM")/snapshot"
wait_authenticated_player_snapshot \
  "host player snapshot readiness before CancelRoom" \
  "$TERM_PLAYER_SNAP_URL" "$TOKEN_B" "$CAP_TMPDIR/term-player-snap-ready.json" \
  "$ROOM_READY_TIMEOUT_S"
wait_spectator_snapshot_status "term spectator waiting" "$TERM_SPEC_URL" waiting 1 \
  "$CAP_TMPDIR/spec-term-wait.json" "$SPECTATOR_PROJECTION_TIMEOUT_S"

# This mutation is attempted exactly once after the safe-read readiness gate.
must_cli_json "CancelRoom" "$CAP_TMPDIR/cancel-term.json" \
  command --token "$TOKEN_B" --type CancelRoom --room-id "$ROOM_TERM" \
  --command-id "cmd-cancel-term-$RUN_ID" --expected-sequence 1 --payload '{}'
# CreateRoom accepted at seq 1; CancelRoom must advance to seq 2.
capability_assert_accepted "CancelRoom" "$CAP_TMPDIR/cancel-term.json" 2

wait_http_status "term spectator snapshot 403" "$TERM_SPEC_URL" 403 \
  "$CAP_TMPDIR/spec-term-denied.json" "$SPECTATOR_PROJECTION_TIMEOUT_S"
capability_json_assert "term spectator snapshot spectator_denied" "$CAP_TMPDIR/spec-term-denied.json" \
  "assert data.get('code')=='spectator_denied', data"

# Raw stream endpoint must refuse immediately with HTTP 403 (not a hung open SSE).
# Bound curl so a broken denial that opens SSE cannot hang the harness forever.
TERM_STREAM_URL="$BASE/v1/streams/spectator?roomId=$(capability_urlencode "$ROOM_TERM")"
set +e
code="$(CAP_HTTP_MAX_TIME=3 CAP_HTTP_BODY_FILE="$CAP_TMPDIR/spec-term-stream-http.json" \
  capability_http_code "$TERM_STREAM_URL" 2>"$CAP_TMPDIR/spec-term-stream-http.err")"
term_http_rc=$?
set -e
if [ "$term_http_rc" -eq 28 ]; then
  echo "not ok - term spectator stream HTTP timed out (expected immediate 403 denial)" >&2
  cat "$CAP_TMPDIR/spec-term-stream-http.err" >&2 || true
  head -c 1000 "$CAP_TMPDIR/spec-term-stream-http.json" >&2 || true
  exit 1
fi
if [ "$term_http_rc" -ne 0 ]; then
  echo "not ok - term spectator stream HTTP curl failed (rc=$term_http_rc)" >&2
  cat "$CAP_TMPDIR/spec-term-stream-http.err" >&2 || true
  exit 1
fi
capability_assert_http "term spectator stream HTTP 403" "403" "$code"
capability_json_assert "term spectator stream spectator_denied" "$CAP_TMPDIR/spec-term-stream-http.json" \
  "assert data.get('code')=='spectator_denied', data"

# Bounded CLI: denial is immediate; hang/timeout is a failure.
set +e
capability_run_bounded 5 "$CAP_TMPDIR/spec-term-stream.out" -- \
  "$CLI" stream spectator --room-id "$ROOM_TERM" --correlation-id "corr-spec-term-$RUN_ID"
term_rc=$?
set -e
if [ "${CAP_RUN_BOUNDED_TIMED_OUT:-0}" -eq 1 ]; then
  echo "not ok - spectator stream after CancelRoom hung (expected immediate denial)" >&2
  cat "$CAP_TMPDIR/spec-term-stream.out" >&2 || true
  exit 1
fi
if [ "$term_rc" -eq 0 ]; then
  echo "not ok - spectator stream after CancelRoom should fail" >&2
  cat "$CAP_TMPDIR/spec-term-stream.out" >&2 || true
  exit 1
fi
echo "ok - spectator stream after CancelRoom exits nonzero without hang"
capability_assert_cli_terminal_denial "terminal stream denial CLI evidence" \
  "$(cat "$CAP_TMPDIR/spec-term-stream.out" 2>/dev/null || true)"

echo
echo "live client parity acceptance passed (run_id=$RUN_ID url=$UNOARENA_API_URL)"
