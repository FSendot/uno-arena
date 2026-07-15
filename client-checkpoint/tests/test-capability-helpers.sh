#!/usr/bin/env bash
# Focused unit checks for capability harness helpers (no Docker stack).
set -euo pipefail

TESTS_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=lib/capability-assert.sh
. "$TESTS_DIR/lib/capability-assert.sh"
# shellcheck source=lib/capability-fixtures.sh
. "$TESTS_DIR/lib/capability-fixtures.sh"

TMP="$(mktemp -d "${TMPDIR:-/tmp}/uno-cap-helper.XXXXXX")"
trap 'capability_kill_bg_pids; rm -rf "$TMP"' EXIT
export CAP_TMPDIR="$TMP"

pass=0
fail=0
check() {
  local label="$1"
  shift
  if "$@"; then
    echo "ok - $label"
    pass=$((pass + 1))
  else
    echo "not ok - $label" >&2
    fail=$((fail + 1))
  fi
}

# Select a free loopback port for the stdlib HTTP fixtures in this test. The
# live kind wrapper owns Gateway port selection separately.
free_localhost_port() {
  python3 - <<'PY'
import socket
with socket.socket() as sock:
    sock.bind(("127.0.0.1", 0))
    print(sock.getsockname()[1])
PY
}

localhost_port_usable() {
  case "${1:-}" in
    ''|*[!0-9]*) return 1 ;;
  esac
  [ "$1" -ge 1 ] && [ "$1" -le 65535 ]
}

# --- capability_run_bounded: immediate exit 7 must surface ---
set +e
capability_run_bounded 5 "$TMP/exit7.out" -- bash -c 'exit 7'
rc=$?
set -e
check "bounded immediate exit 7 returns 7" test "$rc" -eq 7
check "bounded immediate exit 7 not marked timed_out" test "${CAP_RUN_BOUNDED_TIMED_OUT:-0}" -eq 0

# --- capability_run_bounded: still-alive timeout returns 0 + flag ---
set +e
capability_run_bounded 1 "$TMP/sleep.out" -- bash -c 'sleep 30'
rc=$?
set -e
check "bounded timeout returns 0" test "$rc" -eq 0
check "bounded timeout sets CAP_RUN_BOUNDED_TIMED_OUT" test "${CAP_RUN_BOUNDED_TIMED_OUT:-0}" -eq 1

# --- PID tracking: track + kill_bg_pids reaps ---
bash -c 'sleep 60' &
pid=$!
capability_track_pid "$pid"
check "tracked pid is alive" kill -0 "$pid"
capability_kill_bg_pids
set +e
kill -0 "$pid" 2>/dev/null
alive=$?
set -e
check "kill_bg_pids reaped tracked pid" test "$alive" -ne 0

# --- Descendant reap: non-exec child (pipeline) must die with parent ---
CAP_SPAWN_ERR=""
capability_spawn_pg "$TMP/pipeline.out" -- bash -c 'sleep 120 | sleep 120'
pipe_leader="$CAP_SPAWN_PID"
capability_track_pid "$pipe_leader"
# Give bash time to spawn pipeline children.
sleep 0.5
descendants="$(pgrep -P "$pipe_leader" 2>/dev/null || true)"
check "pipeline leader has descendants before kill" test -n "$descendants"
# Snapshot descendant PIDs for post-kill proof.
desc_list="$descendants"
capability_kill_bg_pids
sleep 0.3
set +e
kill -0 "$pipe_leader" 2>/dev/null
leader_alive=$?
set -e
check "pipeline leader reaped" test "$leader_alive" -ne 0
desc_alive=0
for d in $desc_list; do
  if kill -0 "$d" 2>/dev/null; then
    desc_alive=1
  fi
done
check "pipeline descendants reaped" test "$desc_alive" -eq 0

# --- SSE frame helper: exact event + data (not JSON substring) ---
printf 'event: session_invalidated\ndata: {"sessionId":"s1","reason":"session_invalidated"}\n\n' >"$TMP/ctrl.sse"
check "sse_frame_has_event accepts session_invalidated frame" \
  capability_sse_frame_has_event "$TMP/ctrl.sse" "session_invalidated"
printf '{"code":"unauthorized","message":"session_invalidated"}\n' >"$TMP/ctrl-401.json"
if capability_sse_frame_has_event "$TMP/ctrl-401.json" "session_invalidated"; then
  echo "not ok - sse_frame_has_event rejects 401 JSON substring" >&2
  fail=$((fail + 1))
else
  echo "ok - sse_frame_has_event rejects 401 JSON substring"
  pass=$((pass + 1))
fi
printf 'event: SnapshotSanitized\ndata: {"schemaVersion":1,"roomId":"r1"}\n\n' >"$TMP/spec.sse"
check "sse_frame_has_event accepts SnapshotSanitized frame" \
  capability_sse_frame_has_event "$TMP/spec.sse" "SnapshotSanitized"
printf 'event: projection_updated\ndata: {"schemaVersion":1,"roomId":"r1","sequence":2}\n\n' >"$TMP/spec-projection.sse"
check "sse_frame_has_event accepts projection_updated frame" \
  capability_sse_frame_has_event "$TMP/spec-projection.sse" "projection_updated"
check "sse_live_ok accepts projection_updated public frame" \
  capability_sse_live_ok "$TMP/spec-projection.sse"
printf 'event: SnapshotSanitized\ndata:\n\n' >"$TMP/spec-empty-data.sse"
if capability_sse_frame_has_event "$TMP/spec-empty-data.sse" "SnapshotSanitized"; then
  echo "not ok - sse_frame_has_event rejects empty data" >&2
  fail=$((fail + 1))
else
  echo "ok - sse_frame_has_event rejects empty data"
  pass=$((pass + 1))
fi

# --- SSE live proof helper ---
printf 'event: snapshot\ndata: {"schemaVersion":1}\n\n' >"$TMP/good.sse"
check "sse_live_ok accepts framing" capability_sse_live_ok "$TMP/good.sse"
printf 'FAIL: SSE /v1/streams/spectator returned HTTP 403\n' >"$TMP/deny.sse"
if capability_sse_live_ok "$TMP/deny.sse"; then
  echo "not ok - sse_live_ok rejects HTTP 403" >&2
  fail=$((fail + 1))
else
  echo "ok - sse_live_ok rejects HTTP 403"
  pass=$((pass + 1))
fi
printf 'event: snapshot\ndata: {}\nSSE request failed (curl exit 56)\n' >"$TMP/killed.sse"
check "sse_live_ok accepts frames despite post-kill curl noise" capability_sse_live_ok "$TMP/killed.sse"
: >"$TMP/empty.sse"
if capability_sse_live_ok "$TMP/empty.sse"; then
  echo "not ok - sse_live_ok rejects empty" >&2
  fail=$((fail + 1))
else
  echo "ok - sse_live_ok rejects empty"
  pass=$((pass + 1))
fi

# --- free localhost port selector (Python socket) ---
free1="$(free_localhost_port)"
check "free_localhost_port returns usable nonzero" localhost_port_usable "$free1"
# Hold free1 so the next selection cannot reuse it while bound.
python3 -c "import socket,time; s=socket.socket(); s.bind(('127.0.0.1', int('$free1'))); time.sleep(60)" &
hold_pid=$!
capability_track_pid "$hold_pid"
sleep 0.2
free2="$(free_localhost_port)"
check "free_localhost_port second pick usable" localhost_port_usable "$free2"
check "free_localhost_port second pick differs while first held" test "$free1" != "$free2"
capability_kill_bg_pids

# --- value channel: assert ok diagnostics must not be command-substituted with values ---
# Mirrors the harness login_token bug: must_cli_json / capability_json_assert print
# "ok - …" on stdout; capturing the whole helper pollutes tokens used as Bearer values.
printf '{"token":"tok-clean","playerId":"p1","sessionId":"s1"}\n' >"$TMP/login-value.json"
assert_ok_out="$(capability_json_assert "login has token" "$TMP/login-value.json" \
  "assert data.get('token') and data.get('playerId') and data.get('sessionId'), data")"
check "json_assert ok diagnostic is on stdout" test "$assert_ok_out" = "ok - login has token"

# Buggy pattern: assert then get inside one captured function (stdout mixes logs + value).
polluted_login_token() {
  capability_json_assert "login has token" "$TMP/login-value.json" \
    "assert data.get('token') and data.get('playerId') and data.get('sessionId'), data"
  capability_json_get "$TMP/login-value.json" token
}
polluted="$(polluted_login_token)"
case "$polluted" in
  tok-clean)
    echo "not ok - polluted capture should include assert ok diagnostic" >&2
    fail=$((fail + 1))
    ;;
  *"ok - login has token"*tok-clean*)
    echo "ok - polluted capture mixes ok diagnostic with token"
    pass=$((pass + 1))
    ;;
  *)
    echo "not ok - polluted capture unexpected: $polluted" >&2
    fail=$((fail + 1))
    ;;
esac

# Correct pattern: diagnostics outside capture; only json_get is substituted.
capability_json_assert "login has token (clean path)" "$TMP/login-value.json" \
  "assert data.get('token') and data.get('playerId') and data.get('sessionId'), data" \
  >/dev/null
clean="$(capability_json_get "$TMP/login-value.json" token)"
check "json_get-only capture is raw token" test "$clean" = "tok-clean"

# --- cli_json: must not leak set -e into callers (Phase 7 expected-failure path) ---
CLI="$TMP/stub-cli-fail"
cat >"$CLI" <<'EOF'
#!/usr/bin/env bash
echo '{"ok":false}' 
echo 'stub fail' >&2
exit 7
EOF
chmod +x "$CLI"
# Subshell: buggy cli_json re-enables errexit then returns nonzero → aborts before continued=1.
set +e
cli_leak_probe="$(
  set +e
  cli_json "$TMP/cli-leak.out" probe-args
  rc=$?
  # Still under caller's +e: a failing command must not abort if cli_json did not leak -e.
  false
  printf 'rc=%s continued=1' "$rc"
)"
cli_leak_rc=$?
set -e
check "cli_json nonzero under +e continues with status" test "$cli_leak_probe" = "rc=7 continued=1"
check "cli_json leak probe subshell exits 0" test "$cli_leak_rc" -eq 0
check "cli_json wrote stdout file" test -f "$TMP/cli-leak.out"
check "cli_json wrote stderr file" test -f "$TMP/cli-leak.out.err"

set +e
must_cli_json "expect fail" "$TMP/must-fail.out" probe-args >/dev/null 2>&1
must_fail_rc=$?
set -e
check "must_cli_json fails on nonzero cli" test "$must_fail_rc" -ne 0

CLI="$TMP/stub-cli-ok"
cat >"$CLI" <<'EOF'
#!/usr/bin/env bash
echo '{"ok":true}'
exit 0
EOF
chmod +x "$CLI"
set +e
must_cli_json "expect ok" "$TMP/must-ok.out" probe-args >/dev/null 2>&1
must_ok_rc=$?
set -e
check "must_cli_json succeeds on zero cli" test "$must_ok_rc" -eq 0

# --- fixtures / urlencode smoke ---
rid="$(capability_unique_run_id)"
check "unique_run_id non-empty" test -n "$rid"
enc="$(capability_urlencode 'a b/c')"
check "urlencode encodes slash and space" test "$enc" = "a%20b%2Fc"

# --- assert_rejected: reason_substr only in reason/rejectionReason/code (not commandId) ---
printf '{"status":"rejected","commandId":"cmd-join-stale-echo","reason":"room_full","schemaVersion":1}\n' \
  >"$TMP/reject-cmdid-stale.json"
if capability_assert_rejected "cmdId stale bait" "$TMP/reject-cmdid-stale.json" "stale" \
  >/dev/null 2>&1; then
  echo "not ok - assert_rejected must not match commandId containing stale" >&2
  fail=$((fail + 1))
else
  echo "ok - assert_rejected ignores commandId containing stale"
  pass=$((pass + 1))
fi
printf '{"status":"rejected","commandId":"cmd-join-oldseq-1","reason":"stale sequence","schemaVersion":1}\n' \
  >"$TMP/reject-reason-stale.json"
check "assert_rejected matches reason field" \
  capability_assert_rejected "stale in reason" "$TMP/reject-reason-stale.json" "stale"
printf '{"status":"rejected","commandId":"cmd-x","rejectionReason":"STALE_SEQUENCE","schemaVersion":1}\n' \
  >"$TMP/reject-rejectionReason.json"
check "assert_rejected matches rejectionReason field" \
  capability_assert_rejected "stale in rejectionReason" "$TMP/reject-rejectionReason.json" "stale"
printf '{"status":"rejected","commandId":"cmd-x","code":"stale_expected_sequence","schemaVersion":1}\n' \
  >"$TMP/reject-code-stale.json"
check "assert_rejected matches code field" \
  capability_assert_rejected "stale in code" "$TMP/reject-code-stale.json" "stale"

# --- command replay equality: semantic field compare (not textual dump) ---
# Required: status/type/schemaVersion/payload. sequenceNumber optional but presence+value must match.
printf '{"status":"accepted","type":"CloseRegistration","schemaVersion":1,"sequenceNumber":3,"payload":{"tournamentId":"t1","extra":true},"commandId":"cmd-a"}\n' \
  >"$TMP/replay-a.json"
# Different key order / whitespace / ignored fields must still equal on the compared keys.
printf '{ "payload" : { "extra" : true , "tournamentId" : "t1" } , "sequenceNumber" : 3 , "schemaVersion" : 1 , "type" : "CloseRegistration" , "status" : "accepted" , "correlationId" : "other" }\n' \
  >"$TMP/replay-b-reordered.json"
check "command_replay_equal accepts reordered equal fields" \
  capability_assert_command_replay_equal "replay reorder" \
  "$TMP/replay-a.json" "$TMP/replay-b-reordered.json"
# CloseRegistration accepted envelopes may omit sequenceNumber on both sides.
printf '{"status":"accepted","type":"CloseRegistration","schemaVersion":1,"payload":{"tournamentId":"t1","extra":true},"commandId":"cmd-a"}\n' \
  >"$TMP/replay-no-seq-a.json"
printf '{ "payload" : { "extra" : true , "tournamentId" : "t1" } , "schemaVersion" : 1 , "type" : "CloseRegistration" , "status" : "accepted" , "correlationId" : "other" }\n' \
  >"$TMP/replay-no-seq-b.json"
check "command_replay_equal accepts both omitting sequenceNumber" \
  capability_assert_command_replay_equal "replay both omit seq" \
  "$TMP/replay-no-seq-a.json" "$TMP/replay-no-seq-b.json"
printf '{"status":"accepted","type":"CloseRegistration","schemaVersion":1,"sequenceNumber":4,"payload":{"tournamentId":"t1","extra":true}}\n' \
  >"$TMP/replay-diff-seq.json"
if capability_assert_command_replay_equal "replay seq diverge" \
  "$TMP/replay-a.json" "$TMP/replay-diff-seq.json" >/dev/null 2>&1; then
  echo "not ok - command_replay_equal must fail on different sequenceNumber" >&2
  fail=$((fail + 1))
else
  echo "ok - command_replay_equal rejects different sequenceNumber"
  pass=$((pass + 1))
fi
# One-sided sequenceNumber presence must fail (optional, but presence must match).
printf '{"status":"accepted","type":"CloseRegistration","schemaVersion":1,"payload":{"tournamentId":"t1","extra":true}}\n' \
  >"$TMP/replay-one-sided-seq.json"
if capability_assert_command_replay_equal "replay one-sided seq" \
  "$TMP/replay-a.json" "$TMP/replay-one-sided-seq.json" >/dev/null 2>&1; then
  echo "not ok - command_replay_equal must fail when only one side has sequenceNumber" >&2
  fail=$((fail + 1))
else
  echo "ok - command_replay_equal rejects one-sided sequenceNumber presence"
  pass=$((pass + 1))
fi
printf '{"status":"accepted","type":"CloseRegistration","schemaVersion":1,"sequenceNumber":3,"payload":{"tournamentId":"t1","extra":false}}\n' \
  >"$TMP/replay-diff-payload.json"
if capability_assert_command_replay_equal "replay payload diverge" \
  "$TMP/replay-a.json" "$TMP/replay-diff-payload.json" >/dev/null 2>&1; then
  echo "not ok - command_replay_equal must fail on different payload" >&2
  fail=$((fail + 1))
else
  echo "ok - command_replay_equal rejects different payload"
  pass=$((pass + 1))
fi
# Missing required keys must not equal via None==None (both omit type).
printf '{"status":"accepted","schemaVersion":1,"payload":{"tournamentId":"t1"}}\n' \
  >"$TMP/replay-missing-type-a.json"
printf '{"status":"accepted","schemaVersion":1,"payload":{"tournamentId":"t1"}}\n' \
  >"$TMP/replay-missing-type-b.json"
if capability_assert_command_replay_equal "replay missing type" \
  "$TMP/replay-missing-type-a.json" "$TMP/replay-missing-type-b.json" >/dev/null 2>&1; then
  echo "not ok - command_replay_equal must fail when required keys are missing" >&2
  fail=$((fail + 1))
else
  echo "ok - command_replay_equal rejects missing required keys"
  pass=$((pass + 1))
fi

# --- capability_http_code: optional CAP_HTTP_MAX_TIME bounds hung responses ---
# Hang server: HTTP 200 + never-ending body (SSE-shaped). Without max-time this would block forever.
hang_port="$(free_localhost_port)"
python3 - "$hang_port" <<'PY' &
import sys, time
from http.server import BaseHTTPRequestHandler, HTTPServer

port = int(sys.argv[1])

class Hang(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.end_headers()
        try:
            while True:
                self.wfile.write(b": keepalive\n\n")
                self.wfile.flush()
                time.sleep(1)
        except BrokenPipeError:
            pass

    def log_message(self, *_args):
        pass

HTTPServer(("127.0.0.1", port), Hang).serve_forever()
PY
hang_pid=$!
capability_track_pid "$hang_pid"
# Wait until the hang server accepts connections.
for _ in 1 2 3 4 5 6 7 8 9 10; do
  if curl --silent --output /dev/null --max-time 0.2 "http://127.0.0.1:${hang_port}/" 2>/dev/null; then
    break
  fi
  # Connection refused until listen; max-time 0.2 may also fire once accepted — either means up.
  if curl --silent --output /dev/null --write-out '%{http_code}' --max-time 0.3 \
    "http://127.0.0.1:${hang_port}/" 2>/dev/null | grep -q .; then
    break
  fi
  sleep 0.1
done
# Outer watchdog: if CAP_HTTP_MAX_TIME is ignored, fail within ~4s instead of hanging forever.
: >"$TMP/hang-http.err"
: >"$TMP/hang-http.code"
(
  set +e
  CAP_HTTP_BODY_FILE="$TMP/hang-body.bin" CAP_HTTP_MAX_TIME=1 \
    code="$(capability_http_code "http://127.0.0.1:${hang_port}/" 2>"$TMP/hang-http.err")"
  rc=$?
  printf '%s\n' "$code" >"$TMP/hang-http.code"
  printf '%s\n' "$rc" >"$TMP/hang-http.rc"
) &
hang_probe=$!
capability_track_pid "$hang_probe"
hang_wait=0
while [ "$hang_wait" -lt 4 ] && kill -0 "$hang_probe" 2>/dev/null; do
  sleep 1
  hang_wait=$((hang_wait + 1))
done
if kill -0 "$hang_probe" 2>/dev/null; then
  capability_kill_pid_group "$hang_probe"
  wait "$hang_probe" 2>/dev/null || true
  capability_untrack_pid "$hang_probe"
  echo "not ok - http_code CAP_HTTP_MAX_TIME ignored (hung past outer watchdog)" >&2
  fail=$((fail + 1))
  hang_rc=99
  hang_code="hung"
else
  wait "$hang_probe" 2>/dev/null || true
  capability_untrack_pid "$hang_probe"
  hang_rc="$(cat "$TMP/hang-http.rc" 2>/dev/null || echo 99)"
  hang_code="$(cat "$TMP/hang-http.code" 2>/dev/null || true)"
fi
check "http_code max-time returns curl timeout status 28" test "$hang_rc" -eq 28
check "http_code max-time stderr mentions timed out" \
  grep -qi 'timed out' "$TMP/hang-http.err"
# Timed-out open stream must not look like a successful denial code.
check "http_code max-time does not report 403" test "$hang_code" != "403"
# Quick path without max-time still works (python stdlib echo).
quick_port="$(free_localhost_port)"
python3 - "$quick_port" <<'PY' &
import sys
from http.server import BaseHTTPRequestHandler, HTTPServer

port = int(sys.argv[1])

class Ok(BaseHTTPRequestHandler):
    def do_GET(self):
        body = b'{"ok":true}'
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_args):
        pass

HTTPServer(("127.0.0.1", port), Ok).serve_forever()
PY
capability_track_pid $!
sleep 0.2
quick_code="$(
  CAP_HTTP_BODY_FILE="$TMP/quick-body.json" \
    capability_http_code "http://127.0.0.1:${quick_port}/"
)"
check "http_code without max-time returns 200" test "$quick_code" = "200"
check "http_code without max-time wrote body" test -s "$TMP/quick-body.json"
capability_kill_bg_pids

# --- terminal denial CLI evidence: anchored HTTP 403 / spectator_denied (not bare 403/port) ---
check "cli_terminal_denial accepts HTTP 403" \
  capability_assert_cli_terminal_denial "deny-403" \
  'FAIL: SSE /v1/streams/spectator returned HTTP 403'
check "cli_terminal_denial accepts spectator_denied" \
  capability_assert_cli_terminal_denial "deny-code" \
  '{"code":"spectator_denied","message":"room_terminal"}'
if capability_assert_cli_terminal_denial "deny-path-only" \
  'FAIL: SSE /v1/streams/spectator returned no HTTP status' >/dev/null 2>&1; then
  echo "not ok - cli_terminal_denial rejects path-only spectator substring" >&2
  fail=$((fail + 1))
else
  echo "ok - cli_terminal_denial rejects path-only spectator substring"
  pass=$((pass + 1))
fi
if capability_assert_cli_terminal_denial "deny-port-40312" \
  'FAIL: connect 127.0.0.1:40312 connection refused' >/dev/null 2>&1; then
  echo "not ok - cli_terminal_denial must not match port 40312 as HTTP 403" >&2
  fail=$((fail + 1))
else
  echo "ok - cli_terminal_denial rejects port 40312 substring"
  pass=$((pass + 1))
fi
if capability_assert_cli_terminal_denial "deny-http-40312" \
  'FAIL: SSE /v1/streams/spectator returned HTTP 40312' >/dev/null 2>&1; then
  echo "not ok - cli_terminal_denial must not match HTTP 40312 as HTTP 403" >&2
  fail=$((fail + 1))
else
  echo "ok - cli_terminal_denial rejects HTTP 40312"
  pass=$((pass + 1))
fi
if capability_assert_cli_terminal_denial "deny-http-4030" \
  'FAIL: stream returned HTTP 4030' >/dev/null 2>&1; then
  echo "not ok - cli_terminal_denial must not match returned HTTP 4030 as HTTP 403" >&2
  fail=$((fail + 1))
else
  echo "ok - cli_terminal_denial rejects returned HTTP 4030"
  pass=$((pass + 1))
fi

# --- wait_http: timed-out 200 (hung body) must retry then fail; fast 200 must pass ---
# Threading server: a hung body must not block the next probe (false-pass shape + wait_http).
unset CAP_HTTP_BODY_FILE CAP_HTTP_MAX_TIME
wait_hang_port="$(free_localhost_port)"
python3 - "$wait_hang_port" <<'PY' &
import sys, time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

port = int(sys.argv[1])

class Hang(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.end_headers()
        try:
            while True:
                self.wfile.write(b": keepalive\n\n")
                self.wfile.flush()
                time.sleep(1)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def log_message(self, *_args):
        pass

ThreadingHTTPServer(("127.0.0.1", port), Hang).serve_forever()
PY
capability_track_pid $!
# Ready only when headers report 200 (do not treat curl 000 / connection-refused as up).
wait_hang_ready=0
for _ in $(seq 1 50); do
  set +e
  wait_hang_ready_code="$(
    curl --silent --output /dev/null --write-out '%{http_code}' --max-time 0.5 \
      "http://127.0.0.1:${wait_hang_port}/" 2>/dev/null
  )"
  set -e
  if [ "$wait_hang_ready_code" = "200" ]; then
    wait_hang_ready=1
    break
  fi
  sleep 0.1
done
check "wait_http hang fixture listening with HTTP 200" test "$wait_hang_ready" -eq 1
# Prove the false-pass shape: headers say 200 but curl times out (rc 28).
# Scope CAP_HTTP_* inside the substitution (same contract as capability_wait_http).
set +e
wait_hang_probe_rc=0
wait_hang_probe_code="$(
  CAP_HTTP_BODY_FILE="$TMP/wait-hang-probe.bin" CAP_HTTP_MAX_TIME=1 \
    capability_http_code "http://127.0.0.1:${wait_hang_port}/" 2>/dev/null
)" || wait_hang_probe_rc=$?
set -e
check "wait_http hang fixture yields timed-out 200" \
  test "$wait_hang_probe_code" = "200" -a "$wait_hang_probe_rc" -eq 28
check "wait_http hang probe does not persist CAP_HTTP_BODY_FILE" \
  test "${CAP_HTTP_BODY_FILE+x}" = ""
check "wait_http hang probe does not persist CAP_HTTP_MAX_TIME" \
  test "${CAP_HTTP_MAX_TIME+x}" = ""
# Outer watchdog: without per-probe max-time, wait_http hangs inside the first curl forever.
: >"$TMP/wait-http.rc"
(
  set +e
  CAP_WAIT_HTTP_PROBE_MAX_TIME=1 \
    capability_wait_http "http://127.0.0.1:${wait_hang_port}/" 200 3 "$TMP/wait-hang-body.bin" \
    >/dev/null 2>&1
  printf '%s\n' "$?" >"$TMP/wait-http.rc"
) &
wait_probe=$!
capability_track_pid "$wait_probe"
wait_watch=0
while [ "$wait_watch" -lt 8 ] && kill -0 "$wait_probe" 2>/dev/null; do
  sleep 1
  wait_watch=$((wait_watch + 1))
done
if kill -0 "$wait_probe" 2>/dev/null; then
  capability_kill_pid_group "$wait_probe"
  wait "$wait_probe" 2>/dev/null || true
  capability_untrack_pid "$wait_probe"
  echo "not ok - wait_http hung inside per-probe curl (overall timeout ignored)" >&2
  fail=$((fail + 1))
  wait_rc=99
else
  wait "$wait_probe" 2>/dev/null || true
  capability_untrack_pid "$wait_probe"
  wait_rc="$(cat "$TMP/wait-http.rc" 2>/dev/null || echo 99)"
fi
check "wait_http overall timeout returns 1 on hung body" test "$wait_rc" -eq 1
capability_kill_bg_pids

# Fast-closing 200 must succeed (rc 0 + matching code).
wait_ok_port="$(free_localhost_port)"
python3 - "$wait_ok_port" <<'PY' &
import sys
from http.server import BaseHTTPRequestHandler, HTTPServer

port = int(sys.argv[1])

class Ok(BaseHTTPRequestHandler):
    def do_GET(self):
        body = b'{"ok":true}'
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_args):
        pass

HTTPServer(("127.0.0.1", port), Ok).serve_forever()
PY
capability_track_pid $!
for _ in 1 2 3 4 5 6 7 8 9 10; do
  if curl --silent --output /dev/null --max-time 0.3 "http://127.0.0.1:${wait_ok_port}/" 2>/dev/null; then
    break
  fi
  sleep 0.1
done
unset CAP_HTTP_BODY_FILE CAP_HTTP_MAX_TIME
set +e
CAP_WAIT_HTTP_PROBE_MAX_TIME=1 \
  capability_wait_http "http://127.0.0.1:${wait_ok_port}/" 200 5 "$TMP/wait-ok-body.json" \
  >/dev/null 2>&1
wait_ok_rc=$?
set -e
check "wait_http fast closing 200 returns 0" test "$wait_ok_rc" -eq 0
check "wait_http fast path wrote body" grep -q '"ok":true' "$TMP/wait-ok-body.json"
check "wait_http fast path does not persist CAP_HTTP_BODY_FILE" \
  test "${CAP_HTTP_BODY_FILE+x}" = ""
check "wait_http fast path does not persist CAP_HTTP_MAX_TIME" \
  test "${CAP_HTTP_MAX_TIME+x}" = ""
capability_kill_bg_pids

# --- public-read fixture validates live response shapes (not on-disk theater) ---
FIXTURE="$TESTS_DIR/fixtures/public-read-schema.json"
printf '{"entries":[]}\n' >"$TMP/lb-ok.json"
printf '{"authoritative":false,"gameplayMetrics":[],"tournamentStats":[],"ratingStats":[]}\n' \
  >"$TMP/an-ok.json"
check "public_read_schema accepts valid lb+an" \
  capability_assert_public_read_schema "fixture ok" "$FIXTURE" "$TMP/lb-ok.json" "$TMP/an-ok.json"
printf '{"entries":"not-array"}\n' >"$TMP/lb-bad.json"
if capability_assert_public_read_schema "fixture bad" "$FIXTURE" "$TMP/lb-bad.json" "$TMP/an-ok.json" \
  >/dev/null 2>&1; then
  echo "not ok - public_read_schema rejects bad entries type" >&2
  fail=$((fail + 1))
else
  echo "ok - public_read_schema rejects bad entries type"
  pass=$((pass + 1))
fi
printf '{"authoritative":true,"gameplayMetrics":[],"tournamentStats":[],"ratingStats":[]}\n' \
  >"$TMP/an-auth.json"
if capability_assert_public_read_schema "fixture auth" "$FIXTURE" "$TMP/lb-ok.json" "$TMP/an-auth.json" \
  >/dev/null 2>&1; then
  echo "not ok - public_read_schema rejects authoritative true" >&2
  fail=$((fail + 1))
else
  echo "ok - public_read_schema rejects authoritative true"
  pass=$((pass + 1))
fi

echo
if [ "$fail" -ne 0 ]; then
  echo "helper tests failed: $fail failed, $pass passed" >&2
  exit 1
fi
echo "helper tests passed: $pass passed"
