# Shared HTTP/JSON helpers for capability-stack integration tests.
# Requires: bash, curl, python3. No jq.

# Background PIDs tracked for trap cleanup (control SSE, bounded runs, etc.).
CAP_BG_PIDS=()

capability_require_cmds() {
  local c
  for c in bash curl python3; do
    command -v "$c" >/dev/null 2>&1 || {
      echo "FAIL: required command '$c' is not available" >&2
      return 1
    }
  done
}

capability_track_pid() {
  local pid="$1"
  [ -n "$pid" ] || return 0
  CAP_BG_PIDS+=("$pid")
}

capability_untrack_pid() {
  local pid="$1"
  local kept=()
  local p
  for p in "${CAP_BG_PIDS[@]+"${CAP_BG_PIDS[@]}"}"; do
    if [ "$p" != "$pid" ]; then
      kept+=("$p")
    fi
  done
  if [ "${#kept[@]}" -eq 0 ]; then
    CAP_BG_PIDS=()
  else
    CAP_BG_PIDS=("${kept[@]}")
  fi
}

# Recursively signal a PID and all descendants (macOS + Linux via pgrep -P).
capability_kill_pid_tree() {
  local pid="$1"
  local sig="${2:-TERM}"
  local child
  [ -n "$pid" ] || return 0
  for child in $(pgrep -P "$pid" 2>/dev/null || true); do
    capability_kill_pid_tree "$child" "$sig"
  done
  kill "-$sig" "$pid" 2>/dev/null || true
}

# Kill a background leader: prefer its process group (spawned via setpgrp), then
# walk the descendant tree. Portable on macOS and Linux (no GNU kill -- required).
capability_kill_pid_group() {
  local pid="$1"
  [ -n "$pid" ] || return 0
  # Negative PGID form: kill -TERM -PID (BSD/macOS and Linux).
  kill -TERM -"$pid" 2>/dev/null || true
  capability_kill_pid_tree "$pid" TERM
  kill -KILL -"$pid" 2>/dev/null || true
  capability_kill_pid_tree "$pid" KILL
}

# Kill and wait all tracked background PIDs (and descendants). Safe to call often.
capability_kill_bg_pids() {
  local pid
  for pid in "${CAP_BG_PIDS[@]+"${CAP_BG_PIDS[@]}"}"; do
    [ -n "$pid" ] || continue
    capability_kill_pid_group "$pid"
  done
  for pid in "${CAP_BG_PIDS[@]+"${CAP_BG_PIDS[@]}"}"; do
    [ -n "$pid" ] || continue
    wait "$pid" 2>/dev/null || true
  done
  CAP_BG_PIDS=()
}

# Spawn "$@" in its own process group so group/tree kill reaps pipelines and
# non-exec children. Redirects stdout/stderr to OUT (or OUT + CAP_SPAWN_ERR).
# Sets CAP_SPAWN_PID to the process-group leader. Requires python3 (setpgrp).
# Usage: capability_spawn_pg OUT_FILE -- command args...
capability_spawn_pg() {
  local out="$1"
  shift
  if [ "${1:-}" = "--" ]; then
    shift
  fi
  if [ "$#" -lt 1 ]; then
    echo "FAIL: capability_spawn_pg requires a command" >&2
    return 1
  fi
  local err="${CAP_SPAWN_ERR:-}"
  if [ -n "$err" ]; then
    python3 -c '
import os, sys
out, err, cmd = sys.argv[1], sys.argv[2], sys.argv[3:]
os.setpgrp()
fd_out = os.open(out, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
fd_err = os.open(err, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
os.dup2(fd_out, 1)
os.dup2(fd_err, 2)
os.close(fd_out)
os.close(fd_err)
os.execvp(cmd[0], cmd)
' "$out" "$err" "$@" &
  else
    python3 -c '
import os, sys
out, cmd = sys.argv[1], sys.argv[2:]
os.setpgrp()
fd = os.open(out, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
os.dup2(fd, 1)
os.dup2(fd, 2)
os.close(fd)
os.execvp(cmd[0], cmd)
' "$out" "$@" &
  fi
  CAP_SPAWN_PID=$!
}

# urlencode a single path segment (empty safe charset).
capability_urlencode() {
  python3 -c 'import urllib.parse,sys; print(urllib.parse.quote(sys.argv[1], safe=""))' "$1"
}

# GET/POST helper: writes body to CAP_HTTP_BODY file, echoes HTTP status code.
# Usage: code=$(capability_http_code [-H "Header: v" ...] URL)
# Optional env: CAP_HTTP_METHOD (default GET), CAP_HTTP_BODY_FILE, CAP_HTTP_DATA,
#   CAP_HTTP_MAX_TIME (seconds; portable curl --max-time). When set and curl times
#   out (exit 28), prints a clear FAIL on stderr, echoes the write-out code (often
#   200 for an opened SSE), and returns 28 so callers cannot treat a hang as success.
capability_http_code() {
  local method="${CAP_HTTP_METHOD:-GET}"
  local out="${CAP_HTTP_BODY_FILE:-${CAP_TMPDIR:?}/http-body.json}"
  local args=()
  local max_time="${CAP_HTTP_MAX_TIME:-}"
  local code=""
  local rc=0
  if [ -n "${CAP_HTTP_DATA+x}" ]; then
    args+=(--data-binary "${CAP_HTTP_DATA}")
  fi
  if [ -n "$max_time" ]; then
    args+=(--max-time "$max_time")
  fi
  code="$(curl --silent --show-error --output "$out" --write-out '%{http_code}' \
    --request "$method" \
    "${args[@]}" \
    "$@" )" || rc=$?
  if [ -n "$max_time" ] && [ "$rc" -eq 28 ]; then
    echo "FAIL: capability_http_code timed out after ${max_time}s (response may have hung open)" >&2
    printf '%s\n' "${code:-000}"
    return 28
  fi
  if [ "$rc" -ne 0 ]; then
    printf '%s\n' "${code:-000}"
    return "$rc"
  fi
  printf '%s\n' "$code"
  return 0
}

# Read a dotted JSON path from a file or stdin (-). Prints empty string if missing.
# Usage: capability_json_get FILE path.to.field
capability_json_get() {
  local src="$1"
  local path="$2"
  python3 - "$src" "$path" <<'PY'
import json, sys
src, path = sys.argv[1], sys.argv[2]
if src == "-":
    data = json.load(sys.stdin)
else:
    with open(src, encoding="utf-8") as f:
        data = json.load(f)
cur = data
for part in path.split("."):
    if isinstance(cur, dict) and part in cur:
        cur = cur[part]
    else:
        print("")
        raise SystemExit(0)
if cur is None:
    print("")
elif isinstance(cur, bool):
    print("true" if cur else "false")
elif isinstance(cur, (dict, list)):
    print(json.dumps(cur, separators=(",", ":")))
else:
    print(cur)
PY
}

# Assert JSON from file/stdin matches a python expression over `data`.
# Usage: capability_json_assert LABEL FILE 'assert data["status"]=="ok"'
capability_json_assert() {
  local label="$1"
  local src="$2"
  local expr="$3"
  if python3 - "$src" <<PY
import json, sys
src = sys.argv[1]
if src == "-":
    data = json.load(sys.stdin)
else:
    with open(src, encoding="utf-8") as f:
        data = json.load(f)
${expr}
PY
  then
    echo "ok - $label"
    return 0
  else
    echo "not ok - $label" >&2
    if [ "$src" != "-" ] && [ -f "$src" ]; then
      echo "  body: $(head -c 2000 "$src")" >&2
    fi
    return 1
  fi
}

# Assert command envelope accepted; optionally expect sequenceNumber.
# Usage: capability_assert_accepted LABEL JSON_FILE [expected_seq]
capability_assert_accepted() {
  local label="$1"
  local src="$2"
  local expect_seq="${3:-}"
  if [ -n "$expect_seq" ]; then
    capability_json_assert "$label" "$src" \
      "assert data.get('status')=='accepted', data; assert data.get('schemaVersion')==1, data; assert int(data.get('sequenceNumber'))==${expect_seq}, data"
  else
    capability_json_assert "$label" "$src" \
      "assert data.get('status')=='accepted', data; assert data.get('schemaVersion')==1, data"
  fi
}

# Assert same-command-id replay is semantically identical (not textual dump equality).
# Required keys status/type/schemaVersion/payload must be present and equal (Python deep
# equality so key order / whitespace cannot false-fail). sequenceNumber is optional:
# presence must match across both; when present, values must match. Missing required
# keys are not equal via None==None.
# Usage: capability_assert_command_replay_equal LABEL FIRST_JSON SECOND_JSON
capability_assert_command_replay_equal() {
  local label="$1"
  local first="$2"
  local second="$3"
  if python3 - "$first" "$second" <<'PY'
import json, sys

REQUIRED = ("status", "type", "schemaVersion", "payload")

def load(path):
    with open(path, encoding="utf-8") as f:
        return json.load(f)

a, b = load(sys.argv[1]), load(sys.argv[2])
for key in REQUIRED:
    assert key in a, (key, "missing in first", a)
    assert key in b, (key, "missing in second", b)
    assert a[key] == b[key], (key, a[key], b[key], a, b)
has_seq_a = "sequenceNumber" in a
has_seq_b = "sequenceNumber" in b
assert has_seq_a == has_seq_b, ("sequenceNumber", "presence mismatch", has_seq_a, has_seq_b, a, b)
if has_seq_a:
    assert a["sequenceNumber"] == b["sequenceNumber"], (
        "sequenceNumber", a["sequenceNumber"], b["sequenceNumber"], a, b
    )
PY
  then
    echo "ok - $label"
    return 0
  else
    echo "not ok - $label" >&2
    echo "  first:  $(head -c 1000 "$first" 2>/dev/null || true)" >&2
    echo "  second: $(head -c 1000 "$second" 2>/dev/null || true)" >&2
    return 1
  fi
}

# Assert rejected command (HTTP 200 + status=rejected).
# Optional reason_substr must appear in an authoritative reason field only
# (reason, rejectionReason, or code) — never the full JSON dump (commandId bait).
capability_assert_rejected() {
  local label="$1"
  local src="$2"
  local reason_substr="${3:-}"
  if [ -n "$reason_substr" ]; then
    capability_json_assert "$label" "$src" \
      "assert data.get('status')=='rejected', data; reason=str(data.get('reason') or data.get('rejectionReason') or data.get('code') or ''); assert '${reason_substr}' in reason.lower(), (reason, data)"
  else
    capability_json_assert "$label" "$src" \
      "assert data.get('status')=='rejected', data"
  fi
}

capability_assert_eq() {
  local label="$1" expected="$2" actual="$3"
  if [ "$expected" = "$actual" ]; then
    echo "ok - $label"
    return 0
  fi
  echo "not ok - $label" >&2
  echo "  expected: $expected" >&2
  echo "  actual:   $actual" >&2
  return 1
}

capability_assert_contains() {
  local label="$1" needle="$2" haystack="$3"
  case "$haystack" in
    *"$needle"*)
      echo "ok - $label"
      return 0
      ;;
  esac
  echo "not ok - $label" >&2
  echo "  missing: $needle" >&2
  return 1
}

# CLI terminal-stream denial evidence: require HTTP 403 with a non-digit/end
# boundary after the status, or spectator_denied. Reject bare "403" (ports like
# :40312), digit-extended statuses (HTTP 40312 / returned HTTP 4030), and bare
# "spectator" (appears in the stream path).
capability_assert_cli_terminal_denial() {
  local label="$1"
  local haystack="$2"
  if printf '%s' "$haystack" | grep -qiE '(HTTP[[:space:]]+403|returned HTTP 403)([^0-9]|$)|spectator_denied'; then
    echo "ok - $label"
    return 0
  fi
  echo "not ok - $label" >&2
  echo "  missing exact evidence: HTTP 403 / returned HTTP 403 / spectator_denied" >&2
  echo "  got: $(printf '%s' "$haystack" | head -c 500)" >&2
  return 1
}

# Validate live leaderboard + analytics responses against fixtures/public-read-schema.json.
# Usage: capability_assert_public_read_schema LABEL FIXTURE_JSON LB_JSON AN_JSON
capability_assert_public_read_schema() {
  local label="$1"
  local fixture="$2"
  local lb="$3"
  local an="$4"
  if python3 - "$fixture" "$lb" "$an" <<'PY'
import json, sys

def load(path):
    with open(path, encoding="utf-8") as f:
        return json.load(f)

spec = load(sys.argv[1])
lb = load(sys.argv[2])
an = load(sys.argv[3])
lb_spec = spec["leaderboard"]
an_spec = spec["analytics"]

for key in lb_spec["requiredKeys"]:
    assert key in lb, (key, lb)
if lb_spec.get("entriesType") == "array":
    assert isinstance(lb.get("entries"), list), lb

for key in an_spec["requiredKeys"]:
    assert key in an, (key, an)
if "authoritative" in an_spec:
    assert an.get("authoritative") is an_spec["authoritative"], an
for key in an_spec.get("arrayKeys", []):
    assert isinstance(an.get(key), list), (key, an)
PY
  then
    echo "ok - $label"
    return 0
  else
    echo "not ok - $label" >&2
    return 1
  fi
}

capability_assert_http() {
  local label="$1" expected="$2" actual="$3"
  capability_assert_eq "$label" "$expected" "$actual"
}

# Poll until GET URL returns expected HTTP code (default 200) or timeout seconds.
# Each probe uses a short CAP_HTTP_MAX_TIME (CAP_WAIT_HTTP_PROBE_MAX_TIME, default 2)
# so a hung response body cannot stall the overall READY_TIMEOUT_S loop. Probe
# timeouts count as failed probes and retry; they do not abort the wait.
# Success requires probe exit 0 AND matching status — a timed-out curl that still
# wrote %{http_code} 200 (open SSE) must not count as ready.
capability_wait_http() {
  local url="$1"
  local want="${2:-200}"
  local timeout_s="${3:-120}"
  local body_file="${4:-${CAP_TMPDIR:?}/wait-body.json}"
  local probe_max="${CAP_WAIT_HTTP_PROBE_MAX_TIME:-2}"
  local start=$SECONDS
  local code=""
  local probe_rc=0
  local elapsed=0
  while [ "$((SECONDS - start))" -lt "$timeout_s" ]; do
    # Scope CAP_HTTP_* inside the substitution so they do not persist in the caller.
    # Capture probe rc via || without toggling set -e (same pattern as cli_json).
    probe_rc=0
    code="$(
      CAP_HTTP_BODY_FILE="$body_file" CAP_HTTP_MAX_TIME="$probe_max" \
        capability_http_code "$url" 2>/dev/null
    )" || probe_rc=$?
    elapsed=$((SECONDS - start))
    if [ "$probe_rc" -eq 0 ] && [ "$code" = "$want" ]; then
      echo "ok - wait $url -> $want (${elapsed}s)"
      return 0
    fi
    sleep 2
  done
  elapsed=$((SECONDS - start))
  echo "not ok - wait $url timed out after ${timeout_s}s (last=$code rc=$probe_rc)" >&2
  if [ -f "$body_file" ]; then
    echo "  body: $(head -c 1000 "$body_file")" >&2
  fi
  return 1
}

# Run CLI; write stdout to OUT and stderr to OUT.err; return the CLI exit code.
# Requires CLI (path to unoarena binary). Does not toggle shell `set -e`
# (avoids leaking errexit to callers that wrap expected failures in set +e).
cli_json() {
  local out="$1"
  shift
  local rc=0
  "$CLI" "$@" >"$out" 2>"$out.err" || rc=$?
  return "$rc"
}

must_cli_json() {
  local label="$1"
  local out="$2"
  shift 2
  if cli_json "$out" "$@"; then
    echo "ok - $label (cli exit 0)"
  else
    echo "not ok - $label (cli exit nonzero)" >&2
    cat "$out.err" >&2 || true
    cat "$out" >&2 || true
    return 1
  fi
}

# Run a background command for up to max_seconds without GNU timeout.
# Exposes child outcome truthfully:
#   - If the child exits on its own, return its exit status (e.g. 7).
#   - If still running at max_seconds, kill/wait it, set CAP_RUN_BOUNDED_TIMED_OUT=1, return 0.
# Does not toggle shell `set -e` (avoids leaking errexit to callers).
# Spawns in its own process group so descendants are reaped on timeout.
# Usage: capability_run_bounded MAX_SECONDS OUT_FILE -- command args...
capability_run_bounded() {
  local max_s="$1"
  local out="$2"
  shift 2
  if [ "$1" = "--" ]; then
    shift
  fi
  CAP_RUN_BOUNDED_TIMED_OUT=0
  CAP_SPAWN_ERR=""
  capability_spawn_pg "$out" -- "$@"
  local pid="$CAP_SPAWN_PID"
  capability_track_pid "$pid"
  local i=0
  local rc=0
  while [ "$i" -lt "$max_s" ]; do
    if ! kill -0 "$pid" 2>/dev/null; then
      rc=0
      wait "$pid" || rc=$?
      capability_untrack_pid "$pid"
      return "$rc"
    fi
    sleep 1
    i=$((i + 1))
  done
  capability_kill_pid_group "$pid"
  wait "$pid" 2>/dev/null || true
  capability_untrack_pid "$pid"
  CAP_RUN_BOUNDED_TIMED_OUT=1
  return 0
}

# True if FILE contains an SSE frame with exact event name and a non-empty data: line.
# Does not match bare JSON substrings (e.g. 401 bodies mentioning the event name).
capability_sse_frame_has_event() {
  local file="$1"
  local event="$2"
  [ -f "$file" ] || return 1
  python3 - "$file" "$event" <<'PY'
import sys
path, want = sys.argv[1], sys.argv[2]
try:
    text = open(path, encoding="utf-8", errors="replace").read()
except OSError:
    raise SystemExit(1)
lines = text.splitlines()
i = 0
while i < len(lines):
    line = lines[i]
    if line.startswith("event:"):
        name = line[6:].strip()
        if name == want:
            j = i + 1
            while j < len(lines) and lines[j] != "":
                if lines[j].startswith("data:"):
                    payload = lines[j][5:].lstrip()
                    if payload != "":
                        raise SystemExit(0)
                j += 1
    i += 1
raise SystemExit(1)
PY
}

# Poll until FILE has an exact SSE event+data frame or timeout.
capability_wait_sse_event() {
  local file="$1"
  local event="$2"
  local timeout_s="${3:-15}"
  local elapsed=0
  while [ "$elapsed" -lt "$timeout_s" ]; do
    if capability_sse_frame_has_event "$file" "$event"; then
      return 0
    fi
    sleep 0.25
    elapsed=$((elapsed + 1))
    sleep 0.75
  done
  return 1
}

# True if OUT looks like a live SSE body (framing or known spectator/control frames).
# Ignores post-kill curl noise when frames are already present (bounded runs SIGTERM the child).
capability_sse_live_ok() {
  local file="$1"
  [ -f "$file" ] || return 1
  # Hard denials / HTTP error responses from the CLI (not SIGTERM curl teardown noise).
  if grep -qiE 'HTTP[[:space:]]+403|spectator_denied|returned HTTP [45][0-9][0-9]' "$file" 2>/dev/null; then
    return 1
  fi
  if capability_sse_frame_has_event "$file" "SnapshotSanitized" \
    || capability_sse_frame_has_event "$file" "snapshot" \
    || capability_sse_frame_has_event "$file" "session_invalidated"; then
    return 0
  fi
  if grep -qiE '^(event:|data:|id:)' "$file" 2>/dev/null; then
    return 0
  fi
  if grep -qiE 'Connection refused|Could not resolve host|returned no HTTP status' "$file" 2>/dev/null; then
    return 1
  fi
  return 1
}

# Poll a file until it contains needle or timeout.
capability_wait_file_contains() {
  local file="$1"
  local needle="$2"
  local timeout_s="${3:-10}"
  local elapsed=0
  while [ "$elapsed" -lt "$timeout_s" ]; do
    if [ -f "$file" ] && grep -F -q -- "$needle" "$file" 2>/dev/null; then
      return 0
    fi
    sleep 0.25
    # bash arithmetic with tenths is awkward; count in 1s after first second
    elapsed=$((elapsed + 1))
    sleep 0.75
  done
  return 1
}
