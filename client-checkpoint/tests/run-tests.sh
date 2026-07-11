#!/usr/bin/env bash
# Offline CLI tests against an ephemeral stdlib fake BFF. No persistent servers.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
CLI="${ROOT}/bin/unoarena"
FAKE="${ROOT}/tests/fake_gateway.py"
PASS=0
FAIL=0

die() {
  echo "FAIL: $*" >&2
  exit 1
}

assert_eq() {
  local label="$1" expected="$2" actual="$3"
  if [ "$expected" = "$actual" ]; then
    echo "ok - $label"
    PASS=$((PASS + 1))
  else
    echo "not ok - $label" >&2
    echo "  expected: $expected" >&2
    echo "  actual:   $actual" >&2
    FAIL=$((FAIL + 1))
  fi
}

assert_contains() {
  local label="$1" needle="$2" haystack="$3"
  case "$haystack" in
    *"$needle"*)
      echo "ok - $label"
      PASS=$((PASS + 1))
      ;;
    *)
      echo "not ok - $label" >&2
      echo "  missing: $needle" >&2
      echo "  in: $haystack" >&2
      FAIL=$((FAIL + 1))
      ;;
  esac
}

assert_exit() {
  local label="$1" expected="$2"
  shift 2
  set +e
  "$@" >/tmp/unoarena-test-out.$$ 2>/tmp/unoarena-test-err.$$
  local code=$?
  set -e
  if [ "$code" -eq "$expected" ]; then
    echo "ok - $label (exit $code)"
    PASS=$((PASS + 1))
  else
    echo "not ok - $label (exit $code, expected $expected)" >&2
    cat /tmp/unoarena-test-err.$$ >&2 || true
    FAIL=$((FAIL + 1))
  fi
}

json_field() {
  local json="$1" field="$2"
  printf '%s' "$json" | python3 -c '
import json, sys
field = sys.argv[1]
data = json.load(sys.stdin)
print(data.get(field, ""))
' "$field"
}

json_number_field() {
  local json="$1" field="$2"
  printf '%s' "$json" | python3 -c '
import json, sys
field = sys.argv[1]
data = json.load(sys.stdin)
v = data.get(field)
print("" if v is None else v)
' "$field"
}

py_assert_request() {
  local label="$1"
  local predicate="$2"
  local reqs
  reqs="$(curl --silent --show-error "${UNOARENA_API_URL}/__requests")"
  if printf '%s' "$reqs" | python3 -c "
import json, sys
doc = json.load(sys.stdin)
requests = doc.get('requests', [])
${predicate}
" 2>/tmp/unoarena-py-assert-err.$$; then
    echo "ok - $label"
    PASS=$((PASS + 1))
  else
    echo "not ok - $label" >&2
    cat /tmp/unoarena-py-assert-err.$$ >&2 || true
    echo "  requests: $reqs" >&2
    FAIL=$((FAIL + 1))
  fi
}

start_fake() {
  python3 "$FAKE" 0 >/tmp/unoarena-fake-url.$$ 2>/tmp/unoarena-fake-err.$$ &
  FAKE_PID=$!
  for _ in 1 2 3 4 5 6 7 8 9 10; do
    if [ -s /tmp/unoarena-fake-url.$$ ]; then
      break
    fi
    sleep 0.05
  done
  API_URL="$(tr -d '\n' </tmp/unoarena-fake-url.$$)"
  [ -n "$API_URL" ] || {
    cat /tmp/unoarena-fake-err.$$ >&2 || true
    die "fake gateway did not print a URL"
  }
  export UNOARENA_API_URL="$API_URL"
}

stop_fake() {
  if [ -n "${FAKE_PID:-}" ]; then
    kill "$FAKE_PID" 2>/dev/null || true
    wait "$FAKE_PID" 2>/dev/null || true
  fi
  rm -f /tmp/unoarena-fake-url.$$ /tmp/unoarena-fake-err.$$ /tmp/unoarena-test-out.$$ /tmp/unoarena-test-err.$$ /tmp/unoarena-py-assert-err.$$ /tmp/unoarena-player-err.$$ /tmp/unoarena-spec-err.$$
}

trap stop_fake EXIT

chmod +x "$CLI"

echo "==> syntax check"
bash -n "$CLI"
python3 -m py_compile "$FAKE"
echo "ok - bash -n and py_compile"
PASS=$((PASS + 1))

echo "==> usage / env errors (no server)"
unset UNOARENA_API_URL || true
assert_exit "missing UNOARENA_API_URL fails" 1 env -u UNOARENA_API_URL "$CLI" register --user a --pass b
assert_exit "unknown command fails" 2 env UNOARENA_API_URL=http://127.0.0.1:9 "$CLI" nope
assert_exit "whoami without token fails" 2 env UNOARENA_API_URL=http://127.0.0.1:9 "$CLI" whoami

echo "==> advisory countdown (local, no HTTP)"
COUNT_JSON="$("$CLI" countdown --expires-at 2026-05-17T12:00:09Z --opening-sequence 42 --now 2026-05-17T12:00:04Z --json)"
assert_contains "countdown marks advisory" '"advisory":true' "$COUNT_JSON"
assert_eq "countdown opening sequence" "42" "$(json_number_field "$COUNT_JSON" openingSequence)"
assert_eq "countdown remainingSeconds" "5" "$(json_number_field "$COUNT_JSON" remainingSeconds)"
assert_contains "countdown expiresAt" '2026-05-17T12:00:09Z' "$COUNT_JSON"

EXPIRED_JSON="$("$CLI" countdown --expires-at 2026-05-17T12:00:09Z --opening-sequence 42 --now 2026-05-17T12:00:10Z --json)"
assert_eq "countdown expired remaining" "0" "$(json_number_field "$EXPIRED_JSON" remainingSeconds)"
assert_contains "countdown expired flag" '"expired":true' "$EXPIRED_JSON"

assert_exit "countdown rejects non-integer opening sequence" 1 \
  "$CLI" countdown --expires-at 2026-05-17T12:00:09Z --opening-sequence '4;rm' --now 2026-05-17T12:00:04Z --json
assert_exit "countdown rejects decimal opening sequence" 1 \
  "$CLI" countdown --expires-at 2026-05-17T12:00:09Z --opening-sequence 4.2 --now 2026-05-17T12:00:04Z --json

echo "==> gateway-backed commands"
start_fake

HEALTH="$("$CLI" health --correlation-id corr-health)"
assert_contains "health status" '"status": "ok"' "$HEALTH"
assert_contains "health service" '"service": "gateway"' "$HEALTH"

REG="$("$CLI" register --user 'alice"drop' --pass 'secr"et' --correlation-id corr-reg)"
assert_contains "register status" '"status": "ok"' "$REG"
assert_contains "register username escaped" '"username": "alice\"drop"' "$REG"

LOGIN="$("$CLI" login --user alice --pass secret --correlation-id corr-login)"
TOKEN="$(json_field "$LOGIN" token)"
assert_eq "login token" "tok-alice" "$TOKEN"

# raw: must never be an in-band JSON literal sentinel for untrusted scalars
RAW_REG="$("$CLI" register --user 'raw:true' --pass 'raw:true' --correlation-id corr-raw-reg)"
assert_contains "raw: username still string in response" '"username": "raw:true"' "$RAW_REG"

RAW_LOGIN="$("$CLI" login --user 'raw:true' --pass 'raw:true' --correlation-id corr-raw-login)"
assert_eq "raw: username login token" "tok-raw:true" "$(json_field "$RAW_LOGIN" token)"

RAW_CMD="$("$CLI" command --token "$TOKEN" --type 'raw:true' --command-id 'raw:true' --correlation-id corr-raw-cmd --schema-version 1)"
assert_eq "raw: command type echoed as string" "raw:true" "$(json_field "$RAW_CMD" type)"
assert_eq "raw: commandId echoed as string" "raw:true" "$(json_field "$RAW_CMD" commandId)"

WHO="$("$CLI" whoami --token "$TOKEN" --correlation-id corr-who)"
assert_eq "whoami username" "alice" "$(json_field "$WHO" username)"

ROOM="$("$CLI" room create --token "$TOKEN" --command-id cmd-room-1 --correlation-id corr-room --schema-version 1)"
assert_eq "room create status" "accepted" "$(json_field "$ROOM" status)"
assert_eq "room create type" "CreateRoom" "$(json_field "$ROOM" type)"

CMD="$("$CLI" command --token "$TOKEN" --type PlayCard --room-id 'room/1 x' --command-id cmd-play-1 --expected-sequence 3 --payload '{"cardId":"c1"}' --correlation-id corr-play --schema-version 1)"
assert_eq "room command status" "accepted" "$(json_field "$CMD" status)"
assert_eq "room command type" "PlayCard" "$(json_field "$CMD" type)"

assert_exit "missing expected sequence rejected locally" 1 \
  "$CLI" command --token "$TOKEN" --type PlayCard --room-id room-1 --command-id cmd-missing-seq --payload '{}'
assert_contains "missing sequence error mentions expectedSequenceNumber" "expectedSequenceNumber" "$(cat /tmp/unoarena-test-err.$$)"

assert_exit "non-integer expected sequence rejected" 1 \
  "$CLI" command --token "$TOKEN" --type PlayCard --room-id room-1 --command-id cmd-bad-seq --expected-sequence '3,99' --payload '{}'

assert_exit "non-integer schemaVersion rejected" 1 \
  "$CLI" command --token "$TOKEN" --type PlayCard --room-id room-1 --command-id cmd-bad-sv --expected-sequence 1 --schema-version '1e1' --payload '{}'
assert_exit "schemaVersion other than 1 rejected" 1 \
  "$CLI" command --token "$TOKEN" --type PlayCard --room-id room-1 --command-id cmd-sv2 --expected-sequence 1 --schema-version 2 --payload '{}'


assert_exit "non-object payload rejected" 1 \
  "$CLI" command --token "$TOKEN" --type PlayCard --room-id room-1 --command-id cmd-bad-payload --expected-sequence 1 --payload '[]'

assert_exit "malformed payload JSON rejected" 1 \
  "$CLI" command --token "$TOKEN" --type PlayCard --room-id room-1 --command-id cmd-inject --expected-sequence 1 --payload '{"x":1},{"y":2}'

TOUR="$("$CLI" tournament create --token "$TOKEN" --command-id cmd-tour-1 --payload '{"name":"Open"}' --correlation-id corr-tour)"
assert_eq "tournament create type" "CreateTournament" "$(json_field "$TOUR" type)"

TREG="$("$CLI" tournament register --token "$TOKEN" --command-id cmd-treg-1 --payload '{"tournamentId":"t1"}' --correlation-id corr-treg)"
assert_eq "tournament register type" "RegisterPlayer" "$(json_field "$TREG" type)"

TCLOSE="$("$CLI" tournament close-registration --token "$TOKEN" --command-id cmd-tclose-1 --payload '{"tournamentId":"t1"}' --correlation-id corr-tclose)"
assert_eq "tournament close type" "CloseRegistration" "$(json_field "$TCLOSE" type)"

LB="$("$CLI" leaderboard --correlation-id corr-lb)"
assert_contains "leaderboard entries" '"rating": 1200' "$LB"

AN="$("$CLI" analytics --correlation-id corr-an)"
assert_eq "analytics gamesPlayed" "42" "$(json_number_field "$AN" gamesPlayed)"

PLAYER_STREAM="$("$CLI" stream player --token "$TOKEN" --room-id 'room/1 x' --last-event-id 41 --correlation-id corr-pstream 2>/tmp/unoarena-player-err.$$)"
assert_contains "player SSE event" "CardPlayed" "$PLAYER_STREAM"
assert_contains "player SSE schemaVersion" '"schemaVersion": 1' "$PLAYER_STREAM"
assert_contains "player advisory countdown stderr" "openingSequence=42" "$(cat /tmp/unoarena-player-err.$$)"
assert_contains "player advisory expiresAt stderr" "expiresAt=2099-01-01T00:00:05Z" "$(cat /tmp/unoarena-player-err.$$)"
assert_contains "player advisory accepts legacy openingRoomSequence alias" "openingSequence=99" "$(cat /tmp/unoarena-player-err.$$)"

SPEC_STREAM="$("$CLI" stream spectator --room-id room-1 --last-event-id 6 --correlation-id corr-sstream 2>/tmp/unoarena-spec-err.$$)"
assert_contains "spectator SSE event" "SpectatorUpdate" "$SPEC_STREAM"
assert_contains "spectator SSE schemaVersion" '"schemaVersion": 1' "$SPEC_STREAM"
assert_contains "spectator advisory countdown stderr" "openingSequence=7" "$(cat /tmp/unoarena-spec-err.$$)"

CTRL_STREAM="$("$CLI" stream control --token "$TOKEN" --last-event-id ctrl-0 --correlation-id corr-cstream)"
assert_contains "control SSE event" "SessionInvalidated" "$CTRL_STREAM"
assert_contains "control SSE schemaVersion" '"schemaVersion": 1' "$CTRL_STREAM"

curl --silent --show-error -X POST "${UNOARENA_API_URL}/__fail-next-stream" >/dev/null
assert_exit "player SSE HTTP failure exits nonzero" 1 \
  "$CLI" stream player --token "$TOKEN" --room-id room-fail --correlation-id corr-fail-p
assert_contains "player SSE failure mentions HTTP" "HTTP" "$(cat /tmp/unoarena-test-err.$$)"

curl --silent --show-error -X POST "${UNOARENA_API_URL}/__fail-next-stream" >/dev/null
assert_exit "spectator SSE HTTP failure exits nonzero" 1 \
  "$CLI" stream spectator --room-id room-fail --correlation-id corr-fail-s

curl --silent --show-error -X POST "${UNOARENA_API_URL}/__fail-next-stream" >/dev/null
assert_exit "control SSE HTTP failure exits nonzero" 1 \
  "$CLI" stream control --token "$TOKEN" --correlation-id corr-fail-c

echo "==> request body / header assertions"
py_assert_request "register body + correlation" '
regs = [r for r in requests if r["path"] == "/v1/auth/register"]
assert regs, "missing register"
body = json.loads(regs[0]["body"])
assert body == {"username": "alice\"drop", "password": "secr\"et"}, body
assert regs[0]["headers"].get("x-correlation-id") == "corr-reg"
'

py_assert_request "raw: username/password stay JSON strings" '
regs = [r for r in requests if r["path"] == "/v1/auth/register" and r["headers"].get("x-correlation-id") == "corr-raw-reg"]
assert regs, "missing raw: register"
body = json.loads(regs[0]["body"])
assert body == {"username": "raw:true", "password": "raw:true"}, body
assert isinstance(body["username"], str) and isinstance(body["password"], str)
'

py_assert_request "raw: login username/password stay JSON strings" '
logs = [r for r in requests if r["path"] == "/v1/auth/login" and r["headers"].get("x-correlation-id") == "corr-raw-login"]
assert logs, "missing raw: login"
body = json.loads(logs[0]["body"])
assert body == {"username": "raw:true", "password": "raw:true"}, body
assert isinstance(body["username"], str) and isinstance(body["password"], str)
'

py_assert_request "raw: commandId and type stay JSON strings" '
cmds = [r for r in requests if r["path"] == "/v1/commands" and r["headers"].get("x-correlation-id") == "corr-raw-cmd"]
assert cmds, "missing raw: command"
body = json.loads(cmds[0]["body"])
assert body["commandId"] == "raw:true", body
assert body["type"] == "raw:true", body
assert isinstance(body["commandId"], str) and isinstance(body["type"], str)
assert cmds[0]["headers"].get("x-command-id") == "raw:true"
'

py_assert_request "login body + correlation" '
logs = [r for r in requests if r["path"] == "/v1/auth/login" and r["headers"].get("x-correlation-id") == "corr-login"]
assert logs, "missing login"
body = json.loads(logs[0]["body"])
assert body == {"username": "alice", "password": "secret"}, body
assert logs[0]["headers"].get("x-correlation-id") == "corr-login"
'

py_assert_request "whoami auth + correlation" '
whos = [r for r in requests if r["path"] == "/v1/auth/whoami"]
assert whos, "missing whoami"
assert whos[0]["headers"].get("authorization") == "Bearer tok-alice"
assert whos[0]["headers"].get("x-correlation-id") == "corr-who"
'

py_assert_request "CreateRoom complete body + command-id header" '
cmds = [r for r in requests if r["path"] == "/v1/commands" and "CreateRoom" in r.get("body","")]
assert cmds, "missing CreateRoom"
body = json.loads(cmds[0]["body"])
assert body == {
  "commandId": "cmd-room-1",
  "type": "CreateRoom",
  "schemaVersion": 1,
  "payload": {},
}, body
assert "expectedSequenceNumber" not in body
assert cmds[0]["headers"].get("authorization") == "Bearer tok-alice"
assert cmds[0]["headers"].get("x-command-id") == "cmd-room-1"
assert cmds[0]["headers"].get("x-correlation-id") == "corr-room"
'

py_assert_request "PlayCard complete body + URL-encoded room path" '
plays = [r for r in requests if r["path"].endswith("/commands") and r["path"].startswith("/v1/rooms/")]
assert plays, "missing room command"
# room/1 x -> room%2F1%20x
assert plays[0]["path"] == "/v1/rooms/room%2F1%20x/commands", plays[0]["path"]
body = json.loads(plays[0]["body"])
assert body == {
  "commandId": "cmd-play-1",
  "type": "PlayCard",
  "schemaVersion": 1,
  "expectedSequenceNumber": 3,
  "payload": {"cardId": "c1"},
}, body
assert plays[0]["headers"].get("authorization") == "Bearer tok-alice"
assert plays[0]["headers"].get("x-command-id") == "cmd-play-1"
assert plays[0]["headers"].get("x-correlation-id") == "corr-play"
'

py_assert_request "player stream Last-Event-ID + auth + encoded roomId" '
ps = [r for r in requests if r["path"] == "/v1/streams/player" and r["headers"].get("x-correlation-id") == "corr-pstream"]
assert ps, "missing player stream"
last = ps[-1]
assert last["headers"].get("authorization") == "Bearer tok-alice"
assert last["headers"].get("last-event-id") == "41"
assert last["headers"].get("x-correlation-id") == "corr-pstream"
assert last["query"].get("roomId") == ["room/1 x"], last["query"]
'

py_assert_request "spectator stream Last-Event-ID" '
ss = [r for r in requests if r["path"] == "/v1/streams/spectator"]
assert ss, "missing spectator stream"
# first successful spectator (room-1), not the fail probe
ok = [r for r in ss if r["query"].get("roomId") == ["room-1"]]
assert ok, ss
assert ok[0]["headers"].get("last-event-id") == "6"
assert ok[0]["headers"].get("x-correlation-id") == "corr-sstream"
'

py_assert_request "control stream auth + Last-Event-ID" '
cs = [r for r in requests if r["path"] == "/v1/streams/control"]
assert cs, "missing control stream"
ok = [r for r in cs if r["headers"].get("x-correlation-id") == "corr-cstream"]
assert ok, cs
assert ok[0]["headers"].get("authorization") == "Bearer tok-alice"
assert ok[0]["headers"].get("last-event-id") == "ctrl-0"
'

py_assert_request "health + leaderboard + analytics hit OpenAPI paths" '
paths = {r["path"] for r in requests}
assert "/health" in paths
assert "/v1/rankings/leaderboards" in paths
assert "/v1/analytics/public" in paths
assert "/v1/streams/control" in paths
'

py_assert_request "no legacy identity paths" '
legacy = [r for r in requests if r["path"] in ("/register", "/login", "/whoami")]
assert not legacy, legacy
'

echo
echo "Passed: $PASS  Failed: $FAIL"
[ "$FAIL" -eq 0 ]
