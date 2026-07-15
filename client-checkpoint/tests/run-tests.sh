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

SESSION_DIR="$(mktemp -d /tmp/unoarena-session-XXXXXX)"
export UNOARENA_SESSION_FILE="${SESSION_DIR}/session.json"
export PYTHONPYCACHEPREFIX="${SESSION_DIR}/pycache"
unset UNOARENA_TOKEN || true

stop_fake() {
  if [ -n "${FAKE_PID:-}" ]; then
    kill "$FAKE_PID" 2>/dev/null || true
    wait "$FAKE_PID" 2>/dev/null || true
  fi
  rm -rf "$SESSION_DIR"
  rm -f /tmp/unoarena-fake-url.$$ /tmp/unoarena-fake-err.$$ /tmp/unoarena-test-out.$$ /tmp/unoarena-test-err.$$ /tmp/unoarena-py-assert-err.$$ /tmp/unoarena-player-err.$$ /tmp/unoarena-spec-err.$$ /tmp/unoarena-seed-out.$$
}

trap stop_fake EXIT

chmod +x "$CLI"

echo "==> syntax check"
bash -n "$CLI"
python3 -m py_compile "$FAKE"
python3 -m py_compile "${ROOT}/lib/game_client.py" "${ROOT}/tests/fake_game_gateway.py"
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

RAW_CMD="$("$CLI" command --token "$TOKEN" --type CreateRoom --command-id 'raw:true' --correlation-id corr-raw-cmd --schema-version 1)"
assert_eq "raw: CreateRoom status" "accepted" "$(json_field "$RAW_CMD" status)"
assert_eq "raw: commandId echoed as string" "raw:true" "$(json_field "$RAW_CMD" commandId)"

assert_exit "unknown command type rejected locally" 1 \
  "$CLI" command --token "$TOKEN" --type 'raw:true' --command-id cmd-unknown --schema-version 1
assert_contains "unknown type mentions catalog" "public catalog" "$(cat /tmp/unoarena-test-err.$$)"

assert_exit "unknown room-scoped type rejected locally" 1 \
  "$CLI" command --token "$TOKEN" --type NotARealCommand --room-id room-1 --command-id cmd-unknown-room --expected-sequence 1 --schema-version 1
assert_contains "unknown room-scoped mentions catalog" "public catalog" "$(cat /tmp/unoarena-test-err.$$)"

WHO="$("$CLI" whoami --token "$TOKEN" --correlation-id corr-who)"
assert_eq "whoami username" "alice" "$(json_field "$WHO" username)"

ROOM="$("$CLI" room create --token "$TOKEN" --command-id cmd-room-1 --correlation-id corr-room --schema-version 1 --payload '{}')"
assert_eq "room create status" "accepted" "$(json_field "$ROOM" status)"
assert_eq "room create type" "CreateRoom" "$(json_field "$ROOM" type)"

assert_exit "CreateRoom rejects expected-sequence locally" 1 \
  "$CLI" command --token "$TOKEN" --type CreateRoom --command-id cmd-cr-seq --expected-sequence 0 --schema-version 1
assert_contains "CreateRoom sequence prohibited message" "expectedSequenceNumber is not allowed" "$(cat /tmp/unoarena-test-err.$$)"

assert_exit "tournament create rejects expected-sequence locally" 1 \
  "$CLI" tournament create --token "$TOKEN" --command-id cmd-tour-seq --expected-sequence 1 --payload '{"tournamentId":"t1"}'
assert_contains "tournament sequence prohibited message" "expectedSequenceNumber is not allowed" "$(cat /tmp/unoarena-test-err.$$)"

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

TOUR="$("$CLI" tournament create --token "$TOKEN" --command-id cmd-tour-1 --payload '{"tournamentId":"t1","capacity":8}' --correlation-id corr-tour)"
assert_eq "tournament create type" "CreateTournament" "$(json_field "$TOUR" type)"
assert_eq "tournament create status" "accepted" "$(json_field "$TOUR" status)"

TREG="$("$CLI" tournament register --token "$TOKEN" --command-id cmd-treg-1 --payload '{"tournamentId":"t1"}' --correlation-id corr-treg)"
assert_eq "tournament register type" "RegisterPlayer" "$(json_field "$TREG" type)"

TCLOSE="$("$CLI" tournament close-registration --token "$TOKEN" --command-id cmd-tclose-1 --payload '{"tournamentId":"t1"}' --correlation-id corr-tclose)"
assert_eq "tournament close type" "CloseRegistration" "$(json_field "$TCLOSE" type)"

LB="$("$CLI" leaderboard --correlation-id corr-lb)"
assert_contains "leaderboard entries" '"rating": 1200' "$LB"
LB_TOUR="$("$CLI" leaderboard --board-type tournament_placement --limit 25 --cursor 'next page' --correlation-id corr-lb-tour)"
assert_contains "tournament leaderboard entries" '"rating": 1200' "$LB_TOUR"
assert_exit "leaderboard rejects unknown board type" 1 \
  "$CLI" leaderboard --board-type seasonal
assert_exit "leaderboard rejects limit above contract" 1 \
  "$CLI" leaderboard --limit 501

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

echo "==> fake gateway public envelope validation (raw POST)"
# Bypass CLI local checks so the fake BFF contract guard is exercised directly.
post_envelope() {
  local path="$1" body="$2"
  curl --silent --show-error -w '\n%{http_code}' \
    -X POST "${UNOARENA_API_URL}${path}" \
    -H 'Content-Type: application/json' \
    -H "Authorization: Bearer ${TOKEN}" \
    --data "$body"
}

assert_envelope_invalid() {
  local label="$1" path="$2" body="$3" needle="$4"
  local resp code out
  resp="$(post_envelope "$path" "$body")"
  code="$(printf '%s\n' "$resp" | tail -n 1)"
  out="$(printf '%s\n' "$resp" | sed '$d')"
  assert_eq "$label http" "400" "$code"
  assert_contains "$label code" '"invalid_envelope"' "$out"
  assert_contains "$label message" "$needle" "$out"
}

assert_envelope_accepted() {
  local label="$1" path="$2" body="$3"
  local resp code out
  resp="$(post_envelope "$path" "$body")"
  code="$(printf '%s\n' "$resp" | tail -n 1)"
  out="$(printf '%s\n' "$resp" | sed '$d')"
  assert_eq "$label http" "200" "$code"
  assert_eq "$label status" "accepted" "$(json_field "$out" status)"
}

assert_envelope_invalid "unknown top-level field" "/v1/commands" \
  '{"commandId":"c-extra","type":"CreateRoom","schemaVersion":1,"payload":{},"extra":true}' \
  'unknown field'
assert_envelope_invalid "schemaVersion bool true rejected" "/v1/commands" \
  '{"commandId":"c-sv-bool","type":"CreateRoom","schemaVersion":true,"payload":{}}' \
  'schemaVersion must be 1'
assert_envelope_invalid "schemaVersion 2 rejected" "/v1/commands" \
  '{"commandId":"c-sv2","type":"CreateRoom","schemaVersion":2,"payload":{}}' \
  'schemaVersion must be 1'
assert_envelope_invalid "unknown payload field" "/v1/commands" \
  '{"commandId":"c-pay-extra","type":"CreateTournament","schemaVersion":1,"payload":{"name":"Open"}}' \
  'unknown payload field'
assert_envelope_invalid "non-string roomId type" "/v1/rooms/room-1/commands" \
  '{"commandId":"c-roomid","type":"JoinRoom","expectedSequenceNumber":1,"schemaVersion":1,"payload":{"roomId":1}}' \
  'must be a string'
assert_envelope_invalid "invalid visibility enum" "/v1/commands" \
  '{"commandId":"c-vis","type":"CreateRoom","schemaVersion":1,"payload":{"visibility":"friends"}}' \
  'invalid value'
assert_envelope_invalid "invalid ChooseColor enum" "/v1/rooms/room-1/commands" \
  '{"commandId":"c-color","type":"ChooseColor","expectedSequenceNumber":1,"schemaVersion":1,"payload":{"color":"purple"}}' \
  'invalid value'
assert_envelope_invalid "maxSeats bound 1" "/v1/commands" \
  '{"commandId":"c-ms1","type":"CreateRoom","schemaVersion":1,"payload":{"maxSeats":1}}' \
  'maxSeats'
assert_envelope_invalid "capacity below minimum" "/v1/commands" \
  '{"commandId":"c-cap0","type":"CreateTournament","schemaVersion":1,"payload":{"capacity":0}}' \
  'must be >= 1'
assert_envelope_invalid "negative disconnectVersion" "/v1/rooms/room-1/commands" \
  '{"commandId":"c-dv","type":"ReconnectToRoom","expectedSequenceNumber":1,"schemaVersion":1,"payload":{"disconnectVersion":-1}}' \
  'must be >= 0'
assert_envelope_invalid "expectedSequenceNumber bool rejected" "/v1/rooms/room-1/commands" \
  '{"commandId":"c-seq-bool","type":"PlayCard","expectedSequenceNumber":true,"schemaVersion":1,"payload":{}}' \
  'expectedSequenceNumber must be an integer'

assert_envelope_accepted "maxSeats 0 edge" "/v1/commands" \
  '{"commandId":"c-ms0","type":"CreateRoom","schemaVersion":1,"payload":{"maxSeats":0}}'
assert_envelope_accepted "maxSeats 2 edge" "/v1/commands" \
  '{"commandId":"c-ms2","type":"CreateRoom","schemaVersion":1,"payload":{"maxSeats":2}}'
assert_envelope_accepted "maxSeats 10 edge" "/v1/commands" \
  '{"commandId":"c-ms10","type":"CreateRoom","schemaVersion":1,"payload":{"maxSeats":10,"visibility":"private"}}'
assert_envelope_accepted "disconnectVersion 0 edge" "/v1/rooms/room-1/commands" \
  '{"commandId":"c-dv0","type":"ReconnectToRoom","expectedSequenceNumber":0,"schemaVersion":1,"payload":{"disconnectVersion":0}}'
assert_envelope_accepted "tournament bound edges" "/v1/commands" \
  '{"commandId":"c-tour-edge","type":"CreateTournament","schemaVersion":1,"payload":{"tournamentId":"t1","capacity":1,"retryBudget":0,"batchSize":1}}'
assert_envelope_accepted "ChooseColor blue edge" "/v1/rooms/room-1/commands" \
  '{"commandId":"c-blue","type":"ChooseColor","expectedSequenceNumber":1,"schemaVersion":1,"payload":{"color":"blue"}}'

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

py_assert_request "raw: commandId stays JSON string on CreateRoom" '
cmds = [r for r in requests if r["path"] == "/v1/commands" and r["headers"].get("x-correlation-id") == "corr-raw-cmd"]
assert cmds, "missing raw: command"
body = json.loads(cmds[0]["body"])
assert body["commandId"] == "raw:true", body
assert body["type"] == "CreateRoom", body
assert isinstance(body["commandId"], str) and isinstance(body["type"], str)
assert "expectedSequenceNumber" not in body
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
cmds = [r for r in requests if r["path"] == "/v1/commands" and r["headers"].get("x-correlation-id") == "corr-room"]
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
leaderboards = [r for r in requests if r["path"] == "/v1/rankings/leaderboards"]
assert any(r["query"].get("boardType") == ["casual_elo"] for r in leaderboards), leaderboards
assert any(
    r["query"].get("boardType") == ["tournament_placement"]
    and r["query"].get("limit") == ["25"]
    and r["query"].get("cursor") == ["next page"]
    for r in leaderboards
), leaderboards
'

py_assert_request "no legacy identity paths" '
legacy = [r for r in requests if r["path"] in ("/register", "/login", "/whoami")]
assert not legacy, legacy
'

echo "==> session persistence / logout / whoami fallback"
rm -f "$UNOARENA_SESSION_FILE"
LOGIN_SESS="$("$CLI" login --user sessuser --pass secret --correlation-id corr-sess-login)"
assert_eq "login still prints token" "tok-sessuser" "$(json_field "$LOGIN_SESS" token)"
assert_eq "session file mode 0600" "600" "$(python3 -c 'import os,stat; print(oct(os.stat("'"$UNOARENA_SESSION_FILE"'").st_mode & 0o777)[2:])')"
SESS_JSON="$(cat "$UNOARENA_SESSION_FILE")"
assert_eq "session token field" "tok-sessuser" "$(json_field "$SESS_JSON" token)"
assert_eq "session playerId field" "player-sessuser" "$(json_field "$SESS_JSON" playerId)"
assert_eq "session sessionId field" "sess-sessuser" "$(json_field "$SESS_JSON" sessionId)"
assert_eq "session username field" "sessuser" "$(json_field "$SESS_JSON" username)"

WHO_SESS="$("$CLI" whoami --correlation-id corr-who-sess)"
assert_eq "whoami from session file" "sessuser" "$(json_field "$WHO_SESS" username)"

curl --silent --show-error -X POST "${UNOARENA_API_URL}/__fail-next-logout" >/dev/null
assert_exit "logout backend failure retains session" 1 "$CLI" logout
[ -e "$UNOARENA_SESSION_FILE" ]
echo "ok - logout backend failure retained session file"
PASS=$((PASS + 1))

LOGOUT_OUT="$("$CLI" logout)"
assert_contains "logout ok json" '"status":"ok"' "$LOGOUT_OUT"
case "$LOGOUT_OUT" in
  *tok-*) echo "not ok - logout must not print token" >&2; FAIL=$((FAIL + 1)) ;;
  *) echo "ok - logout does not print token"; PASS=$((PASS + 1)) ;;
esac
[ ! -e "$UNOARENA_SESSION_FILE" ]
echo "ok - logout removed session file"
PASS=$((PASS + 1))
py_assert_request "logout sends session bearer and correlation" '
logs = [r for r in requests if r["path"] == "/v1/auth/logout"]
assert logs, "missing logout"
assert logs[-1]["headers"].get("authorization") == "Bearer tok-sessuser"
assert logs[-1]["headers"].get("x-correlation-id")
'
LOGOUT2="$(env -u UNOARENA_API_URL "$CLI" logout)"
assert_contains "logout idempotent" '"status":"ok"' "$LOGOUT2"

# Corrupt / unsafe session fails closed for whoami fallback
printf '%s\n' '{"token":"x"}' >"$UNOARENA_SESSION_FILE"
chmod 600 "$UNOARENA_SESSION_FILE"
assert_exit "corrupt session fails closed" 1 "$CLI" whoami
assert_contains "corrupt session error" "corrupt session" "$(cat /tmp/unoarena-test-err.$$)"

printf '%s\n' '{"token":"tok-x","playerId":"p","sessionId":"s","username":"u"}' >"$UNOARENA_SESSION_FILE"
chmod 644 "$UNOARENA_SESSION_FILE"
assert_exit "world-readable session fails closed" 1 "$CLI" whoami
assert_contains "unsafe session error" "unsafe" "$(cat /tmp/unoarena-test-err.$$)"
rm -f "$UNOARENA_SESSION_FILE"

# --token still wins over env and session
printf '%s\n' '{"token":"tok-sess","playerId":"p","sessionId":"s","username":"sess"}' >"$UNOARENA_SESSION_FILE"
chmod 600 "$UNOARENA_SESSION_FILE"
WHO_FLAG="$("$CLI" whoami --token tok-alice --correlation-id corr-who-flag)"
assert_eq "whoami --token overrides session" "alice" "$(json_field "$WHO_FLAG" username)"
WHO_ENV="$(UNOARENA_TOKEN=tok-alice "$CLI" whoami --correlation-id corr-who-env)"
assert_eq "whoami UNOARENA_TOKEN overrides session" "alice" "$(json_field "$WHO_ENV" username)"
rm -f "$UNOARENA_SESSION_FILE"

echo "==> seed JSONL / register-or-ensure"
SEED_OUT="$("$CLI" seed --count 2 --prefix load 2>/tmp/unoarena-test-err.$$)"
printf '%s\n' "$SEED_OUT" >/tmp/unoarena-seed-out.$$
SEED_LINES="$(printf '%s\n' "$SEED_OUT" | python3 -c 'import sys; print(sum(1 for line in sys.stdin if line.strip()))')"
assert_eq "seed emits one JSON object per account" "2" "$SEED_LINES"
SEED_TOTAL_LINES="$(printf '%s\n' "$SEED_OUT" | python3 -c 'import sys; print(len(sys.stdin.read().splitlines()))')"
assert_eq "seed JSONL has no blank records" "2" "$SEED_TOTAL_LINES"
printf '%s\n' "$SEED_OUT" | python3 -c '
import json, sys
lines = [json.loads(l) for l in sys.stdin if l.strip()]
assert lines[0]["username"] == "load0001"
assert lines[0]["password"] == "load0001-pass"
assert lines[0]["token"] == "tok-load0001"
assert lines[0]["playerId"] == "player-load0001"
assert lines[0]["sessionId"] == "sess-load0001"
assert lines[0]["result"] == "ok"
assert lines[1]["username"] == "load0002"
assert lines[1]["result"] == "ok"
print("ok")
' >/dev/null
echo "ok - seed JSONL fields"
PASS=$((PASS + 1))
[ ! -e "$UNOARENA_SESSION_FILE" ] || [ ! -s "$UNOARENA_SESSION_FILE" ]
echo "ok - seed does not persist interactive session"
PASS=$((PASS + 1))

SEED_RERUN="$("$CLI" seed --count 1 --prefix load)"
assert_eq "seed rerun result exists" "exists" "$(printf '%s\n' "$SEED_RERUN" | python3 -c 'import json,sys; print(json.loads(sys.stdin.readline())["result"])')"
assert_eq "seed rerun still returns token" "tok-load0001" "$(json_field "$SEED_RERUN" token)"

assert_exit "seed count 0 rejected" 1 "$CLI" seed --count 0
assert_exit "seed count over max rejected" 1 "$CLI" seed --count 10001

echo "==> room list / create generated / join leave sequence"
LIST="$("$CLI" room list --correlation-id corr-room-list)"
assert_contains "room list public roomId" '"roomId": "room-public-1"' "$LIST"
assert_contains "room list visibility public" '"visibility": "public"' "$LIST"

py_assert_request "room list hits GET /v1/rooms without bearer" '
lists = [r for r in requests if r["path"] == "/v1/rooms" and r["method"] == "GET"]
assert lists, "missing room list"
assert lists[0]["headers"].get("x-correlation-id") == "corr-room-list"
assert "authorization" not in lists[0]["headers"]
'

GEN_ROOM="$("$CLI" room create --token tok-alice --command-id cmd-gen-room --correlation-id corr-gen-room --max 4)"
assert_eq "generated room create accepted" "accepted" "$(json_field "$GEN_ROOM" status)"
py_assert_request "generated CreateRoom includes roomId + maxSeats" '
cmds = [r for r in requests if r["path"] == "/v1/commands" and r["headers"].get("x-correlation-id") == "corr-gen-room"]
assert cmds, "missing generated CreateRoom"
body = json.loads(cmds[0]["body"])
assert body["type"] == "CreateRoom"
assert isinstance(body["payload"].get("roomId"), str) and body["payload"]["roomId"]
assert body["payload"].get("maxSeats") == 4
'

assert_exit "room create max 1 rejected" 1 \
  "$CLI" room create --token tok-alice --max 1
assert_exit "room create max 11 rejected" 1 \
  "$CLI" room create --token tok-alice --max 11

# Login to establish session for room tracking
"$CLI" login --user alice --pass secret >/dev/null
JOIN="$("$CLI" room join room-join-1 --command-id cmd-join-1 --correlation-id corr-join)"
assert_eq "room join accepted" "accepted" "$(json_field "$JOIN" status)"
assert_eq "session tracks joined room" "room-join-1" "$(json_field "$(cat "$UNOARENA_SESSION_FILE")" roomId)"

py_assert_request "join uses spectator snapshot sequence then JoinRoom" '
snaps = [r for r in requests if r["path"] == "/v1/spectator/rooms/room-join-1/snapshot"]
assert snaps, "missing spectator snapshot"
joins = [r for r in requests if r["path"] == "/v1/rooms/room-join-1/commands" and r["headers"].get("x-correlation-id") == "corr-join"]
assert joins, "missing JoinRoom"
body = json.loads(joins[0]["body"])
assert body["type"] == "JoinRoom"
assert body["expectedSequenceNumber"] == 0
assert body["payload"] == {"roomId": "room-join-1"}
'

LEAVE="$("$CLI" room leave --command-id cmd-leave-1 --correlation-id corr-leave)"
assert_eq "room leave accepted" "accepted" "$(json_field "$LEAVE" status)"
LEAVE_SESS="$(cat "$UNOARENA_SESSION_FILE")"
case "$LEAVE_SESS" in
  *'"roomId"'*) echo "not ok - roomId should be cleared after leave" >&2; FAIL=$((FAIL + 1)) ;;
  *) echo "ok - session room cleared after leave"; PASS=$((PASS + 1)) ;;
esac

py_assert_request "leave uses player snapshot sequence then LeaveRoom" '
ps = [r for r in requests if r["path"] == "/v1/rooms/room-join-1/snapshot"]
assert ps, "missing player snapshot"
leaves = [r for r in requests if r["path"] == "/v1/rooms/room-join-1/commands" and r["headers"].get("x-correlation-id") == "corr-leave"]
assert leaves, "missing LeaveRoom"
body = json.loads(leaves[0]["body"])
assert body["type"] == "LeaveRoom"
assert "expectedSequenceNumber" in body
assert body["payload"] == {"roomId": "room-join-1"}
'

assert_exit "room join missing roomId fails" 2 env UNOARENA_API_URL="$UNOARENA_API_URL" UNOARENA_SESSION_FILE="$UNOARENA_SESSION_FILE" "$CLI" room join
assert_exit "room leave without room or session room fails" 1 \
  env UNOARENA_API_URL="$UNOARENA_API_URL" UNOARENA_SESSION_FILE="${SESSION_DIR}/empty-session.json" \
  "$CLI" room leave --token tok-alice
assert_contains "room leave missing room message" "room leave requires" "$(cat /tmp/unoarena-test-err.$$)"

echo "==> tournament positional register/status"
TREG_POS="$("$CLI" tournament register t-pos-1 --token tok-alice --command-id cmd-treg-pos --correlation-id corr-treg-pos)"
assert_eq "positional tournament register type" "RegisterPlayer" "$(json_field "$TREG_POS" type)"
py_assert_request "positional register maps tournamentId+playerId" '
regs = [r for r in requests if r["path"] == "/v1/commands" and r["headers"].get("x-correlation-id") == "corr-treg-pos"]
assert regs, "missing positional register"
body = json.loads(regs[0]["body"])
assert body["type"] == "RegisterPlayer"
assert body["payload"] == {"tournamentId": "t-pos-1", "playerId": "player-alice"}, body["payload"]
'

TSTAT="$("$CLI" tournament status t-pos-1 --correlation-id corr-tstat)"
assert_eq "tournament status id" "t-pos-1" "$(json_field "$TSTAT" tournamentId)"
assert_contains "tournament status standings" '"points":3' "$TSTAT"
assert_contains "tournament status bracket" '"slotId":"s1"' "$TSTAT"
py_assert_request "tournament status calls standings and bracket" '
st = [r for r in requests if r["path"] == "/v1/tournaments/t-pos-1/standings"]
br = [r for r in requests if r["path"] == "/v1/tournaments/t-pos-1/bracket"]
assert st and br, (st, br)
'

echo "==> spectate alias"
SPEC_ALIAS="$("$CLI" spectate room-1 --last-event-id 6 --correlation-id corr-spectate-alias 2>/tmp/unoarena-spec-err.$$)"
assert_contains "spectate alias SSE event" "SpectatorUpdate" "$SPEC_ALIAS"
py_assert_request "spectate alias hits spectator stream" '
ss = [r for r in requests if r["path"] == "/v1/streams/spectator" and r["headers"].get("x-correlation-id") == "corr-spectate-alias"]
assert ss, "missing spectate alias request"
assert ss[0]["query"].get("roomId") == ["room-1"]
assert ss[0]["headers"].get("last-event-id") == "6"
'

echo "==> Dockerfile structure"
DF="${ROOT}/Dockerfile"
[ -f "$DF" ] || die "missing client-checkpoint/Dockerfile"
assert_contains "Dockerfile has ENTRYPOINT" "ENTRYPOINT" "$(cat "$DF")"
assert_contains "Dockerfile installs curl" "curl" "$(cat "$DF")"
assert_contains "Dockerfile installs bash" "bash" "$(cat "$DF")"
assert_contains "Dockerfile installs python" "python" "$(cat "$DF")"
assert_contains "Dockerfile uses nonroot USER" "USER " "$(cat "$DF")"
[ -f "${ROOT}/.dockerignore" ]
echo "ok - .dockerignore present"
PASS=$((PASS + 1))

echo "==> Stage B interactive and bot flows"
bash "${ROOT}/tests/test-stage-b.sh"
echo "ok - Stage B focused suite"
PASS=$((PASS + 1))

echo
echo "Passed: $PASS  Failed: $FAIL"
[ "$FAIL" -eq 0 ]
