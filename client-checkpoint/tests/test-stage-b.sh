#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
CLI="${ROOT}/bin/unoarena"
FAKE="${ROOT}/tests/fake_game_gateway.py"
TMP="$(mktemp -d /tmp/unoarena-stage-b-XXXXXX)"
trap 'kill "${PID:-}" 2>/dev/null || true; wait "${PID:-}" 2>/dev/null || true; rm -rf "$TMP"' EXIT
export PYTHONPYCACHEPREFIX="${TMP}/pycache"

python3 "$FAKE" >"$TMP/url" 2>"$TMP/fake.err" &
PID=$!
for _ in {1..50}; do [ -s "$TMP/url" ] && break; sleep .1; done
[ -s "$TMP/url" ] || { cat "$TMP/fake.err" >&2; echo "fake gateway did not start" >&2; exit 1; }
export UNOARENA_API_URL="$(tr -d '\n' <"$TMP/url")"
export UNOARENA_SESSION_FILE="$TMP/session.json"

printf 'quit\n' | "$CLI" play --casual --token tok-host >"$TMP/host.out"
curl --silent --show-error "$UNOARENA_API_URL/__requests" | python3 -c '
import json, sys
reqs=json.load(sys.stdin)["requests"]
cmds=[json.loads(r["body"]) for r in reqs if r["authorization"]=="Bearer tok-host" and r["body"]]
assert [c.get("type") for c in cmds if c.get("type") in {"LockRoom","StartMatch"}] == ["LockRoom","StartMatch"], cmds
'
echo 'ok - interactive casual host applies the bounded lock/start policy'

printf 'state\nplay 1\nquit\n' | "$CLI" play --casual --user alice --pass secret >"$TMP/play.out" 2>"$TMP/play.err"
grep -q 'Discard: R7' "$TMP/play.out"
grep -q '1. R5' "$TMP/play.out"
grep -q 'draw pile: 91' "$TMP/play.out"
grep -q 'Conflict/rejection' "$TMP/play.err"
grep -q 'stale_sequence' "$TMP/play.err"
echo 'ok - interactive board, canonical notation, and stale-command resync'

printf 'state\nquit\n' | "$CLI" play --casual --token tok-json --json >"$TMP/play.jsonl" 2>"$TMP/play-json.err"
python3 - "$TMP/play.jsonl" <<'PY'
import json, sys
lines=[]
for raw in open(sys.argv[1], encoding="utf-8"):
    raw=raw.strip()
    if raw:
        lines.append(json.loads(raw))
required={"ts","action","room","player","latency_ms","result","error_code","seq","correlationId"}
assert lines and all(required <= set(line) for line in lines), lines
assert any(line["action"] == "state" and "state" in line for line in lines), lines
PY
echo 'ok - interactive --json uses the machine-readable line contract'

printf 'pass\nquit\n' | "$CLI" play --casual --token tok-json-pass --json >"$TMP/pass.jsonl" 2>"$TMP/pass.err"
python3 - "$TMP/pass.jsonl" <<'PY'
import json, sys
lines=[json.loads(line) for line in open(sys.argv[1], encoding="utf-8") if line.strip()]
assert any(line["action"] == "pass" and line["result"] == "ok" for line in lines), lines
PY
echo 'ok - JSON interactive pass emits JSONL only'

(sleep 1; printf 'quit\n') | "$CLI" play --casual --token tok-sse >"$TMP/sse-refresh.out" 2>"$TMP/sse-refresh.err"
grep -q 'Discard: B9' "$TMP/sse-refresh.out"
echo 'ok - turn-changing SSE refreshes and re-renders the authoritative board'

(sleep 2; printf 'quit\n') | "$CLI" play --casual --token tok-resync >"$TMP/resync.out" 2>"$TMP/resync.err"
curl --silent --show-error "$UNOARENA_API_URL/__requests" | python3 -c '
import json, sys
streams=[r for r in json.load(sys.stdin)["requests"]
         if r["path"]=="/v1/streams/player" and r["authorization"]=="Bearer tok-resync"]
assert len(streams) >= 3, streams
assert streams[0]["lastEventId"] == "", streams
assert streams[1]["lastEventId"] == "opaque-resume-id", streams
assert streams[2]["lastEventId"] == "", streams
'
echo 'ok - snapshot_required clears the opaque SSE resume marker after reconciliation'

printf 'quit\n' | "$CLI" play --casual --token tok-reconnect >"$TMP/reconnect.out"
curl --silent --show-error "$UNOARENA_API_URL/__requests" | python3 -c '
import json, sys
reqs=json.load(sys.stdin)["requests"]
cmds=[json.loads(r["body"]) for r in reqs if r["path"].startswith("/v1/rooms/") and r["body"]]
assert any(c.get("type")=="ReconnectToRoom" and c.get("payload",{}).get("disconnectVersion")==3 for c in cmds)
'
echo 'ok - disconnected player performs the versioned reconnect handshake'

printf 'quit\n' | UNOARENA_ROOM_START_TIMEOUT_SECONDS=2 "$CLI" play --casual --token tok-starting >"$TMP/starting.out"
curl --silent --show-error "$UNOARENA_API_URL/__requests" | python3 -c '
import json, sys
reqs=json.load(sys.stdin)["requests"]
reads=[r for r in reqs if r["path"]=="/v1/rooms/room-game/snapshot" and r.get("authorization")=="Bearer tok-starting"]
# The first snapshot returns room_starting and must be retried. The interactive
# event follower may perform one additional authoritative refresh before quit.
assert 2 <= len(reads) <= 3, reads
'
echo 'ok - player snapshot safely retries structured room_starting'

printf 'draw\nuno\nchallenge\npass\nquit\n' | "$CLI" play --casual --token tok-actions >"$TMP/actions.out"
grep -q 'Pass is automatic after draw' "$TMP/actions.out"
curl --silent --show-error "$UNOARENA_API_URL/__requests" | python3 -c '
import json, sys
reqs=json.load(sys.stdin)["requests"]
cmds=[json.loads(r["body"]) for r in reqs if r["path"].startswith("/v1/rooms/") and r["body"]]
types={c.get("type") for c in cmds}
assert {"DrawCard","CallUno","ReportMissingUno"} <= types, types
challenge=next(c for c in cmds if c.get("type")=="ReportMissingUno")
assert challenge["payload"]["targetId"] == "player-other", challenge
'
echo 'ok - draw, uno, challenge, and backend auto-pass mapping'

"$CLI" bot --room room-game --token tok-bot --seed 123 --max-actions 1 --max-wait 2 --poll-interval .01 >"$TMP/bot.jsonl"
python3 - "$TMP/bot.jsonl" <<'PY'
import json, sys
lines = [json.loads(line) for line in open(sys.argv[1], encoding="utf-8") if line.strip()]
assert len(lines) == 2, lines
required = {"ts", "action", "room", "player", "latency_ms", "result", "error_code", "seq", "correlationId"}
assert all(required <= set(line) for line in lines), lines
assert lines[0]["action"] == "play_card" and lines[0]["result"] == "ok", lines[0]
assert lines[-1]["action"] == "summary" and lines[-1]["total_actions"] == 1, lines[-1]
PY
echo 'ok - seeded bot emits action JSONL and final summary'

"$CLI" bot --tournament tournament-1 --token tok-tourney --seed 7 --max-actions 1 --max-wait 2 --poll-interval .01 >"$TMP/tournament.jsonl"
python3 - "$TMP/tournament.jsonl" <<'PY'
import json, sys
lines=[json.loads(line) for line in open(sys.argv[1], encoding="utf-8") if line.strip()]
assert lines[0]["action"] == "tournament_register" and lines[0]["result"] == "ok", lines
assert lines[-1]["action"] == "summary" and lines[-1]["result"] == "ok", lines
PY
echo 'ok - tournament bot registers and follows its authenticated assignment'

"$CLI" bot --tournament tournament-1 --token tok-tourney-unknown --seed 7 \
  --max-actions 1 --max-wait 2 --poll-interval .01 >"$TMP/tournament-unknown.jsonl"
python3 - "$TMP/tournament-unknown.jsonl" <<'PY'
import json, sys
lines=[json.loads(line) for line in open(sys.argv[1], encoding="utf-8") if line.strip()]
assert lines[0]["action"] == "tournament_register" and lines[0]["result"] == "ok", lines
assert lines[-1]["action"] == "summary" and lines[-1]["result"] == "ok", lines
PY
curl --silent --show-error "$UNOARENA_API_URL/__requests" | python3 -c '
import json, sys
reqs=json.load(sys.stdin)["requests"]
posts=[r for r in reqs if r["path"]=="/v1/commands" and r.get("authorization")=="Bearer tok-tourney-unknown"]
registrations=[r for r in posts if json.loads(r["body"]).get("type")=="RegisterPlayer"]
assert len(registrations) == 1, registrations
'
echo 'ok - tournament bot reconciles an unknown registration result without replay'

UNOARENA_ROOM_START_TIMEOUT_SECONDS=2 "$CLI" bot --tournament tournament-1 --token tok-tourney-starting \
  --seed 7 --max-actions 1 --max-wait 2 --poll-interval .01 >"$TMP/tournament-starting.jsonl"
python3 - "$TMP/tournament-starting.jsonl" <<'PY'
import json, sys
lines=[json.loads(line) for line in open(sys.argv[1], encoding="utf-8") if line.strip()]
assert lines[-1]["action"] == "summary" and lines[-1]["result"] == "ok", lines
PY
echo 'ok - tournament assignment safely retries structured room_starting'

if UNOARENA_ROOM_START_TIMEOUT_SECONDS=0 "$CLI" bot --room room-game --token tok-never \
  --max-actions 1 --max-wait 1 --poll-interval .01 >"$TMP/starting-timeout.jsonl"; then
  echo 'not ok - exhausted room_starting retry returned success' >&2
  exit 1
fi
python3 - "$TMP/starting-timeout.jsonl" <<'PY'
import json, sys
lines=[json.loads(line) for line in open(sys.argv[1], encoding="utf-8") if line.strip()]
assert lines[0]["action"] == "bot_setup" and lines[0]["error_code"] == "room_start_timeout", lines
assert lines[-1]["action"] == "summary" and lines[-1]["result"] == "error", lines
PY
echo 'ok - exhausted room_start retry is structured and nonzero'

if printf 'quit\n' | UNOARENA_ROOM_START_TIMEOUT_SECONDS=0 "$CLI" play --casual \
  --token tok-never --json >"$TMP/starting-timeout-interactive.jsonl" 2>"$TMP/starting-timeout-interactive.err"; then
  echo 'not ok - exhausted interactive room_starting retry returned success' >&2
  exit 1
fi
python3 - "$TMP/starting-timeout-interactive.jsonl" <<'PY'
import json, sys
lines=[json.loads(line) for line in open(sys.argv[1], encoding="utf-8") if line.strip()]
required={"ts","action","room","player","latency_ms","result","error_code","seq","correlationId"}
assert len(lines) == 1 and required <= set(lines[0]), lines
assert lines[0]["result"] == "error" and lines[0]["error_code"] == "room_start_timeout", lines
PY
echo 'ok - interactive JSON exhaustion emits one structured failure'

if "$CLI" bot --room room-game --token tok-mutation --max-actions 1 --max-wait 1 \
  --poll-interval .01 >"$TMP/mutation-503.jsonl"; then
  echo 'not ok - unknown mutation outcome returned success' >&2
  exit 1
fi
python3 - "$TMP/mutation-503.jsonl" <<'PY'
import json, sys
lines=[json.loads(line) for line in open(sys.argv[1], encoding="utf-8") if line.strip()]
assert lines[0]["action"] == "playcard" and lines[0]["error_code"] == 503, lines
assert lines[-1]["result"] == "error", lines
PY
curl --silent --show-error "$UNOARENA_API_URL/__requests" | python3 -c '
import json, sys
reqs=json.load(sys.stdin)["requests"]
posts=[r for r in reqs if r["path"]=="/v1/rooms/room-game/commands" and r.get("authorization")=="Bearer tok-mutation"]
plays=[r for r in posts if json.loads(r["body"]).get("type")=="PlayCard"]
assert len(plays) == 1, plays
'
echo 'ok - mutation room_starting is never retried'

if "$CLI" bot --tournament tournament-1 --token tok-tourney-timeout --seed 7 --max-actions 1 --max-wait .05 --poll-interval .01 >"$TMP/tournament-timeout.jsonl"; then
  echo 'not ok - tournament timeout returned success' >&2
  exit 1
fi
if "$CLI" bot --tournament tournament-1 --token tok-tourney-cancel --seed 7 --max-actions 1 --max-wait 1 --poll-interval .01 >"$TMP/tournament-cancel.jsonl"; then
  echo 'not ok - cancelled tournament returned success' >&2
  exit 1
fi
python3 - "$TMP/tournament-timeout.jsonl" "$TMP/tournament-cancel.jsonl" <<'PY'
import json, sys
for path in sys.argv[1:]:
    lines=[json.loads(line) for line in open(path, encoding="utf-8") if line.strip()]
    assert lines[-1]["action"] == "summary" and lines[-1]["result"] == "error", lines
PY
echo 'ok - tournament timeout and cancellation remain nonzero until terminal completion'

curl --silent --show-error "$UNOARENA_API_URL/__requests" | python3 -c '
import json, sys
reqs=json.load(sys.stdin)["requests"]
commands=[json.loads(r["body"]) for r in reqs if r["path"].startswith("/v1/rooms/") and r["path"].endswith("/commands")]
assert any(c.get("type")=="PlayCard" and c.get("payload",{}).get("cardId")=="card-5" for c in commands)
assert all("expectedSequenceNumber" in c for c in commands)
'
echo 'ok - game actions use room-scoped versioned command envelopes'

python3 - "${ROOT}/lib/game_client.py" <<'PY'
import importlib.util, pathlib, random, sys
from types import SimpleNamespace
path = pathlib.Path(sys.argv[1])
spec = importlib.util.spec_from_file_location("unoarena_game_client", path)
mod = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = mod
spec.loader.exec_module(mod)
hand = [
    {"id": "y9", "color": "yellow", "face": "9"},
    {"id": "g2", "color": "green", "face": "draw_two"},
]
snapshot = {"game": {"activeColor": "yellow", "discardTop": {"face": "wild_draw_four"},
                     "penaltyAmount": 4}, "hand": hand}
kind, payload, color = mod.bot_action(None, "r", snapshot, "p", random.Random(1))
assert (kind, payload, color) == ("DrawCard", {}, None), (kind, payload, color)
wild_hand = [{"id": "w4", "color": "", "face": "wild_draw_four"},
             {"id": "y1", "color": "yellow", "face": "1"}]
assert not mod.playable(wild_hand[0], snapshot["game"], wild_hand)
class CancelledGateway:
    def snapshot(self, _room):
        return {"roomId": "cancelled-room", "status": "cancelled", "sequenceNumber": 1}
assert not mod.play_bot_room(
    CancelledGateway(), "cancelled-room", "p", "u",
    SimpleNamespace(max_wait=1, seed=1, max_actions=10, poll_interval=.01),
    mod.Metrics(player="u"),
)
class UnknownThenCompletedGateway:
    def __init__(self, code):
        self.code = code
        self.commands = 0
        self.snapshots = 0
        self.room_start_retry = mod.RoomStartRetryPolicy(timeout_seconds=60)
    def snapshot(self, _room):
        self.snapshots += 1
        if self.snapshots == 1:
            return {"roomId": "unknown-room", "status": "in_progress", "sequenceNumber": 7,
                    "hostId": "host", "game": {"currentPlayer": "p", "activeColor": "red",
                    "discardTop": {"face": "4"}}, "hand": [{"id": "r4", "color": "red", "face": "4"}]}
        return {"roomId": "unknown-room", "status": "completed", "sequenceNumber": 8,
                "hostId": "host", "game": {}, "hand": []}
    def command(self, *_args):
        self.commands += 1
        raise mod.ClientError(f"HTTP {self.code}", self.code)
for code in (502, 503):
    unknown = UnknownThenCompletedGateway(code)
    assert mod.play_bot_room(
        unknown, "unknown-room", "p", "u",
        SimpleNamespace(max_wait=1, seed=1, max_actions=10, poll_interval=.01),
        mod.Metrics(player="u"),
    )
    assert unknown.commands == 1, (code, unknown.commands)

# The hidden bot action interval paces every known-result gameplay mutation,
# including the immediate ChooseColor/CallUno follow-ups. An unknown transport
# result must still enter authoritative safe-read recovery immediately instead
# of treating a sleep as reconciliation.
assert mod.parser().parse_args(["bot", "--room", "room"]).action_interval == 0
assert "--action-interval" not in mod.parser().format_help()

class BurstGateway:
    def __init__(self):
        self.command_times = []
        self.kinds = []
        self.sequence = 7
        self.completed = False
    def snapshot(self, _room):
        return {
            "roomId": "paced-room", "status": "completed" if self.completed else "in_progress",
            "sequenceNumber": self.sequence, "hostId": "host",
            "game": {"currentPlayer": "p", "activeColor": "red", "discardTop": {"face": "4"}},
            "hand": [] if self.completed else [{"id": "wild", "color": "", "face": "wild"}],
        }
    def command(self, _room, kind, _sequence, _payload):
        self.command_times.append(mod.time.monotonic())
        self.kinds.append(kind)
        self.sequence += 1
        if kind == "CallUno":
            self.completed = True
        return {"status": "accepted", "sequenceNumber": self.sequence}, f"corr-{kind}"

class RejectedGateway(BurstGateway):
    def command(self, _room, kind, _sequence, _payload):
        self.command_times.append(mod.time.monotonic())
        self.kinds.append(kind)
        self.completed = True
        return {"status": "rejected", "reason": "not_allowed"}, f"corr-{kind}"

class UnknownWithoutDelayGateway(BurstGateway):
    def __init__(self):
        super().__init__()
        self.room_start_retry = mod.RoomStartRetryPolicy(timeout_seconds=60)
    def command(self, _room, kind, _sequence, _payload):
        self.command_times.append(mod.time.monotonic())
        self.kinds.append(kind)
        self.completed = True
        raise mod.ClientError("HTTP 502", 502)

real_monotonic, real_sleep = mod.time.monotonic, mod.time.sleep
virtual_now = [0.0]
mod.time.monotonic = lambda: virtual_now[0]
mod.time.sleep = lambda seconds: virtual_now.__setitem__(0, virtual_now[0] + seconds)
paced_args = SimpleNamespace(
    max_wait=30, seed=1, max_actions=10, poll_interval=.01, action_interval=2,
)
try:
    burst = BurstGateway()
    assert mod.play_bot_room(burst, "paced-room", "p", "u", paced_args, mod.Metrics(player="u"))
    assert burst.kinds == ["PlayCard", "ChooseColor", "CallUno"], burst.kinds
    assert burst.command_times == [0.0, 2.0, 4.0], burst.command_times
    assert virtual_now[0] == 6.0, virtual_now[0]

    virtual_now[0] = 0.0
    rejected = RejectedGateway()
    assert mod.play_bot_room(rejected, "paced-room", "p", "u", paced_args, mod.Metrics(player="u"))
    assert rejected.command_times == [0.0], rejected.command_times
    assert virtual_now[0] >= 2.0, virtual_now[0]

    virtual_now[0] = 0.0
    unknown = UnknownWithoutDelayGateway()
    assert mod.play_bot_room(unknown, "paced-room", "p", "u", paced_args, mod.Metrics(player="u"))
    assert unknown.command_times == [0.0], unknown.command_times
    assert virtual_now[0] == paced_args.poll_interval, virtual_now[0]
finally:
    mod.time.monotonic, mod.time.sleep = real_monotonic, real_sleep

# Reconciliation uses the configured safe-read budget rather than a fixed
# short window. Model a runtime replacement whose authoritative sequence does
# not advance until more than ten virtual seconds after the unknown outcome.
class DelayedAdvanceGateway:
    def __init__(self):
        self.room_start_retry = mod.RoomStartRetryPolicy(timeout_seconds=60)
        self.snapshots = 0
    def snapshot(self, _room):
        self.snapshots += 1
        sequence = 8 if self.snapshots >= 12 else 7
        return {"roomId": "delayed-room", "status": "in_progress", "sequenceNumber": sequence,
                "hostId": "host", "game": {}, "hand": []}

real_monotonic, real_sleep = mod.time.monotonic, mod.time.sleep
virtual_now = [0.0]
mod.time.monotonic = lambda: virtual_now[0]
mod.time.sleep = lambda seconds: virtual_now.__setitem__(0, virtual_now[0] + seconds)
try:
    delayed = DelayedAdvanceGateway()
    reconciled = mod.recover_unknown_bot_outcome(
        delayed, "delayed-room", mod.ClientError("HTTP 502", 502), 7,
        deadline=120, poll_interval=1,
    )
    assert reconciled and reconciled["sequenceNumber"] == 8, reconciled
    assert virtual_now[0] > 10, virtual_now[0]
finally:
    mod.time.monotonic, mod.time.sleep = real_monotonic, real_sleep

gateway = mod.Gateway("http://unused")
gateway.room_start_retry = mod.RoomStartRetryPolicy(
    timeout_seconds=1, interval_cap_seconds=.001, jitter_seconds=0,
)
attempts = 0
def transient_read(method, path):
    global attempts
    attempts += 1
    if attempts < 3:
        raise mod.ClientError("HTTP 502", 502)
    return {"roomId": "safe-read", "status": "completed"}, "corr", 200
gateway.request = transient_read
snapshot, _, _ = gateway.safe_read("/v1/rooms/safe-read/snapshot")
assert snapshot["status"] == "completed" and attempts == 3, (snapshot, attempts)

# Bot setup starts with an authenticated identity GET. It is just as safe to
# retry as snapshots and assignments, and a transient Gateway failure must not
# abort an otherwise healthy tournament flow.
attempts = 0
def transient_whoami(method, path):
    global attempts
    attempts += 1
    assert (method, path) == ("GET", "/v1/auth/whoami")
    if attempts < 3:
        raise mod.ClientError("HTTP 502", 502)
    return {"playerId": "safe-player", "username": "safe-user"}, "corr", 200
gateway.request = transient_whoami
who = gateway.whoami()
assert who == {"playerId": "safe-player", "username": "safe-user"} and attempts == 3, (who, attempts)
PY
echo 'ok - bot resolves penalties, enforces wild-draw-four legality, fails cancelled rooms, reconciles unknown outcomes without replay, and retries transient setup/read failures'

python3 - "${ROOT}/lib/game_client.py" <<'PY'
import importlib.util, pathlib, random, sys

path = pathlib.Path(sys.argv[1])
spec = importlib.util.spec_from_file_location("unoarena_game_client", path)
mod = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = mod
spec.loader.exec_module(mod)

penalty_game = {
    "activeColor": "yellow",
    "discardTop": {"face": "7"},
    "penaltyAmount": 2,
}
ordinary = {"id": "y5", "color": "yellow", "face": "5"}
legal_draw_two = {"id": "y2", "color": "yellow", "face": "draw_two"}
illegal_draw_two = {"id": "g2", "color": "green", "face": "draw_two"}
assert not mod.playable(ordinary, penalty_game, [ordinary])
assert mod.playable(legal_draw_two, penalty_game, [legal_draw_two])
assert not mod.playable(illegal_draw_two, penalty_game, [illegal_draw_two])
matching_face_draw_two = {"id": "g2-face", "color": "green", "face": "draw_two"}
assert mod.playable(matching_face_draw_two, {
    "activeColor": "yellow", "discardTop": {"face": "draw_two"}, "penaltyAmount": 2,
}, [matching_face_draw_two])

legal_wild_draw_four = {"id": "w4-legal", "color": "", "face": "wild_draw_four"}
assert mod.playable(legal_wild_draw_four, penalty_game, [
    legal_wild_draw_four, {"id": "g1", "color": "green", "face": "1"},
])
illegal_wild_draw_four = {"id": "w4-illegal", "color": "", "face": "wild_draw_four"}
assert not mod.playable(illegal_wild_draw_four, penalty_game, [
    illegal_wild_draw_four, {"id": "y1", "color": "yellow", "face": "1"},
])

hand = [ordinary, illegal_draw_two, legal_draw_two, illegal_wild_draw_four]
kind, payload, color = mod.bot_action(None, "room", {"game": penalty_game, "hand": hand}, "player", random.Random(1))
assert (kind, payload, color) == ("PlayCard", {"cardId": "y2"}, None)
PY
echo 'ok - penalty legality unifies interactive playability and bot choices'

printf 'play 1\nquit\n' | "$CLI" play --casual --token tok-wdf >"$TMP/wdf.out" 2>"$TMP/wdf.err"
grep -q 'not playable' "$TMP/wdf.err"
curl --silent --show-error "$UNOARENA_API_URL/__requests" | python3 -c '
import json, sys
reqs=json.load(sys.stdin)["requests"]
cmds=[json.loads(r["body"]) for r in reqs if r["authorization"]=="Bearer tok-wdf" and r["body"]]
assert not any(c.get("type")=="PlayCard" and c.get("payload",{}).get("cardId")=="w4" for c in cmds), cmds
'
echo 'ok - interactive wild-draw-four legality evaluates the complete hand'

printf 'play 1 X\nG\nquit\n' | "$CLI" play --casual --token tok-wild --json >"$TMP/wild.jsonl" 2>"$TMP/wild.err"
python3 - "$TMP/wild.jsonl" <<'PY'
import json, sys
lines=[json.loads(line) for line in open(sys.argv[1], encoding="utf-8") if line.strip()]
assert lines and all(isinstance(line, dict) for line in lines), lines
assert any(line.get("action") == "PlayCard" for line in lines), lines
PY
grep -q 'Color \[R/G/B/Y\]' "$TMP/wild.err"
grep -q 'Wild color must be R, G, B, or Y' "$TMP/wild.err"
curl --silent --show-error "$UNOARENA_API_URL/__requests" | python3 -c '
import json, sys
cmds=[json.loads(r["body"]) for r in json.load(sys.stdin)["requests"]
      if r["authorization"]=="Bearer tok-wild" and r["body"]]
types=[c.get("type") for c in cmds]
assert types.count("PlayCard") == 1 and types.index("PlayCard") < types.index("ChooseColor"), types
'
echo 'ok - JSON wild-color validation happens before play and keeps prompts off stdout'

python3 - "${ROOT}/lib/game_client.py" <<'PY'
import importlib.util, os, pathlib, sys
path=pathlib.Path(sys.argv[1])
spec=importlib.util.spec_from_file_location("unoarena_retry_policy", path)
mod=importlib.util.module_from_spec(spec)
sys.modules[spec.name]=mod
spec.loader.exec_module(mod)
os.environ.pop("UNOARENA_ROOM_START_TIMEOUT_SECONDS", None)
assert mod.RoomStartRetryPolicy.from_environment().timeout_seconds == 60
os.environ["UNOARENA_ROOM_START_TIMEOUT_SECONDS"] = "12.5"
assert mod.RoomStartRetryPolicy.from_environment().timeout_seconds == 12.5
policy=mod.RoomStartRetryPolicy(timeout_seconds=1)
mod.random.uniform=lambda _low, _high: 0.2
assert policy.retry_delay("2") == 2.2
assert policy.retry_delay("99") == 5.0
PY
echo 'ok - room_start retry policy defaults, configuration, jitter, and five-second cap'
