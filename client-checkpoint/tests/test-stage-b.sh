#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
CLI="${ROOT}/bin/unoarena"
FAKE="${ROOT}/tests/fake_game_gateway.py"
TMP="$(mktemp -d /tmp/unoarena-stage-b-XXXXXX)"
trap 'kill "${PID:-}" 2>/dev/null || true; wait "${PID:-}" 2>/dev/null || true; rm -rf "$TMP"' EXIT

python3 "$FAKE" >"$TMP/url" 2>"$TMP/fake.err" &
PID=$!
for _ in 1 2 3 4 5 6 7 8 9 10; do [ -s "$TMP/url" ] && break; sleep .05; done
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

(sleep .3; printf 'quit\n') | "$CLI" play --casual --token tok-sse >"$TMP/sse-refresh.out" 2>"$TMP/sse-refresh.err"
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
PY
echo 'ok - bot resolves penalties, enforces wild-draw-four legality, and fails cancelled rooms'

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
