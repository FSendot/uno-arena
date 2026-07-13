#!/usr/bin/env python3
"""Focused, stateful fake BFF for Stage B CLI tests."""

from __future__ import annotations

import json
import sys
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import parse_qs, unquote, urlparse


STATE = {"sequence": 7, "complete": False, "requests": [], "stale_seen": False,
         "reconnected": set(), "host_status": "waiting", "sse_delivered": False,
         "resync_stream_calls": 0, "starting_attempts": {}}


def player(headers) -> str:
    token = headers.get("Authorization", "").removeprefix("Bearer ")
    return "player-" + (token.removeprefix("tok-") or "bot")


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *_args) -> None:
        return

    def send_json(self, code: int, data) -> None:
        raw = json.dumps(data).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def record(self, body=b""):
        parsed = urlparse(self.path)
        STATE["requests"].append({"method": self.command, "path": parsed.path,
                                  "body": body.decode() if body else "",
                                  "authorization": self.headers.get("Authorization", ""),
                                  "lastEventId": self.headers.get("Last-Event-ID", "")})
        return parsed, parse_qs(parsed.query)

    def do_POST(self) -> None:  # noqa: N802
        size = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(size) if size else b""
        parsed, _ = self.record(raw)
        data = json.loads(raw or b"{}")
        if parsed.path == "/v1/auth/login":
            user = data["username"]
            self.send_json(200, {"token": f"tok-{user}", "playerId": f"player-{user}", "sessionId": f"sess-{user}"})
            return
        if parsed.path == "/v1/commands":
            self.send_json(200, {"status": "accepted", "sequenceNumber": STATE["sequence"],
                                 "payload": {"roomId": data.get("payload", {}).get("roomId", "room-game")}})
            return
        if parsed.path.startswith("/v1/rooms/") and parsed.path.endswith("/commands"):
            kind = data.get("type")
            if kind == "PlayCard" and player(self.headers) == "player-mutation":
                raw = b'{"code":"room_starting","message":"mutation outcome unknown"}'
                self.send_response(503)
                self.send_header("Content-Type", "application/json")
                self.send_header("Retry-After", "0")
                self.send_header("Content-Length", str(len(raw)))
                self.end_headers()
                self.wfile.write(raw)
                return
            # Alice exercises rejected stale-command reconciliation once.
            if kind == "PlayCard" and player(self.headers) == "player-alice" and not STATE["stale_seen"]:
                STATE["stale_seen"] = True
                STATE["sequence"] += 1
                self.send_json(409, {"status": "rejected", "reason": "stale_sequence",
                                     "sequenceNumber": STATE["sequence"]})
                return
            STATE["sequence"] += 1
            if kind == "ReconnectToRoom":
                STATE["reconnected"].add(player(self.headers))
            if player(self.headers) == "player-host" and kind == "LockRoom":
                STATE["host_status"] = "locked"
            if player(self.headers) == "player-host" and kind == "StartMatch":
                STATE["host_status"] = "in_progress"
            if kind == "PlayCard":
                STATE["complete"] = True
            self.send_json(200, {"status": "accepted", "type": kind, "sequenceNumber": STATE["sequence"]})
            return
        self.send_json(404, {"code": "not_found"})

    def do_GET(self) -> None:  # noqa: N802
        parsed, query = self.record()
        if parsed.path == "/v1/auth/whoami":
            pid = player(self.headers)
            self.send_json(200, {"playerId": pid, "username": pid.removeprefix("player-")})
            return
        if parsed.path == "/v1/rooms":
            self.send_json(200, {"rooms": [{"roomId": "room-game", "status": "waiting",
                "visibility": "public", "currentPlayers": 1, "maxSeats": 4}], "nextCursor": None})
            return
        if parsed.path.startswith("/v1/spectator/rooms/") and parsed.path.endswith("/snapshot"):
            self.send_json(200, {"roomId": "room-game", "sequenceNumber": STATE["sequence"], "status": "waiting"})
            return
        if parsed.path.startswith("/v1/rooms/") and parsed.path.endswith("/snapshot"):
            pid = player(self.headers)
            attempts = STATE["starting_attempts"].get(pid, 0)
            if pid == "player-never":
                raw = b'{"code":"room_starting","message":"runtime pending"}'
                self.send_response(503)
                self.send_header("Content-Type", "application/json")
                self.send_header("Retry-After", "0")
                self.send_header("Content-Length", str(len(raw)))
                self.end_headers()
                self.wfile.write(raw)
                return
            if pid == "player-starting" and attempts == 0:
                STATE["starting_attempts"][pid] = attempts + 1
                raw = b'{"code":"room_starting","message":"runtime pending"}'
                self.send_response(503)
                self.send_header("Content-Type", "application/json")
                self.send_header("Retry-After", "0")
                self.send_header("Content-Length", str(len(raw)))
                self.end_headers()
                self.wfile.write(raw)
                return
            host_status = STATE["host_status"] if pid == "player-host" else None
            sse_state = pid == "player-sse" and STATE["sse_delivered"]
            snapshot = {"roomId": "room-game", "status": "completed" if STATE["complete"] else "in_progress",
                "sequenceNumber": STATE["sequence"], "visibility": "public", "hostId": pid if host_status else "player-host",
                "roomType": "ad_hoc", "playerId": pid,
                "roster": [{"seatIndex": 0, "playerId": pid}, {"seatIndex": 1, "playerId": "player-other"}],
                "uno": {"playerId": "player-other", "openingSequence": 5,
                        "expiresAt": "2099-01-01T00:00:00Z", "called": False},
                "game": {"gameId": "g1", "sequenceNumber": STATE["sequence"],
                    "discardTop": {"id": "top", "color": "red", "face": "7"}, "activeColor": "red",
                    "currentPlayer": pid, "direction": 1, "handCounts": {pid: 1, "player-other": 4},
                    "drawPileSize": 91, "completed": STATE["complete"]},
                "hand": [] if STATE["complete"] else [{"id": "card-5", "color": "red", "face": "5"}]}
            if host_status:
                snapshot["status"] = host_status
            if pid == "player-mutation":
                snapshot["status"] = "in_progress"
                snapshot["game"]["completed"] = False
                snapshot["hand"] = [{"id": "card-5", "color": "red", "face": "5"}]
            if sse_state:
                snapshot["game"]["discardTop"] = {"id": "top-next", "color": "blue", "face": "9"}
                snapshot["game"]["activeColor"] = "blue"
            if pid == "player-wdf":
                snapshot["game"]["activeColor"] = "yellow"
                snapshot["hand"] = [
                    {"id": "w4", "color": "", "face": "wild_draw_four"},
                    {"id": "y1", "color": "yellow", "face": "1"},
                ]
            if pid == "player-wild":
                snapshot["hand"] = [{"id": "wild", "color": "", "face": "wild"}]
            if pid == "player-reconnect" and pid not in STATE["reconnected"]:
                snapshot["disconnect"] = {"disconnectVersion": 3, "deadlineUtc": "2099-01-01T00:00:00Z"}
            self.send_json(200, snapshot)
            return
        if parsed.path.startswith("/v1/tournaments/") and "/players/" in parsed.path and parsed.path.endswith("/assignment"):
            bits = parsed.path.strip("/").split("/")
            pid = unquote(bits[4])
            attempts = STATE["starting_attempts"].get(pid, 0)
            if pid == "player-tourney-starting" and attempts == 0:
                STATE["starting_attempts"][pid] = attempts + 1
                raw = b'{"code":"room_starting","message":"assignment pending"}'
                self.send_response(503)
                self.send_header("Content-Type", "application/json")
                self.send_header("Retry-After", "0")
                self.send_header("Content-Length", str(len(raw)))
                self.end_headers()
                self.wfile.write(raw)
                return
            if pid == "player-tourney-timeout":
                self.send_json(200, {"tournamentId": unquote(bits[2]), "playerId": pid,
                    "phase": "running", "registrationStatus": "registered", "assignment": {}})
                return
            if pid == "player-tourney-cancel":
                self.send_json(200, {"tournamentId": unquote(bits[2]), "playerId": pid,
                    "phase": "cancelled", "registrationStatus": "registered", "assignment": {}})
                return
            self.send_json(200, {"tournamentId": unquote(bits[2]), "playerId": unquote(bits[4]),
                "phase": "completed", "registrationStatus": "registered", "currentRound": 1,
                "assignment": {"roundNumber": 1, "slotId": "slot-1", "slotStatus": "completed",
                               "roomId": "room-game"}})
            return
        if parsed.path == "/v1/streams/player":
            room = (query.get("roomId") or [""])[0]
            if player(self.headers) == "player-resync":
                STATE["resync_stream_calls"] += 1
                if STATE["resync_stream_calls"] == 2:
                    self.send_json(409, {"code": "snapshot_required"})
                    return
            payload = json.dumps({"eventType": "CardPlayed", "roomId": room, "roomSequence": STATE["sequence"],
                                  "playerId": "player-other", "card": {"color": "blue", "face": "skip"},
                                  "activeColor": "blue"})
            if player(self.headers) == "player-sse":
                STATE["sse_delivered"] = True
            event_id = "opaque-resume-id" if player(self.headers) == "player-resync" else str(STATE["sequence"])
            raw = f"id: {event_id}\nevent: CardPlayed\ndata: {payload}\n\n".encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)
            return
        if parsed.path == "/__requests":
            self.send_json(200, {"requests": STATE["requests"]})
            return
        self.send_json(404, {"code": "not_found"})


def main() -> None:
    server = HTTPServer(("127.0.0.1", 0), Handler)
    print(f"http://127.0.0.1:{server.server_address[1]}", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
