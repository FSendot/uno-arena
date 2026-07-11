#!/usr/bin/env python3
"""Ephemeral stdlib fake BFF for offline CLI tests. Not a persistent server."""

from __future__ import annotations

import json
import sys
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any
from urllib.parse import parse_qs, unquote, urlparse


ROOM_COMMANDS_REQUIRING_SEQUENCE = {
    "JoinRoom",
    "LeaveRoom",
    "LockRoom",
    "StartMatch",
    "CancelRoom",
    "PlayCard",
    "DrawCard",
    "ChooseColor",
    "CallUno",
    "ReportMissingUno",
    "ReconnectToRoom",
}


class GatewayState:
    def __init__(self) -> None:
        self.requests: list[dict[str, Any]] = []
        self.lock = threading.Lock()
        self.fail_next_stream = False

    def record(
        self,
        method: str,
        path: str,
        headers: dict[str, str],
        body: bytes,
        query: dict[str, list[str]],
    ) -> None:
        with self.lock:
            self.requests.append(
                {
                    "method": method,
                    "path": path,
                    "headers": headers,
                    "body": body.decode("utf-8") if body else "",
                    "query": query,
                }
            )


STATE = GatewayState()


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, format: str, *args: Any) -> None:  # noqa: A003
        return

    def _read_body(self) -> bytes:
        length = int(self.headers.get("Content-Length", "0") or "0")
        if length <= 0:
            return b""
        return self.rfile.read(length)

    def _json(self, status: int, payload: dict[str, Any]) -> None:
        raw = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def _record(self, body: bytes) -> tuple[str, dict[str, list[str]]]:
        parsed = urlparse(self.path)
        query = parse_qs(parsed.query)
        headers = {k.lower(): v for k, v in self.headers.items()}
        STATE.record(self.command, parsed.path, headers, body, query)
        return parsed.path, query

    def do_POST(self) -> None:  # noqa: N802
        body = self._read_body()
        path, _ = self._record(body)

        if path == "/__fail-next-stream":
            STATE.fail_next_stream = True
            self._json(200, {"status": "ok"})
            return

        if path == "/v1/auth/register":
            data = json.loads(body or b"{}")
            self._json(200, {"status": "ok", "username": data.get("username", "")})
            return
        if path == "/v1/auth/login":
            data = json.loads(body or b"{}")
            self._json(
                200,
                {
                    "sessionId": "sess-1",
                    "playerId": "player-1",
                    "token": f"tok-{data.get('username', 'anon')}",
                },
            )
            return
        if path == "/v1/commands":
            data = json.loads(body or b"{}")
            cmd_type = data.get("type", "")
            if cmd_type in ROOM_COMMANDS_REQUIRING_SEQUENCE and "expectedSequenceNumber" not in data:
                self._json(
                    200,
                    {
                        "commandId": data.get("commandId", ""),
                        "type": cmd_type,
                        "status": "rejected",
                        "schemaVersion": data.get("schemaVersion", 1),
                        "reason": "missing_expected_sequence_number",
                    },
                )
                return
            self._json(
                200,
                {
                    "commandId": data.get("commandId", ""),
                    "type": cmd_type,
                    "status": "accepted",
                    "schemaVersion": data.get("schemaVersion", 1),
                    "sequenceNumber": 1,
                    "payload": {"roomId": "room-1"} if cmd_type == "CreateRoom" else {},
                },
            )
            return
        if path.startswith("/v1/rooms/") and path.endswith("/commands"):
            data = json.loads(body or b"{}")
            cmd_type = data.get("type", "")
            if "expectedSequenceNumber" not in data and cmd_type != "CreateRoom":
                self._json(
                    200,
                    {
                        "commandId": data.get("commandId", ""),
                        "type": cmd_type,
                        "status": "rejected",
                        "schemaVersion": data.get("schemaVersion", 1),
                        "reason": "missing_expected_sequence_number",
                    },
                )
                return
            self._json(
                200,
                {
                    "commandId": data.get("commandId", ""),
                    "type": cmd_type,
                    "status": "accepted",
                    "schemaVersion": data.get("schemaVersion", 1),
                    "sequenceNumber": int(data.get("expectedSequenceNumber", 0)) + 1,
                },
            )
            return

        self._json(404, {"code": "not_found", "message": f"no handler for {path}"})

    def do_GET(self) -> None:  # noqa: N802
        body = b""
        path, query = self._record(body)

        if path == "/health":
            self._json(200, {"status": "ok", "service": "gateway"})
            return

        if path == "/v1/rankings/leaderboards":
            self._json(200, {"entries": [{"playerId": "p1", "rating": 1200}]})
            return

        if path == "/v1/analytics/public":
            self._json(200, {"gamesPlayed": 42, "schemaVersion": 1})
            return

        if path == "/v1/auth/whoami":
            auth = self.headers.get("Authorization", "")
            if not auth.startswith("Bearer "):
                self._json(401, {"code": "unauthorized", "message": "missing bearer token"})
                return
            token = auth[len("Bearer ") :].strip()
            self._json(
                200,
                {
                    "playerId": "player-1",
                    "sessionId": "sess-1",
                    "username": token.replace("tok-", "", 1) if token.startswith("tok-") else "unknown",
                },
            )
            return

        if path == "/v1/streams/player":
            room_id = unquote((query.get("roomId") or [""])[0])
            auth = self.headers.get("Authorization", "")
            if not auth.startswith("Bearer "):
                self._json(401, {"code": "unauthorized", "message": "missing bearer token"})
                return
            if STATE.fail_next_stream:
                STATE.fail_next_stream = False
                self._json(503, {"code": "unavailable", "message": "stream temporarily unavailable"})
                return
            payload = {
                "eventType": "CardPlayed",
                "roomId": room_id,
                "roomSequence": 42,
                "schemaVersion": 1,
                "unoWindow": {
                    "playerId": "p1",
                    "openingSequence": 42,
                    "expiresAt": "2099-01-01T00:00:05Z",
                },
            }
            legacy = {
                "eventType": "CardPlayed",
                "roomId": room_id,
                "schemaVersion": 1,
                "unoWindow": {
                    "playerId": "p1",
                    "openingRoomSequence": 99,
                    "expiresAt": "2099-01-01T00:00:08Z",
                },
            }
            self._write_sse([
                ("42", "CardPlayed", payload),
                ("43", "CardPlayed", legacy),
            ])
            return

        if path == "/v1/streams/spectator":
            room_id = unquote((query.get("roomId") or [""])[0])
            if STATE.fail_next_stream:
                STATE.fail_next_stream = False
                self._json(403, {"code": "forbidden", "message": "admission denied"})
                return
            payload = {
                "eventType": "SpectatorRoomProjectionUpdated",
                "roomId": room_id,
                "roomSequence": 7,
                "schemaVersion": 1,
                "unoWindow": {
                    "playerId": "p1",
                    "openingSequence": 7,
                    "expiresAt": "2099-01-01T00:00:09Z",
                },
            }
            self._write_sse([("7", "SpectatorUpdate", payload)])
            return

        if path == "/v1/streams/control":
            auth = self.headers.get("Authorization", "")
            if not auth.startswith("Bearer "):
                self._json(401, {"code": "unauthorized", "message": "missing bearer token"})
                return
            if STATE.fail_next_stream:
                STATE.fail_next_stream = False
                self._json(401, {"code": "unauthorized", "message": "session invalidated"})
                return
            payload = {
                "eventType": "SessionInvalidated",
                "sessionId": "sess-1",
                "schemaVersion": 1,
            }
            self._write_sse([("ctrl-1", "SessionInvalidated", payload)])
            return

        if path == "/__requests":
            with STATE.lock:
                self._json(200, {"requests": list(STATE.requests)})
            return

        self._json(404, {"code": "not_found", "message": f"no handler for {path}"})

    def _write_sse(self, events: list[tuple[str, str, dict[str, Any]]]) -> None:
        chunks: list[bytes] = []
        for event_id, event_name, data in events:
            if "schemaVersion" not in data:
                data = {**data, "schemaVersion": 1}
            block = (
                f"id: {event_id}\n"
                f"event: {event_name}\n"
                f"data: {json.dumps(data)}\n"
                f"\n"
            )
            chunks.append(block.encode("utf-8"))
        raw = b"".join(chunks)
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)


def main() -> None:
    host = "127.0.0.1"
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 0
    server = HTTPServer((host, port), Handler)
    bound_port = server.server_address[1]
    # Print bound URL for the harness, then serve until killed.
    sys.stdout.write(f"http://{host}:{bound_port}\n")
    sys.stdout.flush()
    server.serve_forever()


if __name__ == "__main__":
    main()
