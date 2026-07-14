#!/usr/bin/env python3
"""Interactive and headless UnoArena client, using only the public Gateway/BFF."""

from __future__ import annotations

import argparse
import json
import os
import random
import stat
import sys
import threading
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen


COLORS = {"R": "red", "G": "green", "B": "blue", "Y": "yellow"}
COLOR_PREFIX = {value: key for key, value in COLORS.items()}
FACE = {
    "skip": "SKIP",
    "reverse": "REV",
    "draw_two": "+2",
    "wild": "WILD",
    "wild_draw_four": "WILD+4",
}


class ClientError(RuntimeError):
    def __init__(self, message: str, code: Any = "client_error", *,
                 detail: Any = None, retry_after: str = "") -> None:
        super().__init__(message)
        self.code = code
        self.detail = detail
        self.retry_after = retry_after


@dataclass(frozen=True)
class RoomStartRetryPolicy:
    """Bounded retry policy for explicitly safe room-read operations only."""

    timeout_seconds: float = 60.0
    interval_cap_seconds: float = 5.0
    jitter_seconds: float = 0.25

    @classmethod
    def from_environment(cls) -> "RoomStartRetryPolicy":
        raw = os.environ.get("UNOARENA_ROOM_START_TIMEOUT_SECONDS", "60")
        try:
            timeout = float(raw)
        except ValueError as exc:
            raise ClientError(
                "UNOARENA_ROOM_START_TIMEOUT_SECONDS must be a non-negative number",
                "invalid_configuration",
            ) from exc
        if not 0 <= timeout <= 3600:
            raise ClientError(
                "UNOARENA_ROOM_START_TIMEOUT_SECONDS must be between 0 and 3600",
                "invalid_configuration",
            )
        return cls(timeout_seconds=timeout)

    def retry_delay(self, retry_after: str, *, now: datetime | None = None) -> float:
        base = 0.25
        value = retry_after.strip()
        if value:
            try:
                base = max(0.0, float(value))
            except ValueError:
                try:
                    parsed = parsedate_to_datetime(value)
                    current = now or datetime.now(timezone.utc)
                    if parsed.tzinfo is None:
                        parsed = parsed.replace(tzinfo=timezone.utc)
                    base = max(0.0, (parsed - current).total_seconds())
                except (TypeError, ValueError, OverflowError):
                    base = 0.25
        return min(self.interval_cap_seconds, base + random.uniform(0.0, self.jitter_seconds))


@dataclass
class InteractiveState:
    """Synchronize the authoritative snapshot and terminal writes across threads."""

    snapshot: dict[str, Any]
    state_lock: threading.Lock = field(default_factory=threading.Lock)
    command_lock: threading.Lock = field(default_factory=threading.Lock)
    output_lock: threading.Lock = field(default_factory=threading.Lock)

    def get(self) -> dict[str, Any]:
        with self.state_lock:
            return self.snapshot

    def replace(self, snapshot: dict[str, Any]) -> None:
        with self.state_lock:
            self.snapshot = snapshot


def now_rfc3339() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def session_path() -> str:
    override = os.environ.get("UNOARENA_SESSION_FILE")
    if override:
        return override
    home = os.environ.get("XDG_STATE_HOME") or os.path.join(os.path.expanduser("~"), ".local", "state")
    return os.path.join(home, "unoarena", "session.json")


def read_session() -> dict[str, Any]:
    path = session_path()
    try:
        mode = os.stat(path).st_mode
        if mode & (stat.S_IRWXG | stat.S_IRWXO):
            raise ClientError("session file permissions must be 0600", "unsafe_session")
        with open(path, encoding="utf-8") as handle:
            data = json.load(handle)
    except FileNotFoundError:
        return {}
    except (OSError, json.JSONDecodeError) as exc:
        raise ClientError(f"cannot read session file: {exc}", "invalid_session") from exc
    return data if isinstance(data, dict) else {}


def canonical_card(card: Any) -> str:
    if isinstance(card, str):
        raw = card.strip().lower().replace("_", "-")
        aliases = {"wild": "WILD", "wild-draw-four": "WILD+4", "wild+4": "WILD+4"}
        if raw in aliases:
            return aliases[raw]
        parts = raw.split("-", 1)
        if len(parts) == 2 and parts[0] in COLOR_PREFIX:
            return COLOR_PREFIX[parts[0]] + FACE.get(parts[1].replace("-", "_"), parts[1].upper())
        # Spectator projections may already carry canonical display notation.
        return card.upper().replace("DRAW_TWO", "+2").replace("REVERSE", "REV")
    if not isinstance(card, dict):
        return "?"
    face = str(card.get("face", ""))
    if face in ("wild", "wild_draw_four"):
        return FACE[face]
    return f"{COLOR_PREFIX.get(str(card.get('color', '')).lower(), '?')}{FACE.get(face, face)}"


def playable(card: dict[str, Any], game: dict[str, Any], hand: list[dict[str, Any]] | None = None) -> bool:
    face = str(card.get("face", ""))
    active = str(game.get("activeColor", "")).lower()
    top = game.get("discardTop") or {}
    top_face = str(top.get("face", ""))
    wild_draw_four_legal = not any(
        other is not card
        and str(other.get("color", "")).lower() == active
        and str(other.get("face", "")) not in ("wild", "wild_draw_four")
        for other in (hand or [])
    )

    ordinary = (
        str(card.get("color", "")).lower() == active
        or (top_face not in ("wild", "wild_draw_four") and face == top_face)
    )
    if int(game.get("penaltyAmount", 0) or 0) > 0:
        if face == "draw_two":
            return ordinary
        if face == "wild_draw_four":
            return wild_draw_four_legal
        return False
    if face == "wild":
        return True
    if face == "wild_draw_four":
        return wild_draw_four_legal
    return ordinary


@dataclass
class Metrics:
    player: str
    room: str = ""
    actions: int = 0
    errors: int = 0
    latencies: list[int] = field(default_factory=list)

    def emit(self, action: str, started: float, result: str, error_code: Any, seq: Any, corr: str) -> None:
        latency = max(0, round((time.monotonic() - started) * 1000))
        self.actions += 1
        self.errors += result == "error"
        self.latencies.append(latency)
        print(json.dumps({
            "ts": now_rfc3339(), "action": action, "room": self.room,
            "player": self.player, "latency_ms": latency, "result": result,
            "error_code": error_code, "seq": seq, "correlationId": corr,
        }, separators=(",", ":")), flush=True)

    def summary(self, success: bool) -> None:
        values = sorted(self.latencies)
        avg = round(sum(values) / len(values), 2) if values else 0
        p95 = values[min(len(values) - 1, int(len(values) * .95))] if values else 0
        print(json.dumps({
            "ts": now_rfc3339(), "action": "summary", "room": self.room,
            "player": self.player, "latency_ms": 0,
            "result": "ok" if success else "error",
            "error_code": None if success else "bot_failed", "seq": None,
            "correlationId": str(uuid.uuid4()), "total_actions": self.actions,
            "error_count": self.errors, "latency_avg_ms": avg,
            "latency_p95_ms": p95,
        }, separators=(",", ":")), flush=True)


class Gateway:
    def __init__(self, base: str, token: str = "") -> None:
        self.base = base.rstrip("/")
        self.token = token
        self.room_start_retry = RoomStartRetryPolicy.from_environment()

    def request(self, method: str, path: str, body: Any = None, *, auth: bool = True,
                correlation_id: str | None = None) -> tuple[Any, str, int]:
        corr = correlation_id or str(uuid.uuid4())
        headers = {"Accept": "application/json", "X-Correlation-Id": corr}
        if auth and self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        raw = None
        if body is not None:
            raw = json.dumps(body, separators=(",", ":")).encode()
            headers["Content-Type"] = "application/json"
        request = Request(self.base + path, data=raw, headers=headers, method=method)
        try:
            with urlopen(request, timeout=15) as response:
                payload = response.read()
                return (json.loads(payload) if payload else {}, corr, response.status)
        except HTTPError as exc:
            payload = exc.read()
            try:
                detail = json.loads(payload) if payload else {}
            except json.JSONDecodeError:
                detail = payload.decode(errors="replace")
            raise ClientError(
                f"HTTP {exc.code}: {detail}", exc.code, detail=detail,
                retry_after=exc.headers.get("Retry-After", ""),
            ) from exc
        except (URLError, TimeoutError) as exc:
            raise ClientError(f"gateway request failed: {exc}", "timeout") from exc

    def login(self, username: str, password: str) -> dict[str, Any]:
        data, _, _ = self.request("POST", "/v1/auth/login", {"username": username, "password": password}, auth=False)
        token = data.get("token")
        if not isinstance(token, str) or not token:
            raise ClientError("login response omitted token", "invalid_login")
        self.token = token
        return data

    def whoami(self) -> dict[str, Any]:
        data, _, _ = self.safe_read("/v1/auth/whoami")
        return data

    def snapshot(self, room: str) -> dict[str, Any]:
        data, _, _ = self.safe_read(f"/v1/rooms/{quote(room, safe='')}/snapshot")
        return data

    def assignment(self, tournament: str, player: str) -> dict[str, Any]:
        path = (
            f"/v1/tournaments/{quote(tournament, safe='')}/players/"
            f"{quote(player, safe='')}/assignment"
        )
        data, _, _ = self.safe_read(path)
        return data

    def safe_read(self, path: str) -> tuple[Any, str, int]:
        """Boundedly retry an explicitly known-safe GET."""
        deadline = time.monotonic() + self.room_start_retry.timeout_seconds
        while True:
            try:
                return self.request("GET", path)
            except ClientError as exc:
                structured_starting = (
                    exc.code == 503
                    and isinstance(exc.detail, dict)
                    and exc.detail.get("code") == "room_starting"
                )
                transient_transport = exc.code in (502, 504, "timeout")
                if not structured_starting and not transient_transport:
                    raise
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    if structured_starting:
                        raise ClientError(
                            "room runtime did not become ready before the configured timeout",
                            "room_start_timeout",
                            detail=exc.detail,
                        ) from exc
                    raise
                delay = min(
                    remaining,
                    self.room_start_retry.retry_delay(exc.retry_after),
                )
                time.sleep(delay)

    def command(self, room: str, kind: str, sequence: int, payload: dict[str, Any]) -> tuple[dict[str, Any], str]:
        command_id, corr = str(uuid.uuid4()), str(uuid.uuid4())
        envelope = {"commandId": command_id, "type": kind, "schemaVersion": 1,
                    "expectedSequenceNumber": sequence, "payload": {"roomId": room, **payload}}
        data, _, _ = self.request("POST", f"/v1/rooms/{quote(room, safe='')}/commands", envelope,
                                  correlation_id=corr)
        return data, corr

    def global_command(self, kind: str, payload: dict[str, Any]) -> tuple[dict[str, Any], str]:
        corr = str(uuid.uuid4())
        envelope = {"commandId": str(uuid.uuid4()), "type": kind, "schemaVersion": 1, "payload": payload}
        data, _, _ = self.request("POST", "/v1/commands", envelope, correlation_id=corr)
        return data, corr

    def register_tournament(self, tournament: str, player: str) -> tuple[dict[str, Any], str]:
        """Register once, reconciling an unknown transport result by safe read."""
        corr = str(uuid.uuid4())
        envelope = {
            "commandId": str(uuid.uuid4()),
            "type": "RegisterPlayer",
            "schemaVersion": 1,
            "payload": {"tournamentId": tournament, "playerId": player},
        }
        try:
            data, _, _ = self.request("POST", "/v1/commands", envelope, correlation_id=corr)
            return data, corr
        except ClientError as exc:
            if exc.code not in (502, 504, "timeout"):
                raise
            # Never replay an uncertain mutation. The authenticated assignment
            # projection is the authoritative, safe reconciliation surface.
            try:
                assignment = self.assignment(tournament, player)
            except ClientError as reconcile_exc:
                raise exc from reconcile_exc
            registration = str(assignment.get("registrationStatus") or "").lower()
            if registration not in ("registered", "eliminated"):
                raise exc
            return {"status": "accepted", "sequenceNumber": assignment.get("projectionVersion")}, corr


def identity(gateway: Gateway, args: argparse.Namespace) -> tuple[str, str]:
    if args.token:
        gateway.token = args.token
    elif args.user or args.password:
        if not args.user or not args.password:
            raise ClientError("--user and --pass must be supplied together", "usage")
        login = gateway.login(args.user, args.password)
        return str(login.get("playerId", "")), args.user
    elif os.environ.get("UNOARENA_TOKEN"):
        gateway.token = os.environ["UNOARENA_TOKEN"]
    else:
        sess = read_session()
        gateway.token = str(sess.get("token", ""))
    if not gateway.token:
        raise ClientError("authentication required: use --user/--pass, --token, or login first", "usage")
    who = gateway.whoami()
    return str(who.get("playerId", "")), str(who.get("username") or who.get("playerId", "player"))


def accepted(response: dict[str, Any]) -> bool:
    return response.get("status") == "accepted"


def command_with_resync(gateway: Gateway, room: str, snapshot: dict[str, Any], kind: str,
                        payload: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any], str, bool]:
    sequence = int(snapshot.get("sequenceNumber", 0))
    try:
        response, corr = gateway.command(room, kind, sequence, payload)
    except ClientError as exc:
        stale_result = (
            exc.code == 409
            and isinstance(exc.detail, dict)
            and exc.detail.get("status") == "rejected"
            and exc.detail.get("reason") == "stale_sequence"
        )
        if not stale_result:
            raise
        return gateway.snapshot(room), exc.detail, str(uuid.uuid4()), False
    if not accepted(response):
        # Rejections, including stale sequence, are surfaced and reconciled.
        return gateway.snapshot(room), response, corr, False
    return gateway.snapshot(room), response, corr, True


def recover_unknown_bot_outcome(
    gateway: Gateway, room: str, exc: ClientError, submitted_sequence: int, *,
    deadline: float, poll_interval: float,
) -> dict[str, Any] | None:
    """Observe bounded authoritative progress without replaying a mutation."""
    if exc.code not in (502, 503, 504, "timeout"):
        return None
    # A peer command, Room-owned timer, or dedicated-runtime replacement may
    # resolve the uncertainty after the transport failure. Use the configured
    # safe-read budget, bounded by the bot's overall deadline, and resume only
    # from newer committed state. The uncertain mutation is never replayed.
    observation_deadline = min(
        deadline,
        time.monotonic() + gateway.room_start_retry.timeout_seconds,
    )
    while time.monotonic() < observation_deadline:
        try:
            snapshot = ensure_reconnected(gateway, room, gateway.snapshot(room))
        except ClientError:
            snapshot = None
        if snapshot is not None:
            if snapshot.get("status") in ("completed", "cancelled"):
                return snapshot
            sequence = snapshot.get("sequenceNumber")
            if type(sequence) is int and sequence > submitted_sequence:
                return snapshot
        time.sleep(max(0.01, poll_interval))
    # The authoritative state did not advance within the observation window,
    # so choosing the same action again could replay an uncertain mutation with
    # a new command ID.
    return None


def ensure_reconnected(gateway: Gateway, room: str, snapshot: dict[str, Any]) -> dict[str, Any]:
    disconnect = snapshot.get("disconnect") or {}
    version = disconnect.get("disconnectVersion")
    if type(version) is not int or version < 0:
        return snapshot
    refreshed, response, _, ok = command_with_resync(
        gateway, room, snapshot, "ReconnectToRoom", {"disconnectVersion": version}
    )
    if not ok:
        raise ClientError(f"reconnect rejected: {response}", "reconnect_rejected")
    return refreshed


def join_room(gateway: Gateway, room: str) -> None:
    public, _, _ = gateway.request("GET", f"/v1/spectator/rooms/{quote(room, safe='')}/snapshot", auth=False)
    sequence = int(public.get("sequenceNumber", public.get("sequence", 0)))
    response, _ = gateway.command(room, "JoinRoom", sequence, {})
    if not accepted(response):
        reason = str(response.get("reason") or response.get("payload") or "join rejected")
        # Already seated is harmless for reconnecting one process/session.
        if "already" not in reason.lower():
            raise ClientError(reason, "join_rejected")


def casual_room(gateway: Gateway) -> str:
    listing, _, _ = gateway.request("GET", "/v1/rooms?status=waiting&limit=50", auth=False)
    for item in listing.get("rooms", []):
        occupied = item.get("currentPlayers", item.get("seatsOccupied", 0))
        if item.get("visibility") == "public" and int(occupied) < int(item.get("maxSeats", 10)):
            room = str(item.get("roomId", ""))
            if room:
                try:
                    join_room(gateway, room)
                    return room
                except ClientError:
                    continue
    room = f"casual-{uuid.uuid4()}"
    response, _ = gateway.global_command("CreateRoom", {"roomId": room, "visibility": "public", "maxSeats": 10})
    if not accepted(response):
        raise ClientError("casual room creation rejected", "create_rejected")
    return str((response.get("payload") or {}).get("roomId") or room)


def render_board(snapshot: dict[str, Any], player_id: str) -> str:
    game = snapshot.get("game") or {}
    hand = snapshot.get("hand") or []
    counts = game.get("handCounts") or {}
    uno = snapshot.get("uno") or {}
    direction = "clockwise" if int(game.get("direction", 1) or 1) > 0 else "counter-clockwise"
    lines = [
        f"\n=== ROOM {snapshot.get('roomId', '?')} · seq {snapshot.get('sequenceNumber', 0)} ===",
        f"Discard: {canonical_card(game.get('discardTop'))} · active: {str(game.get('activeColor', '?')).upper()}",
        f"Direction: {direction} · draw pile: {game.get('drawPileSize', '?')}",
        "Opponents:",
    ]
    for seat in snapshot.get("roster", []):
        pid = str(seat.get("playerId", ""))
        if pid == player_id:
            continue
        flag = " UNO!" if uno.get("playerId") == pid and uno.get("called") else ""
        lines.append(f"  {pid}: {counts.get(pid, '?')} cards{flag}")
    lines.append("Your hand:")
    for index, card in enumerate(hand, 1):
        marker = "*" if isinstance(card, dict) and playable(card, game, hand) else " "
        lines.append(f" {marker} {index:>2}. {canonical_card(card)}")
    if game.get("currentPlayer") == player_id:
        lines.append("YOUR TURN (* = playable)")
    else:
        lines.append(f"Turn: {game.get('currentPlayer', 'waiting')}")
    lines.append("Commands: play <n> [R|G|B|Y], draw, uno, challenge, pass, state, quit")
    return "\n".join(lines)


def emit_interactive_state(snapshot: dict[str, Any], player_id: str) -> None:
    print(json.dumps({
        "ts": now_rfc3339(), "action": "state", "room": snapshot.get("roomId", ""),
        "player": player_id, "latency_ms": 0, "result": "ok", "error_code": None,
        "seq": snapshot.get("sequenceNumber"), "correlationId": str(uuid.uuid4()),
        "state": snapshot,
    }, separators=(",", ":")), flush=True)


def emit_interactive_failure(exc: ClientError, *, player: str = "") -> None:
    """Emit one machine-readable terminal failure for interactive JSON mode."""
    print(json.dumps({
        "ts": now_rfc3339(), "action": "room_start", "room": "",
        "player": player, "latency_ms": 0, "result": "error",
        "error_code": exc.code, "seq": None,
        "correlationId": str(uuid.uuid4()),
    }, separators=(",", ":")), flush=True)


def event_line(data: dict[str, Any]) -> str:
    kind = str(data.get("eventType") or data.get("type") or "event")
    player = str(data.get("playerId") or data.get("actorId") or data.get("targetId") or "player")
    if "CardPlayed" in kind:
        return f"{player} played {canonical_card(data.get('card') or data.get('discardTop'))} · color: {str(data.get('activeColor', '')).upper()}"
    if "Draw" in kind:
        return f"{player} drew {data.get('count', data.get('amount', 1))}"
    if "Uno" in kind:
        return f"{player} called UNO!"
    return f"[{data.get('roomSequence', data.get('sequenceNumber', '?'))}] {kind}"


def follow_events(gateway: Gateway, room: str, stop: threading.Event, json_mode: bool,
                  *, on_event: Any = None, output_lock: threading.Lock | None = None) -> None:
    last = ""
    writes = output_lock or threading.Lock()
    while not stop.is_set():
        path = "/v1/streams/player?" + urlencode({"roomId": room})
        headers = {"Accept": "text/event-stream", "Authorization": f"Bearer {gateway.token}"}
        if last:
            headers["Last-Event-ID"] = last
        try:
            with urlopen(Request(gateway.base + path, headers=headers), timeout=90) as response:
                event_id = ""
                for raw in response:
                    if stop.is_set():
                        return
                    line = raw.decode(errors="replace").rstrip("\r\n")
                    if line.startswith("id:"):
                        event_id = line[3:].strip()
                    elif line.startswith("data:"):
                        try:
                            data = json.loads(line[5:].strip())
                        except json.JSONDecodeError:
                            continue
                        if event_id and event_id == last:
                            continue
                        if event_id:
                            last = event_id
                        with writes:
                            if json_mode:
                                print(json.dumps({"ts": now_rfc3339(), "action": "event", "room": room,
                                    "player": "", "latency_ms": 0, "result": "ok", "error_code": None,
                                    "seq": data.get("roomSequence"), "correlationId": data.get("correlationId", ""),
                                    "event": data}, separators=(",", ":")), flush=True)
                            else:
                                print("\n" + event_line(data), flush=True)
                        if on_event is not None:
                            on_event(data)
        except HTTPError as exc:
            if exc.code == 409:
                try:
                    snap = gateway.snapshot(room)
                    # SSE ids are opaque and are not interchangeable with a room
                    # sequence. Resume without a marker after snapshot recovery.
                    last = ""
                    print(f"\nConflict: stream history unavailable; reconciled at sequence "
                          f"{snap.get('sequenceNumber', '')}", file=sys.stderr)
                except ClientError:
                    pass
            elif exc.code in (401, 403):
                print(f"\nStream closed: HTTP {exc.code}", file=sys.stderr)
                return
        except (URLError, TimeoutError, OSError):
            pass
        stop.wait(.5)


def maybe_start(gateway: Gateway, room: str, snapshot: dict[str, Any], player_id: str) -> dict[str, Any]:
    if snapshot.get("hostId") != player_id or len(snapshot.get("roster") or []) < 2:
        return snapshot
    if snapshot.get("status") == "waiting":
        snapshot, _, _, ok = command_with_resync(gateway, room, snapshot, "LockRoom", {})
        if not ok:
            return snapshot
    if snapshot.get("status") == "locked":
        snapshot, _, _, _ = command_with_resync(gateway, room, snapshot, "StartMatch", {"gameId": f"game-{uuid.uuid4()}"})
    return snapshot


def play_interactive(args: argparse.Namespace) -> int:
    gateway = Gateway(args.api_url)
    player_id, _ = identity(gateway, args)
    room = casual_room(gateway)
    snapshot = ensure_reconnected(gateway, room, gateway.snapshot(room))
    if args.casual:
        snapshot = maybe_start(gateway, room, snapshot, player_id)
    state = InteractiveState(snapshot)
    stop = threading.Event()

    def refresh_after_event(_event: dict[str, Any]) -> None:
        try:
            with state.command_lock:
                refreshed = ensure_reconnected(gateway, room, gateway.snapshot(room))
                if args.casual:
                    refreshed = maybe_start(gateway, room, refreshed, player_id)
                state.replace(refreshed)
            with state.output_lock:
                if args.json:
                    emit_interactive_state(refreshed, player_id)
                else:
                    print(render_board(refreshed, player_id), flush=True)
        except ClientError as exc:
            print(f"State refresh failed: {exc}", file=sys.stderr)

    thread = threading.Thread(
        target=follow_events,
        args=(gateway, room, stop, args.json),
        kwargs={"on_event": refresh_after_event, "output_lock": state.output_lock},
        daemon=True,
    )
    if args.json:
        emit_interactive_state(snapshot, player_id)
    else:
        print(render_board(snapshot, player_id))
    thread.start()
    try:
        while True:
            raw = input("uno> " if not args.json else "").strip()
            if not raw:
                continue
            parts = raw.split()
            verb = parts[0].lower()
            if verb == "quit":
                return 0
            if verb == "state":
                with state.command_lock:
                    snapshot = ensure_reconnected(gateway, room, gateway.snapshot(room))
                    if args.casual:
                        snapshot = maybe_start(gateway, room, snapshot, player_id)
                    state.replace(snapshot)
                if args.json:
                    emit_interactive_state(snapshot, player_id)
                else:
                    print(render_board(snapshot, player_id))
                continue
            kind, payload = "", {}
            wild_color: str | None = None
            snapshot = state.get()
            if verb == "play" and len(parts) in (2, 3):
                try:
                    index = int(parts[1]) - 1
                    card = (snapshot.get("hand") or [])[index]
                    if index < 0:
                        raise IndexError
                except (ValueError, IndexError):
                    print("Invalid hand index", file=sys.stderr)
                    continue
                if not playable(card, snapshot.get("game") or {}, snapshot.get("hand") or []):
                    print("That card is not playable in the current snapshot", file=sys.stderr)
                    continue
                if str(card.get("face")) in ("wild", "wild_draw_four"):
                    color_arg = parts[2].upper() if len(parts) == 3 else ""
                    while color_arg not in COLORS:
                        if color_arg:
                            print("Wild color must be R, G, B, or Y", file=sys.stderr)
                        print("Color [R/G/B/Y]: ", end="", file=sys.stderr, flush=True)
                        color_arg = input().strip().upper()
                    wild_color = COLORS[color_arg]
                kind, payload = "PlayCard", {"cardId": card.get("id", "")}
            elif verb == "draw":
                kind = "DrawCard"
            elif verb == "uno":
                kind = "CallUno"
            elif verb == "challenge":
                target = str((snapshot.get("uno") or {}).get("playerId", ""))
                if not target:
                    print("No open UNO window to challenge", file=sys.stderr)
                    continue
                kind, payload = "ReportMissingUno", {"targetId": target}
            elif verb == "pass":
                if args.json:
                    print(json.dumps({"ts": now_rfc3339(), "action": "pass", "room": room,
                        "player": player_id, "latency_ms": 0, "result": "ok", "error_code": None,
                        "seq": snapshot.get("sequenceNumber"), "correlationId": str(uuid.uuid4()),
                        "message": "Pass is automatic after draw on this backend."}, separators=(",", ":")))
                else:
                    print("Pass is automatic after draw on this backend.")
                continue
            else:
                print("Unknown command", file=sys.stderr)
                continue
            before = time.monotonic()
            old_seq = snapshot.get("sequenceNumber")
            try:
                with state.command_lock:
                    snapshot = state.get()
                    snapshot, response, corr, ok = command_with_resync(gateway, room, snapshot, kind, payload)
                    state.replace(snapshot)
                if not ok:
                    print(f"Conflict/rejection at sequence {old_seq}: {response}; authoritative state reloaded", file=sys.stderr)
                if kind == "PlayCard" and ok and wild_color:
                    with state.command_lock:
                        snapshot, _, corr, ok = command_with_resync(
                            gateway, room, state.get(), "ChooseColor", {"color": wild_color}
                        )
                        state.replace(snapshot)
                if args.json:
                    latency = round((time.monotonic() - before) * 1000)
                    print(json.dumps({"ts": now_rfc3339(), "action": kind, "room": room, "player": player_id,
                        "latency_ms": latency, "result": "ok" if ok else "error",
                        "error_code": None if ok else 409, "seq": snapshot.get("sequenceNumber"),
                        "correlationId": corr}, separators=(",", ":")))
                else:
                    print(render_board(snapshot, player_id))
            except ClientError as exc:
                print(f"Action failed: {exc}", file=sys.stderr)
    except (EOFError, KeyboardInterrupt):
        return 0
    finally:
        stop.set()
        thread.join(timeout=1)


def bot_action(gateway: Gateway, room: str, snapshot: dict[str, Any], player_id: str,
               rng: random.Random) -> tuple[str, dict[str, Any], str | None]:
    game = snapshot.get("game") or {}
    hand = snapshot.get("hand") or []
    choices = [card for card in hand if isinstance(card, dict) and playable(card, game, hand)]
    if choices:
        card = rng.choice(choices)
        color = rng.choice(list(COLORS.values())) if card.get("face") in ("wild", "wild_draw_four") else None
        return "PlayCard", {"cardId": card.get("id", "")}, color
    return "DrawCard", {}, None


def play_bot_room(gateway: Gateway, room: str, player_id: str, username: str,
                  args: argparse.Namespace, metrics: Metrics) -> bool:
    metrics.room = room
    deadline = time.monotonic() + args.max_wait
    rng = random.Random(args.seed)
    actions = 0
    snapshot = gateway.snapshot(room)
    while time.monotonic() < deadline and actions < args.max_actions:
        snapshot = ensure_reconnected(gateway, room, snapshot)
        snapshot = maybe_start(gateway, room, snapshot, player_id)
        game = snapshot.get("game") or {}
        # A completed game is not necessarily a completed best-of-three match.
        # Stay in the assigned room until the room/match itself is terminal.
        if snapshot.get("status") == "completed":
            return True
        if snapshot.get("status") == "cancelled":
            return False
        if game.get("currentPlayer") != player_id:
            time.sleep(args.poll_interval)
            snapshot = ensure_reconnected(gateway, room, gateway.snapshot(room))
            continue
        kind, payload, color = bot_action(gateway, room, snapshot, player_id, rng)
        started = time.monotonic()
        try:
            snapshot, response, corr, ok = command_with_resync(gateway, room, snapshot, kind, payload)
            metrics.emit("play_card" if kind == "PlayCard" else "draw", started,
                         "ok" if ok else "error", None if ok else 409,
                         snapshot.get("sequenceNumber"), corr)
            actions += 1
            if not ok:
                time.sleep(args.poll_interval)
                continue
            if color:
                started = time.monotonic()
                snapshot, _, corr, ok = command_with_resync(gateway, room, snapshot, "ChooseColor", {"color": color})
                metrics.emit("choose_color", started, "ok" if ok else "error", None if ok else 409,
                             snapshot.get("sequenceNumber"), corr)
                actions += 1
            if len(snapshot.get("hand") or []) == 1:
                started = time.monotonic()
                snapshot, _, corr, ok = command_with_resync(gateway, room, snapshot, "CallUno", {})
                metrics.emit("uno", started, "ok" if ok else "error", None if ok else 409,
                             snapshot.get("sequenceNumber"), corr)
                actions += 1
        except ClientError as exc:
            metrics.emit(kind.lower(), started, "error", exc.code, snapshot.get("sequenceNumber"), str(uuid.uuid4()))
            reconciled = recover_unknown_bot_outcome(
                gateway, room, exc, int(snapshot.get("sequenceNumber", 0)),
                deadline=deadline, poll_interval=args.poll_interval,
            )
            if reconciled is not None:
                snapshot = reconciled
                time.sleep(args.poll_interval)
                continue
            return False
    # A bounded action cap ends the run, but never turns an active match into a
    # success. Reconcile once because the cap may have been reached by the
    # terminal action itself.
    if actions >= args.max_actions:
        try:
            final_snapshot = ensure_reconnected(gateway, room, gateway.snapshot(room))
            return final_snapshot.get("status") == "completed"
        except ClientError:
            return False
    return False


def run_bot(args: argparse.Namespace) -> int:
    gateway = Gateway(args.api_url)
    metrics = Metrics(player=args.user or "player")
    success = False
    try:
        player_id, username = identity(gateway, args)
        metrics.player = username or player_id
        if args.tournament:
            started = time.monotonic()
            response, corr = gateway.register_tournament(args.tournament, player_id)
            ok = accepted(response)
            metrics.emit("tournament_register", started, "ok" if ok else "error", None if ok else "rejected",
                         response.get("sequenceNumber"), corr)
            if not ok:
                raise ClientError("tournament registration rejected", "register_rejected")
            deadline = time.monotonic() + args.max_wait
            seen: set[tuple[Any, Any]] = set()
            success = False
            tournament_completed = False
            while time.monotonic() < deadline:
                assignment = gateway.assignment(args.tournament, player_id)
                slot = assignment.get("assignment") or {}
                key = (slot.get("roundNumber"), slot.get("slotId"))
                room = str(slot.get("roomId") or "")
                if room and key not in seen:
                    seen.add(key)
                    try:
                        join_room(gateway, room)
                    except ClientError:
                        pass
                    if not play_bot_room(gateway, room, player_id, username, args, metrics):
                        success = False
                        break
                    success = True
                if assignment.get("phase") == "completed":
                    tournament_completed = True
                    break
                if assignment.get("phase") == "cancelled":
                    break
                time.sleep(args.poll_interval)
            # A played slot or a registration is not terminal success. The
            # authoritative assignment view must confirm tournament completion.
            success = tournament_completed
        else:
            room = args.room
            if args.casual:
                room = casual_room(gateway)
            elif room:
                try:
                    join_room(gateway, room)
                except ClientError:
                    pass
            if not room:
                raise ClientError("select exactly one of --casual, --room, or --tournament", "usage")
            success = play_bot_room(gateway, room, player_id, username, args, metrics)
    except ClientError as exc:
        metrics.emit("bot_setup", time.monotonic(), "error", exc.code, None, str(uuid.uuid4()))
        success = False
    metrics.summary(success)
    return 0 if success else 1


def parser() -> argparse.ArgumentParser:
    out = argparse.ArgumentParser(prog="unoarena")
    out.add_argument("mode", choices=("play", "bot"))
    target = out.add_mutually_exclusive_group(required=True)
    target.add_argument("--casual", action="store_true")
    target.add_argument("--room")
    target.add_argument("--tournament")
    out.add_argument("--user")
    out.add_argument("--pass", dest="password")
    out.add_argument("--token")
    out.add_argument("--seed", type=int)
    out.add_argument("--json", action="store_true")
    out.add_argument("--max-actions", type=int, default=10000, help=argparse.SUPPRESS)
    out.add_argument("--max-wait", type=float, default=900, help=argparse.SUPPRESS)
    out.add_argument("--poll-interval", type=float, default=.5, help=argparse.SUPPRESS)
    out.add_argument("--api-url", default=os.environ.get("UNOARENA_API_URL", ""), help=argparse.SUPPRESS)
    return out


def main() -> int:
    args = parser().parse_args()
    if not args.api_url:
        print("FAIL: UNOARENA_API_URL must be set", file=sys.stderr)
        return 1
    if args.mode == "play":
        if not args.casual or args.room or args.tournament:
            print("FAIL: interactive mode currently requires play --casual", file=sys.stderr)
            return 2
        try:
            return play_interactive(args)
        except ClientError as exc:
            if args.json:
                emit_interactive_failure(exc, player=args.user or "")
            print(f"FAIL: {exc}", file=sys.stderr)
            return 1
    return run_bot(args)


if __name__ == "__main__":
    raise SystemExit(main())
