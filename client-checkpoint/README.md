# Client Checkpoint CLI

This directory contains the Client Checkpoint command-line artifact used as the
canonical client and smoke-test entry point for this implementation.

For this checkpoint, the repo-owned simple CLI is the **sole client and automated
test interface**. A graphical interface is **explicitly deferred** for later
refactoring. Deferred UI work must continue to speak only to the Realtime
Gateway / BFF and must not introduce direct microservice access.

## Requirements

- `curl` (HTTP/SSE)
- `bash`
- `python3` (stdlib only; JSON encoding/validation, RFC3339 countdown math, offline tests)

## Configuration

| Variable | Required | Purpose |
| --- | --- | --- |
| `UNOARENA_API_URL` | Yes (except `countdown`) | Gateway/BFF base URL. The CLI never calls microservices directly. |
| `UNOARENA_TOKEN` | No | Default bearer token when `--token` is omitted |

## Commands

All HTTP commands use OpenAPI BFF paths on `UNOARENA_API_URL`. HTTP responses are
always JSON; `--json` is only meaningful for `countdown` (structured JSON vs text).

```bash
# Health / public reads
unoarena health
unoarena leaderboard
unoarena analytics

# Auth
unoarena register --user <username> --pass <password>
unoarena login --user <username> --pass <password>
unoarena whoami --token <bearer-token>

# Rooms / command envelopes
unoarena room create --token <bearer-token> [--command-id <id>] [--payload <json>]
unoarena command --token <bearer-token> --type <CommandType> \
  [--room-id <roomId>] [--command-id <id>] --expected-sequence <n> \
  [--payload <json>] [--schema-version <n>] [--correlation-id <id>]

# Tournament command envelopes (no room expectedSequenceNumber)
unoarena tournament create --token <bearer-token> [--command-id <id>] [--payload <json>]
unoarena tournament register --token <bearer-token> [--command-id <id>] [--payload <json>]
unoarena tournament close-registration --token <bearer-token> \
  [--command-id <id>] [--payload <json>]

# SSE subscriptions (player/control require bearer token; spectator token optional)
unoarena stream player --token <bearer-token> --room-id <roomId> [--last-event-id <id>]
unoarena stream spectator --room-id <roomId> [--token <bearer-token>] [--last-event-id <id>]
unoarena stream control --token <bearer-token> [--last-event-id <id>]

# Advisory Uno countdown (local only; no HTTP). --json emits structured JSON.
unoarena countdown --expires-at <RFC3339> --opening-sequence <n> [--now <RFC3339>] [--json]
```

### Gateway paths used

| Command | Method + path |
| --- | --- |
| `health` | `GET /health` |
| `register` | `POST /v1/auth/register` |
| `login` | `POST /v1/auth/login` |
| `whoami` | `GET /v1/auth/whoami` (`Authorization: Bearer …`) |
| `room create` / tournament helpers / `command` without `--room-id` | `POST /v1/commands` |
| `command --room-id …` | `POST /v1/rooms/{roomId}/commands` (roomId URL-encoded) |
| `stream player` | `GET /v1/streams/player?roomId=…` |
| `stream spectator` | `GET /v1/streams/spectator?roomId=…` |
| `stream control` | `GET /v1/streams/control` |
| `leaderboard` | `GET /v1/rankings/leaderboards` |
| `analytics` | `GET /v1/analytics/public` |
| `countdown` | none (local advisory display) |

BFF snapshot routes (OpenAPI; used after SSE `409 snapshot_required` or reconnect) are part of the settled client contract even when invoked via `curl` rather than a dedicated CLI subcommand:

| Purpose | Method + path |
| --- | --- |
| Player reconnect snapshot | `GET /v1/rooms/{roomId}/snapshot` (bearer required) |
| Spectator projection snapshot | `GET /v1/spectator/rooms/{roomId}/snapshot` (anonymous-tolerant for public non-terminal rooms; private rooms need session/invite/operator; invalid bearer → `401`) |

### Command envelope rules

- Request bodies are built with strict JSON encoding (`python3` stdlib).
- `schemaVersion` must equal `1` (digits only). `expectedSequenceNumber` must be a non-negative integer (digits only).
- `--payload` must be a JSON **object** (not an array/string); invalid JSON is rejected before send.
- `expectedSequenceNumber` (`--expected-sequence`) is **required** for mutations of an existing room aggregate (`JoinRoom`, `PlayCard`, …). Documented exception: `CreateRoom`. Tournament commands do not use room sequence.
- Room IDs in paths and stream query params are URL-encoded.
- Requests send `X-Correlation-Id` (auto-generated or `--correlation-id`) and command submissions also send `X-Command-Id`.
- Rejected commands return `status=rejected` with a reason; they are audit-only and never domain events.

### SSE

- Successful streams are written to stdout as received.
- HTTP 4xx/5xx responses cause a nonzero exit (after the response completes).
- `--last-event-id` sets the `Last-Event-ID` resume header.
- Unknown or evicted `Last-Event-ID` yields HTTP `409` with `code=snapshot_required`; resync via the snapshot routes above before resuming.
- Spectator admission is allowed while the room is `waiting` / `locked` / `in_progress`; terminal `RoomCompleted` / `RoomCancelled` deny new admission and close streams (not individual `GameCompleted`).

### Advisory countdown

CLI countdown or local timer display for Uno windows is **advisory only**. Absolute
UTC `expiresAt` values and opening room sequence markers published by the server
(`openingSequence` canonical; `openingRoomSequence` accepted as a legacy ingest alias)
remain authoritative. During `stream player` / `stream spectator`, when an SSE `data:`
payload includes both `expiresAt` and an opening-sequence field (including nested
`uno` / `unoWindow`), the CLI prints an advisory line on stderr using the canonical
`openingSequence=` label. SSE updates and command results correct the client when
local display drifts. `countdown --json` emits `openingSequence` (not the legacy alias).

## Offline tests

Ephemeral stdlib fake gateway (no persistent servers, no external packages):

```bash
./client-checkpoint/tests/run-tests.sh
```

## GUI status

Graphical UI is deferred. This CLI is the sole current client interface.
