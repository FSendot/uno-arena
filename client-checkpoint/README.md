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
- The public command catalog is closed (15 OpenAPI variants). Unknown `--type` values are rejected locally before send.
- `expectedSequenceNumber` (`--expected-sequence`) is **required** for mutations of an existing room aggregate (`JoinRoom`, `PlayCard`, …). It must be **absent** for `CreateRoom` and the three tournament types; passing `--expected-sequence` for those is rejected locally.
- Room IDs in paths and stream query params are URL-encoded.
- Requests send `X-Correlation-Id` (auto-generated or `--correlation-id`) and command submissions also send `X-Command-Id`.
- Rejected commands return HTTP `200` with `status=rejected` and a reason (including stale/wrong sequence); they are audit-only and never domain events. Malformed envelopes (unknown type, missing required sequence, prohibited sequence, bad schemaVersion) are HTTP `400` `invalid_envelope`.

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

## Capability stack integration harness

Repeatable Docker Compose integration against the capability overlay
(`docker-compose.local.yml` + `docker-compose.capability.yml`). Speaks only to the
CLI/BFF edge. Requires `bash`, `curl`, `python3`, and Docker (no `jq`, no GNU
`timeout`).

```bash
make test-capability-stack
# or:
./client-checkpoint/tests/run-capability-stack.sh
```

Behavior:

- Unique `docker compose -p` project name per run; capability overlay scopes
  both `uno_edge` and `uno_private` network names to the project. Gateway
  attaches to non-internal edge (host publish) plus internal private
  (interservice); backends stay private-only. Gateway host port is a concrete
  free localhost port by default (Python `socket` bind; override with
  `CAP_BFF_HOST_PORT`). Bind conflicts retry with another free port — never
  published `0` (Compose 5.x leaves HostPort unbound). After `compose up`, the
  harness polls `compose ps` + `docker inspect` until the published host port
  matches the selected nonzero port, up to `CAP_READY_TIMEOUT_S` (default 180s
  in the harness; 60s in the helper alone). On failure it prints `compose ps`
  diagnostics
- Capability overlay omits KurrentDB because GI uses explicit memory; harness `up`
  uses resolved `config --services` so KurrentDB is never selected
- Polls raw BFF `GET /health` before the first CLI call, then waits for `GET /ready`
- Covers health/ready, register/login/whoami, session takeover with control SSE
  subscribed first and exact `event: session_invalidated` + `data:` framing
  (not a 401 JSON substring), public room create/join/lock/start, anonymous
  spectator snapshots (`waiting`/`locked`/`in_progress`), spectator SSE
  subscribed before `DrawCard` then exact canonical `event: SnapshotSanitized` +
  non-empty `data:` (gateway forwards Room event type unchanged),
  stale-sequence reject, tournament create/register/close with idempotent replay
  (same-command-id responses compared on status/type/schemaVersion/sequenceNumber/payload),
  leaderboard/analytics schema reads validated against
  `fixtures/public-read-schema.json` (empty projections allowed), unknown
  `Last-Event-ID` → `snapshot_required` (raw stream HTTP 409 + CLI evidence;
  bounded so an open-SSE regression cannot hang), and `CancelRoom` as the deterministic
  live representative of terminal spectator denial (raw stream HTTP 403 + CLI
  evidence `403` or `spectator_denied`; no hang). `RoomCompleted` denial is
  covered by existing room-gameplay / spectator-view domain and service tests —
  this harness does not invent a nondeterministic full-game completion path
- Background CLI streams run in their own process groups; trap cleanup reaps
  descendants (pipelines / non-exec children) and is idempotent across INT→EXIT
- Tears down with `down -v --remove-orphans` only for a stack this harness started
- `KEEP_STACK=1` leaves a harness-started stack up (background PIDs are still reaped)
- `CAP_SKIP_UP=1` reuses an existing stack at the caller’s `UNOARENA_API_URL`
  (preserved; never synthesized). Does not tear down external projects/volumes
  unless `CAP_TEARDOWN_EXTERNAL=1` and `CAP_COMPOSE_PROJECT` are set explicitly

Helpers live under `client-checkpoint/tests/lib/`; schema fixtures under
`client-checkpoint/tests/fixtures/` (used to validate live public-read responses,
not merely asserted present on disk). Focused helper checks (no Docker stack):

```bash
./client-checkpoint/tests/test-capability-helpers.sh
```

Compose topology checks (resolved `config` only; no stack up):

```bash
make test-compose-topology
# or:
./client-checkpoint/tests/test-compose-topology.sh
```

## GUI status

Graphical UI is deferred. This CLI is the sole current client interface.
