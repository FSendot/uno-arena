# Client Checkpoint CLI

This directory contains the Client Checkpoint command-line artifact used as the
canonical client and smoke-test entry point for this implementation.

For this checkpoint, the repo-owned simple CLI is the **sole client and automated
test interface**. A graphical interface is **explicitly deferred** for later
refactoring. Deferred UI work must continue to speak only to the Realtime
Gateway / BFF and must not introduce direct microservice access.

## Stage status

| Stage | Scope | Status |
| --- | --- | --- |
| **A** | Auth/session/seed, room utilities, tournament utilities, spectate alias, packaging, offline tests | **Implemented** |
| **B** | Interactive `play` (live feed + turn board) and headless `bot` (JSONL load) | **Implemented** |

## Requirements

- `curl` (HTTP/SSE)
- `bash`
- `python3` (stdlib only; JSON encoding/validation, RFC3339 countdown math, offline tests)

## Configuration

| Variable | Required | Purpose |
| --- | --- | --- |
| `UNOARENA_API_URL` | Yes (except `countdown`, or `logout` when no local session exists) | Gateway/BFF base URL. The CLI never calls microservices directly. |
| `UNOARENA_TOKEN` | No | Default bearer token when `--token` is omitted (after `--token`, before session file) |
| `UNOARENA_SESSION_FILE` | No | Session file path. Default: `${XDG_STATE_HOME:-$HOME/.local/state}/unoarena/session.json` |
| `UNOARENA_ROOM_START_TIMEOUT_SECONDS` | No | Total retry budget for safe `503 room_starting` snapshot/assignment reads. Default: `60`; each retry honors `Retry-After` with jitter and a five-second interval cap. |

Token resolution for one-shot utilities: `--token` → `UNOARENA_TOKEN` → session file.
Corrupt or group/world-readable session files fail closed.

Room creation and assignment are asynchronous to dedicated runtime admission.
Only player-snapshot and tournament-assignment GETs retry a structured
`503 room_starting`; the bounded policy honors `Retry-After`, adds jitter, and
caps each interval at five seconds. Exhaustion is a structured nonzero failure.
The client never automatically retries a mutation or any unknown outcome.

## Invocation

Native:

```bash
export UNOARENA_API_URL=http://127.0.0.1:8080
./client-checkpoint/bin/unoarena health
```

Docker (same image targets any BFF via `UNOARENA_API_URL`):

```bash
docker build -f client-checkpoint/Dockerfile -t uno-arena/client:local ./client-checkpoint
docker run --rm -e UNOARENA_API_URL=http://host.docker.internal:8080 uno-arena/client:local health
docker run --rm -e UNOARENA_API_URL=… -e UNOARENA_TOKEN=… uno-arena/client:local whoami
```

## Canonical commands

All HTTP commands use OpenAPI BFF paths on `UNOARENA_API_URL`. Utility HTTP
responses are JSON. Interactive `play --json` and every `bot` run emit the
machine-readable JSON Lines contract.

```bash
# Health / public reads
unoarena health
unoarena leaderboard
unoarena analytics

# Auth / session
unoarena register --user <username> --pass <password>
unoarena login --user <username> --pass <password>   # prints login JSON; also writes session file (0600)
unoarena logout                                      # authoritatively revokes current session, then removes local file
unoarena whoami [--token <bearer-token>]
unoarena seed --count <N> [--prefix <p>]             # JSONL credentials; does not write interactive session

# Rooms
unoarena room create [--max <N>] [--token …]         # generates roomId when omitted; max 2..10
unoarena room create --token … --payload <json>      # preserved low-level form
unoarena room list [--status waiting|locked|in_progress] [--limit n] [--cursor c]
unoarena room join <roomId> [--token …]
unoarena room leave [<roomId>] [--token …]           # roomId from arg or session current room

# Generic command envelopes
unoarena command --token … --type <CommandType> \
  [--room-id <roomId>] [--command-id <id>] --expected-sequence <n> \
  [--payload <json>] [--schema-version <n>] [--correlation-id <id>]

# Tournaments
unoarena tournament create --token … [--command-id <id>] [--payload <json>]
unoarena tournament register <tournamentId> [--token …]
unoarena tournament register --token … --payload <json>   # preserved low-level form
unoarena tournament close-registration --token … [--command-id <id>] [--payload <json>]
unoarena tournament status <tournamentId> [--token …]

# Streams / spectator
unoarena stream player --token … --room-id <roomId> [--last-event-id <id>]
unoarena stream spectator --room-id <roomId> [--token …] [--last-event-id <id>]
unoarena spectate <roomId> [--token …] [--last-event-id <id>]   # canonical alias of stream spectator
unoarena stream control --token … [--last-event-id <id>]

# Advisory Uno countdown (local only; no HTTP). --json emits structured JSON.
unoarena countdown --expires-at <RFC3339> --opening-sequence <n> [--now <RFC3339>] [--json]

# Interactive casual play (line-oriented feed and numbered turn board)
unoarena play --casual [--user <u> --pass <p> | --token <token>] [--json]

# Headless random-valid-move load client (JSONL plus final summary)
unoarena bot --casual (--user <u> --pass <p> | --token <token>) [--seed <rng>]
unoarena bot --room <roomId> (--user <u> --pass <p> | --token <token>) [--seed <rng>]
unoarena bot --tournament <tournamentId> (--user <u> --pass <p> | --token <token>) [--seed <rng>]
```

### Gateway paths used

| Command | Method + path |
| --- | --- |
| `health` | `GET /health` |
| `register` | `POST /v1/auth/register` |
| `login` | `POST /v1/auth/login` |
| `logout` | `POST /v1/auth/logout`; local file removed only after success |
| `whoami` | `GET /v1/auth/whoami` (`Authorization: Bearer …`) |
| `seed` | `POST /v1/auth/register` + `POST /v1/auth/login` per account |
| `room list` | `GET /v1/rooms` (public; default `status=waiting`; no bearer) |
| `room create` | `POST /v1/commands` (`CreateRoom`) |
| `room join` | `GET /v1/spectator/rooms/{roomId}/snapshot` then `POST /v1/rooms/{roomId}/commands` (`JoinRoom`) |
| `room leave` | `GET /v1/rooms/{roomId}/snapshot` then `POST /v1/rooms/{roomId}/commands` (`LeaveRoom`) |
| `command --room-id …` | `POST /v1/rooms/{roomId}/commands` |
| `command` / tournament helpers without room | `POST /v1/commands` |
| `tournament register` | `POST /v1/commands` (`RegisterPlayer`) |
| `tournament status` | `GET /v1/tournaments/{id}/standings` + `GET /v1/tournaments/{id}/bracket` |
| `stream player` | `GET /v1/streams/player?roomId=…` |
| `stream spectator` / `spectate` | `GET /v1/streams/spectator?roomId=…` |
| `stream control` | `GET /v1/streams/control` |
| `leaderboard` | `GET /v1/rankings/leaderboards` |
| `analytics` | `GET /v1/analytics/public` |
| `countdown` | none (local advisory display) |
| `play --casual` | `GET /v1/rooms`, optional `CreateRoom`/`JoinRoom`, player snapshot + SSE, room-scoped gameplay commands |
| `bot --casual` / `--room` | Same room/snapshot/command paths as interactive play; autonomous random valid moves |
| `bot --tournament` | `RegisterPlayer`, player assignment read, then the assigned room snapshot/command loop for each round |

BFF snapshot routes (OpenAPI; used after SSE `409 snapshot_required` or reconnect) are part of the settled client contract even when invoked via `curl` rather than a dedicated CLI subcommand:

| Purpose | Method + path |
| --- | --- |
| Player reconnect snapshot | `GET /v1/rooms/{roomId}/snapshot` (bearer required) |
| Spectator projection snapshot | `GET /v1/spectator/rooms/{roomId}/snapshot` (anonymous-tolerant for public non-terminal rooms; private rooms need session/invite/operator; invalid bearer → `401`) |

Player and spectator snapshots include authoritative `drawPileSize` (count only) for the interactive turn board (§5.C). Deck order, seed, and undrawn card identities are never exposed.

### Interactive play

`play --casual` joins an available public waiting room or creates one. It opens
the authenticated player SSE stream and renders a line-oriented board from the
authoritative private snapshot. The board includes canonical card notation,
active color, direction, draw-pile size, opponent counts/UNO flag, and a numbered
private hand where `*` marks locally playable cards.
If the casual client is the host, the same bounded policy used by the bot locks
and starts the room once at least two players are present. Turn-changing feed
events trigger an authoritative snapshot refresh and board re-render; feed and
input state are synchronized so commands never use a partially refreshed view.

At the `uno>` prompt use `play <n>`, `play <n> <R|G|B|Y>` for wilds, `draw`,
`uno`, `challenge`, `pass`, `state`, or `quit`. This backend advances the turn
automatically after drawing, so `pass` is an explicit informational no-op.
Every mutation uses the latest snapshot sequence. A rejected stale command is
printed as a conflict and immediately reconciled from the authoritative player
snapshot. SSE resumes with `Last-Event-ID`; a `409 snapshot_required` also
reloads the snapshot and reconnects without a resume marker—the opaque SSE id
is never replaced by the room sequence number. When that snapshot carries a
`disconnectVersion`, the client first submits `ReconnectToRoom` with that exact
version, preserving the server-owned 60-second reconnection rule. `--json` changes state, feed, and
action output to the §6 JSON Lines shape; interactive prompts are written to
stderr so stdout remains JSON-only.

### Headless bots and load

`bot` authenticates inside its process, enters the selected room source, polls
the authoritative private snapshot, and randomly selects among valid cards
(or draws). `--seed` makes that choice reproducible. A wild is followed by a
random color choice and a one-card hand triggers `CallUno`. Each attempted
action emits one JSON object with `ts`, `action`, `room`, `player`,
`latency_ms`, `result`, `error_code`, `seq`, and `correlationId`; termination
adds a summary containing totals, errors, average latency, and p95 latency.
The exit status is zero for a completed/bounded successful run. Recoverable
rejections remain visible in action records and metrics but do not fail a run
that reconciles and reaches its terminal success condition; authentication,
assignment, timeout, or unrecovered gameplay failures remain non-zero.

Tournament bots register themselves, poll the authenticated per-player
assignment resource, join each newly assigned room, and run the same gameplay
loop. They continue discovering later-round assignments until the tournament
finishes or the run timeout is reached; faculty do not need to orchestrate
individual rooms. For local exercise create tournaments with a low capacity
(for example `2`) before closing registration.

Live kind acceptance has exercised both autonomous paths end to end: two bots
completed a casual best-of-three, and two tournament bots completed the
registration, assignment, room play, result-consumption, and terminal
`TournamentCompleted` flow. Tournament lifecycle policy remains server-owned;
the client never submits internal `SeedRound`, `ProvisionRoundMatches`,
`CompleteRound`, or `CompleteTournament` commands.

For bounded smoke runs, the implementation also accepts hidden harness flags
`--max-actions`, `--max-wait`, and `--poll-interval`. Normal faculty invocation
does not need them.

### Seed convention

- Default prefix: `seed`. Usernames are `{prefix}{NNNN}` (1-based, zero-padded to 4), e.g. `seed0001`.
- Password for each account: `{username}-pass` (deterministic; documented for load harnesses).
- `register` that reports username already taken is treated as ensure-exists; login still runs.
- Stdout is JSON Lines only (one object per account: `username`, `password`, `token`, `playerId`, `sessionId`, `result`).
- Non-zero exit if any account fails. Seeded tokens are **not** written to the interactive session file.
- Hard max `--count`: 10000.

### Session file

`login` still prints the unchanged login JSON on stdout and atomically writes
`token` / `playerId` / `sessionId` / `username` (mode `0600`). After accepted
`room create` / `room join`, `roomId` is stored when a session file already exists;
accepted `room leave` clears it. `logout` calls the authoritative BFF endpoint and
deletes the file only after success; network/timeouts/backend `5xx` return nonzero
and retain it for retry. If no local session exists, logout is locally idempotent.
This revokes only the UnoArena session; it does not sign the user out of the
external identity provider or unrelated applications.

### Command envelope rules

- Request bodies are built with strict JSON encoding (`python3` stdlib).
- `schemaVersion` must equal `1` (digits only). `expectedSequenceNumber` must be a non-negative integer (digits only).
- `--payload` must be a JSON **object** (not an array/string); invalid JSON is rejected before send.
- The public command catalog is closed (15 OpenAPI variants). Unknown `--type` values are rejected locally before send. Tournament clients receive only `CreateTournament`, `RegisterPlayer`, and `CloseRegistration`; seeding, provisioning, round completion, and tournament completion are internal Tournament-owned policy operations.
- `expectedSequenceNumber` (`--expected-sequence`) is **required** for mutations of an existing room aggregate (`JoinRoom`, `PlayCard`, …). It must be **absent** for `CreateRoom` and the three tournament types; passing `--expected-sequence` for those is rejected locally.
- Room IDs in paths and stream query params are URL-encoded.
- Requests send `X-Correlation-Id` (auto-generated or `--correlation-id`) and command submissions also send `X-Command-Id`.
- Rejected commands preserve the `status=rejected` `CommandResult` body and are audit-only, never domain events. Exact `reason=stale_sequence` returns HTTP `409` so clients reconcile; every other domain rejection remains HTTP `200`. Malformed envelopes (unknown type, missing required sequence, prohibited sequence, bad schemaVersion) are HTTP `400` `invalid_envelope`.

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

### Tournament test threshold

Tournament start thresholds are backend-owned. For local/capability exercise, configure a
low capacity on `tournament create` (e.g. `"capacity": 2`) so registration can close
without a production-scale roster.

## Offline tests

Ephemeral stdlib fake gateway (no persistent servers, no external packages):

```bash
make test-client-checkpoint
# or:
./client-checkpoint/tests/run-tests.sh
```

The suite includes focused Stage B coverage for canonical board rendering,
interactive JSONL, stale-command reconciliation, reproducible bot actions,
final summaries, and versioned room command envelopes.

Dockerfile packaging structure (no build/pull):

```bash
make test-client-dockerfile
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
