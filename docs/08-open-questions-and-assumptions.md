# Open Questions and Assumptions

This document separates assumptions made for modeling purposes from requirements that appear validated by the assignment statement.

## Validated Requirements

- A spectator capability exists and must be modeled explicitly.
- The language must distinguish `game`, `match`, `round`, and `tournament`.
- Tournament progression, Elo updates, and room completion must be described as event flows.
- Edge cases must include concurrent actions, disconnections, stale/replayed commands, partial failures, security/abuse, and spectator privacy violations.
- The submission should be provided as markdown documents with a root index.

## Assumptions

These assumptions were used to make the domain model concrete where the brief does not define exact rules.

### Gameplay assumptions

- A room hosts exactly one best-of-three match.
- A match is decided when one player wins two games.
- Standard Uno-like turn, draw, wild-color, and Uno-call mechanics apply.
- Rejected stale commands are treated as API outcomes rather than business events.

### Tournament assumptions

- Tournament matches reuse the same room gameplay model as ad-hoc matches.
- Tournament advancement is based on authoritative `MatchCompleted`, not on intermediate `GameCompleted`.
- A round may advance winners only after every assigned match reaches a terminal outcome.

### Ranking assumptions

- Elo is updated once per completed rated match, not once per individual game.
- Casual rooms may be unrated depending on product rules.

### Spectator assumptions

- Spectators can observe public room and tournament state in near real time.
- Spectators must never receive private hand data or player-authentication data.
- Spectator projections may lag slightly behind authoritative room state.

## Connection-Semantics Assumptions

These assumptions are included because the assignment asks to surface connection-related scope decisions.

- Client commands may be retried by the network or by reconnecting clients, so idempotency keys are required.
- Clients may disconnect temporarily and later rejoin with the same authenticated identity.
- Room commands use a room-scoped sequence number so clients can detect stale state.
- Spectator and player channels are logically distinct even if they share transport technology.
- Late-arriving events are possible across contexts, so downstream consumers must be idempotent and order-aware.

## Open Questions

The following items remain unresolved and should be clarified with the teaching staff or product owner.

1. What exact Uno ruleset applies for stacking, jump-ins, and timeout behavior?
2. How long is the reconnect grace period before a tournament forfeit is declared?
3. Is the Uno window based on wall-clock time, room logical time, or number of committed events?
4. Should rejected commands be recorded as audit events in high-stakes tournaments, or is operational logging sufficient?
5. Are ratings updated for tournament matches only, all matches, or only selected queues?
6. Can spectators join before the room is locked, or only once the match starts?
7. If host reassignment occurs in ad-hoc rooms, what exact rule selects the new host?

## Why These Assumptions Matter

These assumptions mostly affect:

- invariant design inside the Room aggregate
- the event catalog for completion and forfeits
- whether ranking updates are triggered by `GameCompleted` or only by `MatchCompleted`
- what data Spectator View is permitted to publish

The model is internally consistent under the assumptions above, but those policy questions should be finalized before implementation.
