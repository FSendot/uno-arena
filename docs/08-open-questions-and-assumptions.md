# Open Questions and Assumptions

This document separates assumptions made for modeling purposes from requirements that appear validated by the assignment statement.

## Validated Requirements

- A spectator capability exists and must be modeled explicitly.
- The language must distinguish `game`, `match`, `round`, and `tournament`.
- Tournament progression, Elo updates, and room completion must be described as event flows.
- Edge cases must include concurrent actions, disconnections, stale/replayed commands, partial failures, security/abuse, and spectator privacy violations.
- The submission should be provided as markdown documents with a root index.
- The Uno challenge window is fixed at 5 seconds and closes earlier if the next player begins their turn.
- The reconnection window is fixed at 60 seconds with no bot substitution.
- Tournament non-final matches advance the top 3 players by match wins, then lowest cumulative card points, then earliest final-game completion time.
- Casual Elo is updated once per completed non-abandoned ad-hoc game; tournament play uses a separate placement rating.
- A new login invalidates the player's previous active session.

## Assumptions

These assumptions were used to make the domain model concrete where the brief does not define exact rules.

### Gameplay assumptions

- A room hosts exactly one best-of-three match.
- A match is decided when one player wins two games, while tournament advancement still records the ranked top 3 for non-final matches.
- Standard Uno-like turn, draw, wild-color, and Uno-call mechanics apply.
- Rejected stale commands are treated as API outcomes rather than business events.

### Tournament assumptions

- Tournament matches reuse the same room gameplay model as ad-hoc matches.
- Tournament advancement is calculated by Tournament Orchestration from authoritative `MatchCompleted` facts, including match wins, card points, completion time, and forfeit/abandonment markers.
- A round may advance players only after every assigned match reaches a terminal outcome.

### Ranking assumptions

- Casual Elo is updated once per completed non-abandoned ad-hoc game.
- Tournament placement rating is updated separately from tournament placement and final standing facts.

### Spectator assumptions

- Spectators can observe public room and tournament state in near real time.
- Spectators must never receive private hand data or player-authentication data.
- Spectator projections may lag slightly behind authoritative room state.

### Analytics assumptions

- Public analytics are derived and non-authoritative.
- Ad-hoc gameplay metrics are anonymized before they enter public analytics.
- Public tournament metrics may include already-public tournament facts, but not hidden card, hand, deck, session, or audit data.

## Connection-Semantics Assumptions

These assumptions are included because the assignment asks to surface connection-related scope decisions.

- Client commands may be retried by the network or by reconnecting clients, so idempotency keys are required.
- Clients may disconnect temporarily and later rejoin with the same authenticated identity.
- Room commands use a room-scoped sequence number so clients can detect stale state.
- Spectator and player channels are logically distinct even if they share transport technology.
- Late-arriving events are possible across contexts, so downstream consumers must be idempotent and order-aware.

## Open Questions

The following items remain unresolved and should be clarified with the teaching staff or product owner.

1. What exact optional Uno ruleset applies for stacking and jump-ins?
2. Should rejected commands be recorded as audit events in high-stakes tournaments, or is operational logging sufficient?
3. Can spectators join before the room is locked, or only once the match starts?
4. If host reassignment occurs in ad-hoc rooms, what exact rule selects the new host?
5. How should client UI display server-authoritative 5-second Uno deadlines under latency?

## Why These Assumptions Matter

These assumptions mostly affect:

- invariant design inside the Room aggregate
- the event catalog for completion and forfeits
- how casual Elo `GameCompleted` events are kept separate from tournament placement events
- what data Spectator View is permitted to publish

The model is internally consistent under the assumptions above, but those policy questions should be finalized before implementation.
