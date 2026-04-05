# Aggregates, Entities, and Value Objects

## Candidate Aggregates and Consistency Boundaries

## Room Gameplay

### Aggregate: `Room`

**Consistency boundary**
One room hosts one match. All changes that affect roster, turn authority, match score, room status, or game progression must be decided here.

**Entities**

- `Seat`
- `CurrentGame`

**Value Objects**

- `RoomStatus`
- `TurnOrder`
- `SequenceNumber`
- `ActiveColor`
- `PenaltyStack`
- `MatchScore`
- `UnoWindow`

**Key invariants**

1. A room can only move forward through `waiting -> locked -> in_progress -> completed` or `waiting -> cancelled`.
2. A command that changes gameplay is only accepted if the acting player is the current turn owner and the submitted sequence number matches the current room version.
3. A player can only play a card currently held in their hand for the active game.
4. A room hosts exactly one active game at a time and at most one active match.
5. A match completes as soon as one player reaches two game wins.
6. No new players may join after the room is locked.
7. Once the room is completed, no gameplay command can change state.

## Game Integrity

### Aggregate: `AuthoritativeDeck`

**Consistency boundary**
One deck per active game. Owns the seed, shuffle state, and draw pointer.

**Value Objects**

- `DeckSeed`
- `DrawPointer`
- `DrawResult`

**Key invariants**

1. The seed is immutable after deck creation.
2. Draws are strictly sequential; the same draw position cannot produce two different results.
3. The same seed and draw pointer must always reproduce the same card order.

### Aggregate: `GameLog`

**Consistency boundary**
Append-only event history for one room.

**Entities**

- `GameLogEntry`

**Value Objects**

- `ReplayPosition`
- `LogOffset`

**Key invariants**

1. Entries are append-only and immutable.
2. Offsets are gapless and strictly ordered.
3. Duplicate append requests with the same event identity are ignored rather than written twice.

## Tournament Orchestration

### Aggregate: `Tournament`

**Consistency boundary**
Owns overall tournament lifecycle and references rounds by identity.

**Value Objects**

- `TournamentPhase`
- `TournamentRules`
- `TournamentSize`

**Key invariants**

1. Phases only move forward: `registration -> seeding -> in_progress -> completed` or `cancelled`.
2. Registration cannot exceed tournament capacity.
3. The tournament cannot complete until the final round has a winner.

### Aggregate: `Round`

**Consistency boundary**
Owns bracket slots and completion state for one elimination tier.

**Entities**

- `BracketSlot`
- `AssignedMatch`

**Value Objects**

- `RoundStatus`
- `AdvancementRule`

**Key invariants**

1. A slot result can only be recorded from the room assigned to that slot.
2. A winner already recorded for a slot cannot be overwritten by a duplicate or conflicting completion message.
3. A round completes only when every assigned match has a terminal outcome: win, forfeit, or disqualification.

## Ranking

### Aggregate: `PlayerRating`

**Consistency boundary**
Owns one player's persistent competitive rating.

**Value Objects**

- `EloRating`
- `RatingDelta`
- `RatingUpdateReason`

**Key invariants**

1. Rating changes are derived only from authoritative completed rated matches.
2. The same completed match cannot be applied twice.
3. Rating cannot drop below the configured floor.

## Identity and Session

### Aggregate: `PlayerSession`

**Consistency boundary**
Owns whether a player's authenticated session is valid for command submission.

**Value Objects**

- `SessionStatus`
- `SessionExpiry`

**Key invariants**

1. Revoked or expired sessions cannot authorize commands.
2. A session is always linked to exactly one `PlayerId`.

## Spectator View

### Aggregate: `SpectatorRoomProjection`

**Consistency boundary**
Projection consistency only. This context does not decide game rules, but it does enforce visibility rules on the view it publishes.

**Entities**

- `VisibleSeat`
- `VisibleDiscardState`

**Value Objects**

- `SpectatorVisibilityPolicy`

**Key invariants**

1. No private hand data may appear in spectator projections.
2. Projected order must be monotonic by upstream event sequence.
3. If an event cannot be safely filtered, it is dropped and an internal audit signal is emitted.

## Reference-by-Identity Rules

- `Room` stores `TournamentId` only when the match is tournament-assigned.
- `Round` stores `RoomId` references, not embedded room state.
- `PlayerRating` stores `PlayerId` and processed `MatchId` references, not full match history objects.
- `SpectatorRoomProjection` stores `RoomId` and visible seat/player references only.

This keeps each aggregate small and prevents accidental cross-context transactional coupling.
