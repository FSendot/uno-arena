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
- `Card`
- `Hand`

**Key invariants**

1. A room can only move forward through `waiting -> locked -> in_progress -> completed` or `waiting -> cancelled`.
2. A command that changes gameplay is only accepted if the acting player is the current turn owner and the submitted sequence number matches the current room version.
3. A player can only play a card currently held in their hand for the active game.
4. A room hosts exactly one active game at a time and at most one active match.
5. A match completes as soon as one player reaches two game wins, but tournament advancement uses the full ranked match result, not only the winner.
6. No new players may join after the room is locked.
7. Once the room is completed, no gameplay command can change state.
8. The Uno challenge window closes exactly 5 seconds after the second-to-last card is played or as soon as the next player begins their turn, whichever comes first.
9. A player who fails to call Uno and is successfully challenged draws 2 penalty cards; an invalid challenger draws 2 cards instead.
10. During a disconnection window, the disconnected player's turn is skipped for up to 60 seconds with no bot substitution; reconnecting within the window restores the original hand.

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
3. The tournament cannot complete until 10 or fewer remaining players have played one final room and that final room has a terminal ranked result.

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
2. An advancement already recorded for a slot cannot be overwritten by a duplicate or conflicting completion message.
3. A non-final match advances exactly the top 3 players by match wins, with ties broken by lowest cumulative card points and then earliest final-game completion time.
4. A round completes only when every assigned match has a terminal outcome: win, forfeit, or disqualification, and all resulting advancement slots are filled.

## Ranking

### Aggregate: `PlayerRating`

**Consistency boundary**
Owns one player's persistent competitive rating.

**Value Objects**

- `EloRating`
- `TournamentPlacementRating`
- `RatingDelta`
- `TournamentPlacementDelta`
- `RatingUpdateReason`

**Key invariants**

1. Casual Elo changes are derived only from authoritative completed non-abandoned ad-hoc games.
2. Tournament games update a separate tournament-placement rating, never casual Elo.
3. The same completed game or tournament placement cannot be applied twice.
4. Rating cannot drop below the configured floor.

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
3. A player can have only one active session at a time; a new successful login invalidates the previous active session.

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

## Analytics and Public Read Models

### Aggregate: `PublicAnalyticsProjection`

**Consistency boundary**
Projection consistency only. This context owns derived public reporting state and does not decide gameplay, tournament advancement, ratings, session validity, spectator privacy, or audit truth.

**Entities**

- `GameplayMetric`
- `TournamentStatistic`
- `PlayerPublicStatistic`

**Value Objects**

- `ProjectionVersion`
- `MetricWindow`
- `AnonymizationPolicy`

**Key invariants**

1. Ad-hoc gameplay metrics are anonymized before they enter public analytics projections.
2. Public tournament metrics may include only facts that are already public through tournament or spectator views.
3. No private hand contents, hidden deck order, private draw identities, session tokens, or raw audit metadata may appear in analytics projections.
4. Analytics projections are rebuildable from upstream published events and do not become authoritative sources for domain decisions.

## Reference-by-Identity Rules

- `Room` stores `TournamentId` only when the match is tournament-assigned.
- `Round` stores `RoomId` references, not embedded room state.
- `PlayerRating` stores `PlayerId` plus processed `GameId` and tournament placement event references, not full room or tournament history objects.
- `SpectatorRoomProjection` stores `RoomId` and visible seat/player references only.
- `PublicAnalyticsProjection` stores derived metric identifiers and source event references, not mutable source aggregates.

This keeps each aggregate small and prevents accidental cross-context transactional coupling.
