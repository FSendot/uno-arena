# Commands and Domain Events Catalog

This catalog lists the main commands, their primary emitted events, causality, and idempotency expectations.

## Room Setup and Match Lifecycle

| Command | Issuer | Primary Event(s) | Causality | Idempotency |
| --- | --- | --- | --- | --- |
| `CreateRoom` | Player | `RoomCreated` | Player starts an ad-hoc room; event payload includes `roomId`, `hostPlayerId`, and `status` | Duplicate client retries collapse by request key |
| `JoinRoom` | Player | `PlayerJoinedRoom` | Player joins an open room | Idempotent by `(roomId, playerId)` while already seated |
| `LeaveRoom` | Player | `PlayerLeftRoom`, optionally `HostReassigned` or `RoomCancelled` | Player exits before lock or disconnect policy removes them. In an ad-hoc room before lock/start, if the leaving player was host and at least one player remains, the remaining player in the lowest occupied seat becomes host and `HostReassigned` is emitted; if nobody remains, `RoomCancelled` is emitted immediately. After lock/start, host reassignment has no gameplay authority | Idempotent if player is already absent |
| `LockRoom` | Host or system | `RoomLocked` | Host starts readiness cutoff, or tournament assignment seals roster | Duplicate lock commands ignored after first success |
| `StartMatch` | Host or system | `MatchStarted`, `GameStarted` | Triggered once roster is valid and initial game is created | Idempotent by room version and match status |
| `CancelRoom` | Host or system | `RoomCancelled` | Waiting room is abandoned before match start | Idempotent once cancelled |

## Gameplay Commands

| Command | Issuer | Primary Event(s) | Causality | Idempotency |
| --- | --- | --- | --- | --- |
| `PlayCard` | Player | `CardPlayed`, optionally `PenaltyStackIncreased` or `TurnAdvanced` | Accepted when sequence, session, rate-limit, and hand ownership are valid and the player either owns the turn, legally stacks a draw card as the targeted player, or makes an exact-match jump-in. Payload includes `roomId`, `gameId`, `playerId`, `card`, `sequenceNumber`, and `playMode` (`turn`, `stack`, or `jump_in`). A stack adds 2 or 4 and transfers the penalty; a jump-in makes the jumper the acting player and continues turn order after that seat | Command deduped by command id; sequence serialization selects one winner among concurrent turn and jump-in attempts |
| `DrawCard` | Player | `CardDrawn`, optionally `PenaltyStackResolved`, then `TurnAdvanced` or `DrawTurnRetained` | Used when a player cannot or chooses not to play. If the player is targeted by a penalty stack and does not stack, the command draws the accumulated total and forfeits the rest of the turn | Duplicate submissions return the previously decided outcome |
| `ChooseColor` | Player | `ColorChosen` | Follows a wild-card play that requires a color decision | Same command id must not apply twice |
| `CallUno` | Player | `UnoCalled` | Allowed only before the 5-second Uno window closes or the next player begins their turn. A successful call closes and resolves the challenge window so later `ReportMissingUno` is inactive. Window openness is decided solely by server state using absolute UTC `expiresAt` and `openingSequence`; client countdown is advisory | Duplicate calls after success are ignored |
| `ReportMissingUno` | Opponent or system | `UnoChallengeIssued`, `UnoPenaltyApplied` | Valid only while the challenge window is still open and the target has not successfully called Uno; penalty event includes `roomId`, `targetPlayerId`, `challengerPlayerId`, and `cardsDrawn=2`. After a successful `CallUno`, expiry, or turn advance, the command is rejected with no facts and no challenger penalty | Idempotent by `(targetPlayerId, triggeringGameEventId)` |
| `ExpireUnoWindow` | Room Gameplay policy | `UnoWindowExpired` | Internal timer command triggered when the persisted absolute UTC Uno deadline is reached and the window was not already closed by `CallUno`, `ReportMissingUno`, or `TurnAdvanced`. Opening the window publishes `expiresAt` and `openingSequence` to clients | Idempotent by `(roomId, gameId, playerId, triggeringGameEventId)` |
| `EndTurn` | System policy | `TurnAdvanced` | Triggered after the accepted action fully resolves | Internal command; ignored if turn already advanced |

## Game and Match Completion

| Command | Issuer | Primary Event(s) | Causality | Idempotency |
| --- | --- | --- | --- | --- |
| `CompleteGame` | Room Gameplay policy | `GameCompleted`, `MatchScoreUpdated` | Triggered when a player empties their hand or wins by rule; event payload includes `gameId`, `roomId`, `placementOrder`, authoritative `participants`, and `isAbandoned` | Same winning state must not be recorded twice |
| `StartNextGame` | Room Gameplay policy | `GameStarted` | Triggered if no player has yet won the best-of-three match | Idempotent if next game already exists |
| `CompleteMatch` | Room Gameplay policy | `MatchCompleted`, `RoomCompleted` | Triggered when a player reaches two game wins | Idempotent by `(roomId, matchNumber)` |

## Tournament Commands

| Command | Issuer | Primary Event(s) | Causality | Idempotency |
| --- | --- | --- | --- | --- |
| `CreateTournament` | Organizer | `TournamentCreated` | Organizer defines tournament rules and capacity | Duplicate requests collapse by request key |
| `RegisterPlayer` | Player or organizer | `PlayerRegisteredInTournament` | Validated through Identity and Session | Idempotent by `(tournamentId, playerId)` |
| `CloseRegistration` | System or organizer | `TournamentRegistrationClosed` | Triggered at capacity or deadline | Duplicate close ignored |
| `SeedRound` | Tournament policy | `TournamentRoundSeeded` | Triggered after registration closes | Idempotent by `(tournamentId, roundNumber)`. Durable path schedules ROUND-1 only (worker finalizes); `TournamentRoundSeeded` is internal-only (no Kafka channel). |
| `ProvisionRoundMatches` | Tournament policy | `TournamentMatchAssigned` | Creates room assignments for bracket slots | Duplicate assignments ignored by slot identity |
| `RecordMatchResult` | Tournament policy | `TournamentMatchResultRecorded`, `PlayersAdvanced` | Consumes authoritative `MatchCompleted` containing ranked match facts such as match wins, card points, completion time, and forfeit/abandonment markers; Tournament Orchestration calculates the advancing players | Idempotent by `(roomId, completionVersion)` |
| `CompleteRound` | Tournament policy | `TournamentRoundCompleted` | Triggered when all assigned matches are terminal | Idempotent by round status |
| `CompleteTournament` | Tournament policy | `TournamentCompleted` | Triggered when the final room has an authoritative ranked result; publishes every final-room player in ordered `finalStandings`, whose first entry is the champion | Idempotent by tournament status |

## Ranking Commands

| Command | Issuer | Primary Event(s) | Causality | Idempotency |
| --- | --- | --- | --- | --- |
| `ApplyCasualEloUpdate` | Ranking policy | `PlayerRatingUpdated` | Consumes authoritative completed non-abandoned casual `GameCompleted`; event payload includes `playerId`, `gameId`, `previousRating`, `newRating`, and placement | Idempotent by `(playerId, gameId)` |
| `ApplyTournamentPlacementUpdate` | Ranking policy | `TournamentPlacementRatingUpdated` | Consumes advancement depth from `PlayersAdvanced` and ordered final placement from `TournamentCompleted`; Ranking awards a non-negative achievement increment and never accepts a delta from Tournament Orchestration | Idempotent by `(playerId, tournamentId, placementEventId)`, where `placementEventId` is the upstream event `eventId` |
| `PublishLeaderboardSnapshot` | Ranking snapshot worker | `LeaderboardSnapshotPublished` | A score-changing transaction marks the board dirty; worker coalesces and publishes at most once per board every 15 seconds | Publishes only the ordered public top 100; zero-delta facts do not dirty the board; generation may repeat safely |

Tournament achievement awards are fixed by Ranking policy: each accepted advancement is worth 10 points, while final places first through tenth are worth `100, 70, 50, 35, 25, 20, 15, 10, 5, 0` points. A smaller final uses the prefix of that table.

The tenth-place zero-point result remains an accepted, idempotent history fact and emits a zero-delta `PlayerRatingUpdated`; it does not trigger a leaderboard snapshot because the score is unchanged.

Each tournament-performance event is one Ranking transaction across all affected players. Ratings, history, per-player idempotency, the stable event outcome, and all rating-update outbox rows commit together; any invalid or conflicting participant prevents every update from that event.

## Identity and Session Commands

| Command | Issuer | Primary Event(s) | Causality | Idempotency |
| --- | --- | --- | --- | --- |
| `LoginPlayer` | Player | `SessionInvalidated`, `PlayerLoggedIn` | A successful new login invalidates the previous active session before the new session becomes active | Idempotent by login request key |
| `InvalidateSession` | Identity and Session | `SessionInvalidated` | Triggered by new login, explicit logout, expiry, or takeover response | Idempotent by `sessionId` |

## Disconnection Commands

| Command | Issuer | Primary Event(s) | Causality | Idempotency |
| --- | --- | --- | --- | --- |
| `ReconnectToRoom` | Player | `PlayerReconnected` | Accepted within the fixed 60-second reconnection window; original seat and hand remain intact | Idempotent by `(roomId, playerId, disconnectVersion)` |
| `SkipDisconnectedTurn` | Room Gameplay policy | `TurnSkipped` | A disconnected player's turn is skipped during the reconnection window with no bot substitution | Idempotent by `(roomId, playerId, turnVersion)` |
| `ForfeitPlayer` | Room Gameplay or Tournament policy | `PlayerForfeited` | Emitted once when the 60-second reconnect deadline expires; casual rooms continue without the player, tournament rooms mark match loss/elimination | Idempotent by `(roomId, playerId, disconnectVersion)` |

## Spectator View Projection Policies

| Projection Handler | Issuer | Primary Event(s) | Causality | Idempotency |
| --- | --- | --- | --- | --- |
| `AdmitSpectatorConnection` | Spectator admission policy | none as domain event; stream opens or admission is denied | New spectator connections are admitted while room status is `waiting`, `locked`, or `in_progress` and public/private authorization passes; denied after `RoomCompleted` or `RoomCancelled`. Terminal means the complete match/room, not an individual game in a best-of-three | Idempotent by `(roomId, spectatorConnectionId)` for reconnect within the same non-terminal room |
| `CloseSpectatorStreamsOnTerminalRoom` | Spectator projection policy | stream-close control signal | Triggered by `RoomCompleted` or `RoomCancelled`; closes existing spectator streams for that room/match | Idempotent by `(roomId, terminalEventId)` |
| `ProjectRoomEventForSpectators` | Spectator projection policy | `SpectatorRoomProjectionUpdated` | Consumes spectator-safe room events, including public Uno `expiresAt` and opening room sequence when a window opens | Idempotent by upstream event id |
| `DropUnsafeSpectatorEvent` | Spectator visibility policy | `SpectatorEventDropped` | Triggered when an event cannot be safely exposed | Idempotent by upstream event id |

## Analytics and Public Read Model Projection Policies

| Projection Handler | Issuer | Primary Event(s) | Causality | Idempotency |
| --- | --- | --- | --- | --- |
| `ProjectGameplayMetric` | Analytics projection policy | `PublicGameplayMetricProjected` | Consumes sanitized gameplay metrics from Room Gameplay; ad-hoc metrics are anonymized before projection | Idempotent by upstream event id |
| `ProjectTournamentStatistic` | Analytics projection policy | `PublicTournamentStatisticProjected` | Consumes public tournament lifecycle, result, and advancement facts | Idempotent by upstream event id |
| `ProjectRatingStatistic` | Analytics projection policy | `PublicRatingStatisticProjected` | Consumes public rating update facts or ranking snapshots from Ranking | Idempotent by upstream event id |

## Saga, Reconciliation, and Fallback Commands

These commands model fallback outcomes that are meaningful to the business process.

| Command | Issuer | Primary Event(s) | Causality | Idempotency |
| --- | --- | --- | --- | --- |
| `RetryTournamentProvisioningBatch` | Tournament provisioning policy | `TournamentProvisioningBatchRetried` | Reissues missing room-provisioning work for a deterministic slot batch after a retryable worker or Room Gameplay failure | Idempotent by `(tournamentId, roundNumber, batchId, retryAttempt)` |
| `QuarantineTournamentProvisioningBatch` | Tournament provisioning policy | `TournamentProvisioningBatchQuarantined` | Marks a provisioning batch for operator review after retry budget is exhausted or conflicting room assignments are detected | Idempotent by `(tournamentId, roundNumber, batchId)` |
| `QuarantineTournamentResult` | Tournament policy | `TournamentResultQuarantined` | A consumed `MatchCompleted` conflicts with slot ownership, completion version, or already-recorded result | Idempotent by `(roomId, completionVersion)` |
| `ReconcileRoomStateFromIntegrityLog` | Room recovery policy | `RoomStateReconciled` | Rebuilds or repairs Room Gameplay operational state from the Game Integrity log after a partial failure between append and local commit | Idempotent by `(roomId, logOffset)` |
| `QuarantineUnsafeProjectionEvent` | Spectator or Analytics projection policy | `ProjectionEventQuarantined` | A projector receives an event that violates privacy, anonymization, schema, or source-ownership rules | Idempotent by upstream event id |

## Rejection and No-Event Cases

The following cases are intentionally treated as command outcomes rather than domain events because the domain state does not change:

- stale `PlayCard` with old sequence number
- out-of-turn `PlayCard` that is not an exact-match jump-in
- jump-in attempted while a penalty stack, wild-color choice, or other mandatory resolution is pending
- attempted draw-card stack by anyone other than the currently targeted player, or with an illegally playable card
- replayed command with already-consumed idempotency key
- command from expired or revoked session
- `JoinRoom` after lock or capacity reached
- `CallUno` after the Uno window has already closed
- `ReportMissingUno` after a successful `CallUno`, after `UnoWindowExpired`, or after the next turn has begun (window inactive; no challenger penalty)
- command from a displaced session after a newer login invalidated it
- forged future sequence number or replayed old sequence number

Terminal spectator admission denial after `RoomCompleted` or `RoomCancelled` is connection/control behavior outside the command envelope. It produces no domain event and is outside rejected-command audit requirements.

Rejected commands never produce domain events and never append Game Integrity log entries. They must emit a structured operational/security audit record that includes:

- `commandId`
- `correlationId`
- session/player identity when known
- room/tournament identity when known
- rejection reason
- submitted and current sequence numbers when applicable
- timestamp

These audit records are observability and security artifacts, not business events, and they do not belong in the Game Integrity stream.
