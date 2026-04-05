# Commands and Domain Events Catalog

This catalog lists the main commands, their primary emitted events, causality, and idempotency expectations.

## Room Setup and Match Lifecycle

| Command | Issuer | Primary Event(s) | Causality | Idempotency |
| --- | --- | --- | --- | --- |
| `CreateRoom` | Player | `RoomCreated` | Player starts an ad-hoc room | Duplicate client retries collapse by request key |
| `JoinRoom` | Player | `PlayerJoinedRoom` | Player joins an open room | Idempotent by `(roomId, playerId)` while already seated |
| `LeaveRoom` | Player | `PlayerLeftRoom` | Player exits before lock or disconnect policy removes them | Idempotent if player is already absent |
| `LockRoom` | Host or system | `RoomLocked` | Host starts readiness cutoff, or tournament assignment seals roster | Duplicate lock commands ignored after first success |
| `StartMatch` | Host or system | `MatchStarted`, `GameStarted` | Triggered once roster is valid and initial game is created | Idempotent by room version and match status |
| `CancelRoom` | Host or system | `RoomCancelled` | Waiting room is abandoned before match start | Idempotent once cancelled |

## Gameplay Commands

| Command | Issuer | Primary Event(s) | Causality | Idempotency |
| --- | --- | --- | --- | --- |
| `PlayCard` | Player | `CardPlayed` | Accepted if turn, sequence, and hand ownership are valid | Command deduped by command id; stale or conflicting commands are rejected |
| `DrawCard` | Player | `CardDrawn`, `TurnAdvanced` or `DrawTurnRetained` | Used when player cannot or chooses not to play | Duplicate submissions return the previously decided outcome |
| `ChooseColor` | Player | `ColorChosen` | Follows a wild-card play that requires a color decision | Same command id must not apply twice |
| `CallUno` | Player | `UnoCalled` | Allowed only inside the active Uno window | Duplicate calls after success are ignored |
| `ReportMissingUno` | Opponent or system | `UnoPenaltyApplied` | Triggered when eligible target failed to call Uno in time | Idempotent by `(targetPlayerId, triggeringGameEventId)` |
| `EndTurn` | System policy | `TurnAdvanced` | Triggered after the accepted action fully resolves | Internal command; ignored if turn already advanced |

## Game and Match Completion

| Command | Issuer | Primary Event(s) | Causality | Idempotency |
| --- | --- | --- | --- | --- |
| `CompleteGame` | Room Gameplay policy | `GameCompleted`, `MatchScoreUpdated` | Triggered when a player empties their hand or wins by rule | Same winning state must not be recorded twice |
| `StartNextGame` | Room Gameplay policy | `GameStarted` | Triggered if no player has yet won the best-of-three match | Idempotent if next game already exists |
| `CompleteMatch` | Room Gameplay policy | `MatchCompleted`, `RoomCompleted` | Triggered when a player reaches two game wins | Idempotent by `(roomId, matchNumber)` |

## Tournament Commands

| Command | Issuer | Primary Event(s) | Causality | Idempotency |
| --- | --- | --- | --- | --- |
| `CreateTournament` | Organizer | `TournamentCreated` | Organizer defines tournament rules and capacity | Duplicate requests collapse by request key |
| `RegisterPlayer` | Player or organizer | `PlayerRegisteredInTournament` | Validated through Identity and Session | Idempotent by `(tournamentId, playerId)` |
| `CloseRegistration` | System or organizer | `TournamentRegistrationClosed` | Triggered at capacity or deadline | Duplicate close ignored |
| `SeedRound` | Tournament policy | `TournamentRoundSeeded` | Triggered after registration closes | Idempotent by `(tournamentId, roundNumber)` |
| `ProvisionRoundMatches` | Tournament policy | `TournamentMatchAssigned` | Creates room assignments for bracket slots | Duplicate assignments ignored by slot identity |
| `RecordMatchResult` | Tournament policy | `TournamentMatchResultRecorded`, `WinnerAdvanced` | Consumes authoritative `MatchCompleted` | Idempotent by `(roomId, completionVersion)` |
| `CompleteRound` | Tournament policy | `TournamentRoundCompleted` | Triggered when all assigned matches are terminal | Idempotent by round status |
| `CompleteTournament` | Tournament policy | `TournamentCompleted` | Triggered when final round has a winner | Idempotent by tournament status |

## Ranking Commands

| Command | Issuer | Primary Event(s) | Causality | Idempotency |
| --- | --- | --- | --- | --- |
| `ApplyRatingUpdate` | Ranking policy | `PlayerRatingUpdated` | Consumes authoritative completed rated match | Idempotent by `(playerId, matchId)` |
| `PublishLeaderboardSnapshot` | Ranking policy | `LeaderboardSnapshotPublished` | Triggered after rating updates or on schedule | Snapshot generation may repeat safely |

## Spectator View Commands

| Command | Issuer | Primary Event(s) | Causality | Idempotency |
| --- | --- | --- | --- | --- |
| `ProjectRoomEventForSpectators` | Spectator projection policy | `SpectatorRoomProjectionUpdated` | Consumes spectator-safe room events | Idempotent by upstream event id |
| `ProjectTournamentEventForSpectators` | Spectator projection policy | `SpectatorBracketProjectionUpdated` | Consumes tournament round and advancement events | Idempotent by upstream event id |
| `DropUnsafeSpectatorEvent` | Spectator visibility policy | `SpectatorEventDropped` | Triggered when an event cannot be safely exposed | Idempotent by upstream event id |

## Rejection and No-Event Cases

The following cases are intentionally treated as command outcomes rather than domain events because the domain state does not change:

- stale `PlayCard` with old sequence number
- replayed command with already-consumed idempotency key
- command from expired or revoked session
- `JoinRoom` after lock or capacity reached
- `CallUno` after the Uno window has already closed

These outcomes should still be observable operationally through logs or metrics, but they do not belong in the business event stream unless audit requirements explicitly demand rejection events.
