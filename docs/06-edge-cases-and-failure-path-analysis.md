# Edge Cases and Failure-Path Analysis

For each case, this document states the expected domain behavior and the business-relevant emitted events.

## 1. Concurrent Conflicting Actions

### Example
Two players attempt to play different cards at nearly the same time.

### Expected domain behavior

- Room Gameplay serializes decisions through the room aggregate.
- The first valid command matching the current sequence number is accepted.
- The later command becomes stale once the room version advances and is rejected.
- The rejected action does not change domain state.

### Emitted events

- accepted branch: `CardPlayed`, follow-up events such as `ColorChosen`, `PenaltyApplied`, `TurnAdvanced`
- rejected branch: no business event; API returns stale-command outcome

## 2. Disconnections and Late Rejoin Attempts

### Expected domain behavior

- A temporary disconnect does not immediately remove the player from the room.
- The seat and hand remain reserved for the fixed 60-second reconnection window.
- If the disconnect happens during another player's turn, play continues and the disconnected player can return with the same hand while the window is open.
- If the disconnect happens during the disconnected player's own turn, that turn is skipped during the 60-second window and no bot substitution occurs.
- Rejoin is allowed only for the same authenticated player identity and only while the match is not terminal.
- Reconnecting within the window restores the original seat and hand.
- Reconnecting after the window observes the resulting forfeit state and does not undo it.
- If the window expires in a casual room, the player leaves participation and the game continues with remaining players.
- If the window expires in a tournament room, `PlayerForfeited` counts as a match loss and eliminates the player from advancement.

### Emitted events

- `PlayerDisconnected`
- `PlayerReconnected` when successful
- `TurnSkipped` if the disconnected player's own turn is skipped
- `PlayerForfeited` if the 60-second reconnect window expires
- `MatchCompleted` and `RoomCompleted` if the forfeit resolves the match

## 3. Stale Commands and Replayed Commands

### Expected domain behavior

- A command with an old sequence number is rejected as stale.
- A command with a previously seen idempotency key is recognized as a duplicate.
- If the original command succeeded, the duplicate should return the already-known result without reapplying state.
- If the original command failed validation, the duplicate should return the same failure classification if available.

### Emitted events

- no new business event for stale rejection
- no duplicate event for replayed commands
- optionally `DuplicateCommandDetected` as an internal audit/operations signal, not a domain event

## 4. Partial Failures Between Contexts

### Example
Room Gameplay commits `MatchCompleted`, but Ranking is temporarily unavailable.

### Expected domain behavior

- Room Gameplay remains authoritative and does not roll back the completed match.
- The completion event is durably retried until Ranking consumes it.
- Tournament Orchestration and Spectator View may process the same result earlier than Ranking.
- Eventual consistency is accepted; cross-context rollback is avoided.

### Emitted events

- initial business event: `MatchCompleted`, `RoomCompleted`
- later downstream business events: `PlayerRatingUpdated`, `TournamentMatchResultRecorded`, `PlayersAdvanced`
- retry attempts themselves are infrastructure concerns, not new business events

## 5. Security and Abuse Scenarios

### Session takeover

**Behavior**

- Commands from revoked or displaced sessions are rejected synchronously by Identity and Session.
- If suspicious takeover is confirmed, the session is revoked and the player may be disconnected from active rooms.

**Events**

- `SessionRevoked`
- `PlayerDisconnected` if an in-room session is terminated

### Spam and flooding

**Behavior**

- Repeated invalid commands do not mutate domain state.
- Domain rate-limits or abuse policies may temporarily mute or remove the player from the room or tournament.

**Events**

- `PlayerMutedForAbuse` if the policy is domain-visible
- `PlayerRemovedFromRoomForAbuse` if moderation escalates
- otherwise no business event for each rejected spam request

### Sequence number spoofing and replay

**Behavior**

- Sequence numbers are server-issued room versions, not client authority.
- A forged future sequence number or replayed old sequence number is rejected before domain mutation.
- The rejected command appends no game-log entry and cannot alter turn order, hands, or penalties.

**Events**

- no business event for the rejected command
- optional internal `SuspiciousCommandDetected` audit signal

## 6. Spectator Privacy Violations

### Example
A player attempts to subscribe through the spectator channel to infer another player's hand.

### Expected domain behavior

- Spectator View publishes only filtered projections.
- Private hand events never cross the Spectator View boundary.
- A reconnecting player receives only their own private hand plus public room state, never opponent hands through any channel.
- The immutable GameLog may contain full private state for audit, but Spectator View cannot query the raw log.
- If an unsafe event enters the projector, it is dropped and flagged internally.
- A player opening a second anonymous spectator connection still receives only the spectator-safe projection unless they are separately authorized as a room participant on the player channel.

### Emitted events

- `SpectatorEventDropped` when filtering blocks an unsafe update
- `SpectatorAccessDenied` when the channel or token is invalid
- no hand-revealing domain event is ever emitted to Spectator View

## 7. Conflicting Match Results

### Expected domain behavior

- Tournament Orchestration accepts only results from the room assigned to the bracket slot.
- If two completions arrive for the same slot with the same completion version, duplicates are ignored.
- If a different winner arrives for an already-finalized slot, the result is quarantined for manual resolution or audit replay.

### Emitted events

- accepted result: `TournamentMatchResultRecorded`, `PlayersAdvanced`
- conflict branch: `TournamentResultQuarantined`

## 8. Host Leaves Before Match Start

### Expected domain behavior

- In an ad-hoc room, host ownership is reassigned if the room still has enough players to continue.
- If no eligible host remains, the room can remain joinable, or the system may cancel it after timeout.

### Emitted events

- `HostReassigned`
- `RoomCancelled` if the room cannot continue

## 9. Integrity Replay Reveals Corruption

### Expected domain behavior

- If replayed state does not match the committed room result, the affected match is frozen for investigation.
- Ranking and tournament advancement must stop processing further consequences from that disputed result until resolved.

### Emitted events

- `IntegrityMismatchDetected`
- `MatchFrozenForInvestigation`

This case is rare, but it is important because cash-prize or high-trust tournaments need a clear business response when auditability fails.
