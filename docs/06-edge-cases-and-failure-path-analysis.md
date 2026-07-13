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

- accepted branch: `CardPlayed`, follow-up events such as `ColorChosen`, `UnoPenaltyApplied`, `TurnAdvanced`
- rejected branch: no domain event and no Game Integrity log entry; API returns stale-command outcome and a structured operational/security audit record is emitted with `commandId`, `correlationId`, session/player, room/tournament when known, rejection reason, submitted/current sequence, and timestamp

### Jump-in race

- An out-of-turn `PlayCard` is eligible only when its card exactly matches the discard by color and rank or action symbol and no penalty stack, wild-color choice, or other mandatory resolution is pending.
- A current-turn play and one or more jump-ins may race against the same room sequence number. Room serialization accepts exactly one valid command; all later commands are rejected as stale.
- An accepted jump-in records `CardPlayed` with `playMode=jump_in`, makes the jumper the acting player, applies the card effect normally, and resumes turn order after the jumper's seat.
- An invalid or losing jump-in changes no state, emits no domain event, appends no Game Integrity entry, and records a structured operational/security audit record.

### Draw-card stacking

- Only the player currently targeted by a draw penalty may stack a `Draw Two` or legally playable `Wild Draw Four`.
- Each stacked card emits `CardPlayed` with `playMode=stack` and `PenaltyStackIncreased`, adds 2 or 4 to the accumulated penalty, and transfers the penalty to the next player in turn order.
- The first targeted player who does not or cannot stack draws the accumulated total, emits `CardDrawn` and `PenaltyStackResolved`, and forfeits the rest of the turn.
- Jump-ins cannot intercept or bypass a pending penalty stack. Concurrent stack attempts use the same sequence-number and idempotency rules as other room commands.

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
- The reconnect deadline is persisted with the room state and `disconnectVersion`, so timer retries after worker restart cannot forfeit the same player twice or forfeit a newer reconnection attempt.

### Emitted events

- `PlayerDisconnected`
- `PlayerReconnected` when successful
- `TurnSkipped` if the disconnected player's own turn is skipped
- `PlayerForfeited` if the 60-second reconnect window expires
- `MatchCompleted` if the forfeit resolves the match, plus `RoomCompleted` if the room reaches terminal status

## 3. Stale Commands and Replayed Commands

### Expected domain behavior

- A command with an old sequence number is rejected as stale.
- A command with a previously seen idempotency key is recognized as a duplicate.
- If the original command succeeded, the duplicate should return the already-known result without reapplying state.
- If the original command failed validation, the duplicate should return the same failure classification if available.

### Emitted events

- no new domain event for stale rejection and no Game Integrity log entry
- no duplicate event for replayed commands
- structured operational/security audit record for each rejected command, including `commandId`, `correlationId`, session/player, room/tournament when known, rejection reason (for example duplicate recognition without reapplying state), submitted/current sequence when applicable, and timestamp; duplicate classification is a field/reason on that record only, never a domain or Game Integrity event

## 4. Partial Failures Between Contexts

### Example
Room Gameplay commits an eligible casual `GameCompleted`, but Ranking is temporarily unavailable.

### Expected domain behavior

- Room Gameplay remains authoritative and does not roll back the completed game.
- The completion event is durably retried until Ranking consumes it.
- Spectator View and Analytics may process the same result earlier than Ranking. Tournament advancement is a separate path from `MatchCompleted`.
- Eventual consistency is accepted; cross-context rollback is avoided.

### Emitted events

- initial business event: `GameCompleted`
- later downstream business event: `PlayerRatingUpdated`
- retry attempts themselves are infrastructure concerns, not new business events

## 4.1 Saga Fallback and Dead-Lettered Work

### Example
A tournament round kickoff emits provisioning batches, but one shard repeatedly fails to create all assigned rooms.

### Expected domain behavior

- Retryable worker failures are retried with the same deterministic slot identities.
- Duplicate room creation is ignored by `(tournamentId, roundNumber, slotId)`.
- If the batch retry budget is exhausted or conflicting assignments are detected, Tournament Orchestration records a business-visible quarantine state rather than silently losing the batch.
- Quarantined batches block round start or completion until reconciled by an explicit operator or recovery command.
- Pure broker redelivery and transient consumer lag remain operational signals; they become domain events only when the saga state changes.

### Emitted events

- `TournamentProvisioningBatchRetried` when retry is recorded as part of saga state
- `TournamentProvisioningBatchQuarantined` when retry budget is exhausted or assignments conflict
- `TournamentResultQuarantined` when a consumed room result conflicts with the assigned slot or previous completion
- `RoomStateReconciled` when Room Gameplay repairs operational state from the Game Integrity log after append/local-commit divergence

## 5. Security and Abuse Scenarios

### Session takeover

**Behavior**

- Commands from revoked or displaced sessions are rejected synchronously by Identity and Session.
- If suspicious takeover is confirmed, the old session is invalidated and active room participation may be disconnected separately.

**Events**

- `SessionInvalidated`
- `PlayerDisconnected` if Room Gameplay terminates active room participation

### Logout outcome is indeterminate

**Behavior**

- `POST /v1/auth/logout` is authoritative and idempotent: active sessions invalidate through the existing Identity outbox path, while already-invalid, expired, or unknown tokens succeed without revealing token existence.
- The CLI removes its local session file only after authoritative success. Network errors, timeouts, or backend `5xx` return nonzero and retain the file for retry; absence of a local session remains local idempotent success.

### Spam and flooding

**Behavior**

- Repeated invalid commands do not mutate domain state.
- Domain rate-limits or abuse policies may temporarily mute or remove the player from the room or tournament.

**Events**

- `PlayerMutedForAbuse` if the policy is domain-visible
- `PlayerRemovedFromRoomForAbuse` if moderation escalates
- otherwise no domain event for each rejected spam request; each rejection still emits a structured operational/security audit record and never appends a Game Integrity entry

### Sequence number spoofing and replay

**Behavior**

- Sequence numbers are server-issued room versions, not client authority.
- A forged future sequence number or replayed old sequence number is rejected before domain mutation.
- The rejected command appends no game-log entry, emits no domain event, and cannot alter turn order, hands, or penalties.

**Events**

- no domain event and no Game Integrity log entry for the rejected command
- structured operational/security audit record with `commandId`, `correlationId`, session/player, room/tournament when known, rejection reason (for example spoofing or suspicious-command patterns), submitted/current sequence, and timestamp; suspicious classification is a field/reason on that record only, never a domain or Game Integrity event

## 6. Spectator Privacy Violations

### Example
A player attempts to subscribe through the spectator channel to infer another player's hand.

### Expected domain behavior

- Spectators may establish a new spectator connection while the room is `waiting`, `locked`, or `in_progress`, subject to public/private authorization.
- Admission is denied after `RoomCompleted` or `RoomCancelled`; existing spectator streams close at that terminal room/match state. Terminal means the complete match/room, not the end of an individual game in a best-of-three match.
- Spectator View publishes only filtered projections.
- Private hand events never cross the Spectator View boundary.
- A reconnecting player receives only their own private hand plus public room state, never opponent hands through any channel.
- The immutable GameLog may contain full private state for audit, but Spectator View cannot query the raw log.
- If an unsafe event enters the projector, it is dropped and flagged internally.
- A player opening a second anonymous spectator connection still receives only the spectator-safe projection unless they are separately authorized as a room participant on the player channel.

### Emitted events

- `SpectatorEventDropped` when filtering blocks an unsafe update
- `SpectatorAccessDenied` when the channel, token, or room terminal state denies admission
- stream-close control signal when the room/match reaches `RoomCompleted` or `RoomCancelled`
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

- In an ad-hoc room before lock/start, if the host leaves and at least one player remains, host ownership transfers deterministically to the remaining player in the lowest occupied seat.
- If nobody remains, the room cancels immediately; there is no host-timeout wait.
- After lock/start, host reassignment has no gameplay authority; lock, start, and in-match decisions follow room and tournament policy rather than host privilege.

### Emitted events

- `PlayerLeftRoom`
- `HostReassigned` when a remaining player becomes host before lock/start
- `RoomCancelled` immediately if the room becomes empty before lock/start

## 8.1 Uno Window Display Under Latency

### Expected domain behavior

- When an Uno window opens, Room Gameplay publishes absolute UTC `expiresAt` and the room sequence at which the window opened.
- Client countdown or display, including the CLI, is advisory only.
- The server exclusively decides whether `CallUno`, `ReportMissingUno`, or `ExpireUnoWindow` is timely.
- A successful `CallUno` closes and resolves the challenge window; a later `ReportMissingUno` is rejected as inactive and never applies a challenger draw penalty.
- SSE updates and command results correct any stale client display.

### Emitted events

- accepted call/challenge/expiry path: `UnoCalled`, `UnoChallengeIssued`/`UnoPenaltyApplied` (`cardsDrawn=2` on a valid challenge), or `UnoWindowExpired` as applicable
- late, post-`CallUno`, or otherwise rejected call/challenge: no domain event, no challenger penalty, and no Game Integrity entry; structured operational/security audit record only

## 9. Integrity Replay Reveals Corruption

### Expected domain behavior

- If replayed state does not match the committed room result, the affected match is frozen for investigation.
- Ranking and tournament advancement must stop processing further consequences from that disputed result until resolved.

### Emitted events

- `IntegrityMismatchDetected`
- `MatchFrozenForInvestigation`

This case is rare, but it is important because cash-prize or high-trust tournaments need a clear business response when auditability fails.
