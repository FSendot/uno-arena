# Domain Glossary

This glossary defines the ubiquitous language used across the rest of the submission. The terms below are intentionally precise so that `game`, `match`, `round`, and `tournament` are never conflated.

## Core Competition Terms

- **Player**: authenticated participant allowed to join rooms, play games, and register for tournaments.
- **Spectator**: read-only observer of a room or tournament who receives filtered updates but cannot submit gameplay commands or see private hands, hidden deck order, or player-only decisions. A new spectator connection may be established while the room is `waiting`, `locked`, or `in_progress`, subject to public/private authorization; admission is denied after `RoomCompleted` or `RoomCancelled`, and existing spectator streams close at that terminal room/match state (not at the end of an individual game inside a best-of-three match).
- **Room**: the bounded play space where a roster of players and spectators connect. A room hosts exactly one match.
- **Game**: one single Uno game, starting from the initial deal and ending when one player empties their hand or another terminating rule applies.
- **Match**: a best-of-three series played inside one room. A match contains up to three games, can end early once a player wins two games, and produces an authoritative ranked result by match wins for advancement.
- **Round**: one elimination tier of a tournament. A round contains many matches running in parallel and closes only after every assigned room reports an authoritative terminal result.
- **Tournament**: the full elimination competition composed of sequential rounds until 10 or fewer players remain for a final room.

## Room Gameplay Terms

- **Seat**: a player's assigned position inside a room roster.
- **Host**: the player with authority to configure or lock an ad-hoc room before the match starts. If the host leaves before lock/start, the remaining player in the lowest occupied seat becomes host deterministically; if nobody remains, the room cancels immediately. After lock/start, host reassignment has no gameplay authority.
- **Roster**: the set of seats assigned to a room.
- **Turn**: the current right to act.
- **Turn Order**: the ordered sequence of seats and the current direction of play.
- **Sequence Number**: monotonic room version attached to commands so stale or replayed actions can be rejected.
- **Active Color**: the current effective color on the discard pile, including a color chosen after a wild card.
- **Penalty Stack**: the accumulated draw penalty created by consecutively stacked `Draw Two` and `Wild Draw Four` cards. The player targeted by the current penalty may add either draw-card type, subject to ordinary card-legality rules, and transfer the increased penalty to the next player. The first targeted player who does not stack must draw the full accumulated penalty and forfeits the rest of their turn.
- **Jump-In**: an out-of-turn play of a card that exactly matches the current discard by both color and rank or action symbol. An accepted jump-in makes the jumper the acting player, resolves the card normally, and continues turn order from the seat after the jumper. Jump-ins cannot bypass an unresolved penalty stack, a pending wild-color choice, or any other mandatory resolution; only the player targeted by a penalty may stack a draw card.
- **Hand**: private set of cards held by one player in a game; only that player may see and play those cards.
- **Uno Window**: the rule window in which a player who reached one remaining card must successfully call Uno or be penalized; it closes after 5 seconds or as soon as the next player begins their turn, whichever comes first. When opened, the room publishes an absolute UTC `expiresAt` and the room sequence at which the window opened. Client countdown or display is advisory; the server exclusively decides timeliness, and SSE/command results correct clients.
- **Challenge Window**: the period in which an opponent may challenge a missing Uno call; a successful challenge makes the target draw 2 cards. A successful `CallUno` closes and resolves the window; a later `ReportMissingUno` is rejected as inactive with no challenger penalty and no domain facts.
- **Game Scoreboard**: the match-level score tracking how many games each player has won in the current best-of-three match.

## Integrity and Audit Terms

- **Deck Seed**: immutable seed used to derive the authoritative shuffle for a game.
- **Authoritative Deck**: the only valid source of dealt and drawn cards for a game.
- **Game Log Entry**: immutable record of one committed state transition affecting room gameplay. Rejected commands never produce Game Log entries.
- **Command Rejection Audit Record**: structured operational/security audit record emitted when a command is rejected. It includes `commandId`, `correlationId`, session/player identity, room/tournament identity when known, rejection reason, submitted and current sequence numbers when applicable, and timestamp. It is not a domain event and is not written to the Game Integrity log.
- **Replay Position**: pointer into the ordered game log used to rebuild state.
- **Idempotency Key**: stable command identifier used to detect duplicated submissions.

## Tournament Terms

- **Bracket Slot**: a participant position in a round, later filled by a registered player or an advancing winner.
- **Bracket Page**: a bounded public slice of bracket slots accompanied by compact tournament and round summary metadata. It contains 100 slots by default and never more than 1,000; continuation uses an opaque Tournament-owned live keyset cursor over stable round/slot identity.
- **Advancement Rule**: rule that determines up to 3 unique advancing players from a non-final match by match wins, then by lowest cumulative card points, then by earliest final-game completion time. An authoritative undersized/forfeit result may advance only 1 or 2 players; an empty advancement is invalid.
- **Forfeit**: loss assigned after the fixed 60-second reconnection window expires. In a casual room it ends the player's participation and the game may continue; in a tournament room it counts as a match loss and eliminates the player.
- **Round Result**: the authoritative set of completed match outcomes used to decide round completion and next-round seeding.

## Ranking Terms

- **Elo Rating**: a player's casual-game skill rating, updated only from completed non-abandoned ad-hoc games.
- **Tournament Placement Rating**: lifetime, monotonically non-decreasing achievement score accumulated from tournament advancement depth and final placement; it never behaves like or mutates casual Elo.
- **Tournament Performance Fact**: authoritative Tournament Orchestration fact consumed by Ranking; it carries either an achieved advancement depth or an ordered final placement, never a producer-calculated rating delta.
- **Rating Delta**: the calculated Elo increase or decrease produced by one completed casual game using final placement order from first through last.
- **Player Performance Snapshot**: immutable summary of a player's standing after a rating update.
- **Leaderboard Page**: a bounded ordered slice of a complete leaderboard. It contains 100 entries by default and never more than 500; continuation uses an opaque Ranking-owned live keyset cursor, with page-local rather than frozen multi-page consistency.
- **Leaderboard Snapshot**: a bounded published top-100 public view of one leaderboard at a point in time. A dirty board publishes at most once every 15 seconds; it is not the complete leaderboard, which remains available through Leaderboard Pages.

## Explicit Distinctions

- A **game** is one Uno game.
- A **match** is the best-of-three series inside a room.
- A **round** is one tournament elimination tier composed of many matches.
- A **tournament** is the whole multi-round competition.

Because of this distinction:

- `GameCompleted` means one Uno game ended and may update casual Elo if the game is ad-hoc and non-abandoned.
- `MatchCompleted` means the best-of-three series ended and the room can transition toward completion or report tournament advancement facts.
- `TournamentRoundCompleted` means every match assigned to that round has a final result.
- `TournamentCompleted` means the final round produced complete ordered standings; the first standing is the champion.
