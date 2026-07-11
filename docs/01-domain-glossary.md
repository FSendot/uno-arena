# Domain Glossary

This glossary defines the ubiquitous language used across the rest of the submission. The terms below are intentionally precise so that `game`, `match`, `round`, and `tournament` are never conflated.

## Core Competition Terms

- **Player**: authenticated participant allowed to join rooms, play games, and register for tournaments.
- **Spectator**: read-only observer of a room or tournament who receives filtered updates but cannot submit gameplay commands or see private hands, hidden deck order, or player-only decisions.
- **Room**: the bounded play space where a roster of players and spectators connect. A room hosts exactly one match.
- **Game**: one single Uno game, starting from the initial deal and ending when one player empties their hand or another terminating rule applies.
- **Match**: a best-of-three series played inside one room. A match contains up to three games, can end early once a player wins two games, and produces an authoritative ranked result by match wins for advancement.
- **Round**: one elimination tier of a tournament. A round contains many matches running in parallel and closes only after every assigned room reports an authoritative terminal result.
- **Tournament**: the full elimination competition composed of sequential rounds until 10 or fewer players remain for a final room.

## Room Gameplay Terms

- **Seat**: a player's assigned position inside a room roster.
- **Host**: the player with authority to configure or lock an ad-hoc room before the match starts.
- **Roster**: the set of seats assigned to a room.
- **Turn**: the current right to act.
- **Turn Order**: the ordered sequence of seats and the current direction of play.
- **Sequence Number**: monotonic room version attached to commands so stale or replayed actions can be rejected.
- **Active Color**: the current effective color on the discard pile, including a color chosen after a wild card.
- **Penalty Stack**: the accumulated draw penalty created by consecutively stacked `Draw Two` and `Wild Draw Four` cards. The player targeted by the current penalty may add either draw-card type, subject to ordinary card-legality rules, and transfer the increased penalty to the next player. The first targeted player who does not stack must draw the full accumulated penalty and forfeits the rest of their turn.
- **Jump-In**: an out-of-turn play of a card that exactly matches the current discard by both color and rank or action symbol. An accepted jump-in makes the jumper the acting player, resolves the card normally, and continues turn order from the seat after the jumper. Jump-ins cannot bypass an unresolved penalty stack, a pending wild-color choice, or any other mandatory resolution; only the player targeted by a penalty may stack a draw card.
- **Hand**: private set of cards held by one player in a game; only that player may see and play those cards.
- **Uno Window**: the rule window in which a player who reached one remaining card must successfully call Uno or be penalized; it closes after 5 seconds or as soon as the next player begins their turn, whichever comes first.
- **Challenge Window**: the period in which an opponent may challenge a missing Uno call; a successful challenge makes the target draw 2 cards, while an invalid challenge makes the challenger draw 2 cards.
- **Game Scoreboard**: the match-level score tracking how many games each player has won in the current best-of-three match.

## Integrity and Audit Terms

- **Deck Seed**: immutable seed used to derive the authoritative shuffle for a game.
- **Authoritative Deck**: the only valid source of dealt and drawn cards for a game.
- **Game Log Entry**: immutable record of one committed state transition affecting room gameplay.
- **Replay Position**: pointer into the ordered game log used to rebuild state.
- **Idempotency Key**: stable command identifier used to detect duplicated submissions.

## Tournament Terms

- **Bracket Slot**: a participant position in a round, later filled by a registered player or an advancing winner.
- **Advancement Rule**: rule that determines the top 3 advancing players from a non-final match by match wins, then by lowest cumulative card points, then by earliest final-game completion time.
- **Forfeit**: loss assigned after the fixed 60-second reconnection window expires. In a casual room it ends the player's participation and the game may continue; in a tournament room it counts as a match loss and eliminates the player.
- **Round Result**: the authoritative set of completed match outcomes used to decide round completion and next-round seeding.

## Ranking Terms

- **Elo Rating**: a player's casual-game skill rating, updated only from completed non-abandoned ad-hoc games.
- **Tournament Placement Rating**: separate tournament score derived from tournament advancement depth and final placement, never from casual Elo updates.
- **Rating Delta**: the calculated Elo increase or decrease produced by one completed casual game using final placement order from first through last.
- **Player Performance Snapshot**: immutable summary of a player's standing after a rating update.

## Explicit Distinctions

- A **game** is one Uno game.
- A **match** is the best-of-three series inside a room.
- A **round** is one tournament elimination tier composed of many matches.
- A **tournament** is the whole multi-round competition.

Because of this distinction:

- `GameCompleted` means one Uno game ended and may update casual Elo if the game is ad-hoc and non-abandoned.
- `MatchCompleted` means the best-of-three series ended and the room can transition toward completion or report tournament advancement facts.
- `TournamentRoundCompleted` means every match assigned to that round has a final result.
- `TournamentCompleted` means the final round produced the champion.
