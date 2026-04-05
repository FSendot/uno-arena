# Domain Glossary

This glossary defines the ubiquitous language used across the rest of the submission. The terms below are intentionally precise so that `game`, `match`, `round`, and `tournament` are never conflated.

## Core Competition Terms

- **Player**: authenticated participant allowed to join rooms, play games, and register for tournaments.
- **Spectator**: read-only observer of a room or tournament who receives filtered updates but cannot submit gameplay commands.
- **Room**: the bounded play space where a roster of players and spectators connect. A room hosts exactly one match.
- **Game**: one single Uno game, starting from the initial deal and ending when one player empties their hand or another terminating rule applies.
- **Match**: a best-of-three series played inside one room. A match contains up to three games and ends when one player or team wins two games.
- **Round**: one elimination tier of a tournament. A round contains many matches running in parallel and advances winners to the next tier.
- **Tournament**: the full elimination competition composed of sequential rounds until one champion remains.

## Room Gameplay Terms

- **Seat**: a player's assigned position inside a room roster.
- **Host**: the player with authority to configure or lock an ad-hoc room before the match starts.
- **Roster**: the set of seats assigned to a room.
- **Turn**: the current right to act.
- **Turn Order**: the ordered sequence of seats and the current direction of play.
- **Sequence Number**: monotonic room version attached to commands so stale or replayed actions can be rejected.
- **Active Color**: the current effective color on the discard pile, including a color chosen after a wild card.
- **Penalty Stack**: the pending draw penalty that must be resolved before ordinary play resumes.
- **Uno Window**: the rule window in which a player who reached one remaining card must successfully call Uno or be penalized.
- **Game Scoreboard**: the match-level score tracking how many games each player has won in the current best-of-three match.

## Integrity and Audit Terms

- **Deck Seed**: immutable seed used to derive the authoritative shuffle for a game.
- **Authoritative Deck**: the only valid source of dealt and drawn cards for a game.
- **Game Log Entry**: immutable record of one committed state transition affecting room gameplay.
- **Replay Position**: pointer into the ordered game log used to rebuild state.
- **Idempotency Key**: stable command identifier used to detect duplicated submissions.

## Tournament Terms

- **Bracket Slot**: a participant position in a round, later filled by a registered player or an advancing winner.
- **Advancement Rule**: rule that determines which player advances from a match to the next round.
- **Forfeit**: loss assigned because a player failed to appear, reconnect, or remain eligible within the allowed policy window.
- **Round Result**: the authoritative set of completed match outcomes used to decide round completion and next-round seeding.

## Ranking Terms

- **Elo Rating**: a player's competitive rating used for persistent ranking across matches.
- **Rating Delta**: the calculated Elo increase or decrease produced by one completed rated match.
- **Player Performance Snapshot**: immutable summary of a player's standing after a rating update.

## Explicit Distinctions

- A **game** is one Uno game.
- A **match** is the best-of-three series inside a room.
- A **round** is one tournament elimination tier composed of many matches.
- A **tournament** is the whole multi-round competition.

Because of this distinction:

- `GameCompleted` means one Uno game ended, but the room may continue if the match score is not yet decisive.
- `MatchCompleted` means the best-of-three series ended and the room can transition toward completion.
- `TournamentRoundCompleted` means every match assigned to that round has a final result.
- `TournamentCompleted` means the final round produced the champion.
