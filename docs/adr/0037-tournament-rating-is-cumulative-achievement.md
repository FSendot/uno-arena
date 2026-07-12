# Tournament Rating Is Cumulative Achievement

## Status
Accepted

## Context
The tournament-placement stream could behave either like a second competitive Elo rating that rises and falls or like a durable achievement score accumulated from tournament progress. The domain already separates casual Elo from tournament placement, describes the latter as an achievement score, initializes it at zero, and gives it a zero floor.

## Decision
Tournament Placement Rating is a monotonically non-decreasing cumulative achievement score. Ranking awards non-negative points for authoritative advancement-depth and final-standing facts from ADR-0036. Each accepted `PlayersAdvanced` fact awards **10 points** to every listed player; `roundNumber` is retained as the achieved depth and audit fact but never multiplies the increment. Depth is represented linearly because a player receives one award for each round advanced. `TournamentCompleted` awards an additional deliberately top-heavy final-placement bonus: **100, 70, 50, 35, 25, 20, 15, 10, 5, and 0 points** for places first through tenth respectively. Finals with fewer than ten players use only the applicable leading positions. Advancement already rewards reaching the final room, while the completion bonus distinguishes winning and podium finishes. A tournament result may add zero or more points but never subtract points, and the score never resets between tournaments. Casual Elo remains the only opponent-relative skill rating.

A zero-point final result is still an accepted tournament-performance fact. Ranking persists per-player idempotency and a history entry containing placement, reason, and equal previous/new ratings, then publishes the corresponding zero-delta `PlayerRatingUpdated` fact for downstream historical completeness. Because no score changed, that result alone does not trigger a leaderboard snapshot.

## Consequences
The tournament leaderboard measures accumulated tournament achievement rather than current skill or a zero-sum ranking. Repeated deep runs increase standing permanently, poor finishes cannot erase earlier achievements, and replay/idempotency must prevent duplicate awards. Fixed per-round awards keep growth linear instead of weighting later rounds twice through both repeated advancement and a round-number multiplier. A top-heavy final bonus makes final placement materially important without taking away the advancement points already earned by lower finishers. Ranking history records the awarded points, advancement depth or final placement, and resulting total for every accepted tournament-performance fact, including zero-point final results. Leaderboard publication remains change-driven rather than history-driven. Any future seasonal or decaying leaderboard must be a separate projection or policy with an explicit migration; it cannot silently reinterpret this lifetime score.
