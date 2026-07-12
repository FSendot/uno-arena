# Bound Leaderboard and Bracket Projections

## Status
Accepted

## Context
UnoArena targets up to one million users, but the previous public read contracts did not bound leaderboard or tournament-bracket responses, and a literal full leaderboard snapshot could grow with every player. Whole-aggregate responses, whole-bracket Redis values, and million-entry Kafka snapshots would make ordinary reads and publications scale linearly with the entire population.

## Decision
Public leaderboard reads use opaque cursor pagination with 100 entries by default and a hard maximum of 500. Ranking keeps the complete board as an incrementally maintained Redis sorted set, while a published `LeaderboardSnapshotPublished` event contains only the ordered public top 100. Public tournament bracket reads return compact tournament/round summary metadata plus an opaque-cursor page of bracket slots: 100 by default and at most 1,000, matching the maximum provisioning-batch bound. Tournament stores its Redis bracket projection in round/batch-sized chunks rather than one tournament-sized document. PostgreSQL remains authoritative for both contexts; Redis projections are disposable and rebuildable.

Ranking transactions mark a board dirty only when at least one score changes. A Ranking-owned snapshot worker coalesces changes and publishes at most one top-100 snapshot per board every 15 seconds. The worker uses durable board versions and Postgres claim/checkpoint locking so multiple replicas cannot publish conflicting snapshots; a zero-delta result does not mark the board dirty.

Public pagination uses live opaque keyset cursors rather than frozen whole-dataset generations. A leaderboard cursor represents the last logical `(rating, playerId)` ordering boundary; a bracket cursor represents stable `(roundNumber, slotIndex)` identity. Every page carries `projectionVersion` and `generatedAt`. Each response is internally consistent, but a leaderboard may change between page requests; version drift is visible and clients that need one visual point-in-time traversal may restart. A version change does not invalidate the cursor or produce `snapshot_required`. Exact audit/export snapshots are internal operations, not the public pagination contract. Cursor encodings remain owned by the producing context and must not expose Redis keys or database offsets.

Public tournament standings are a separate bounded compact projection: `phase`, `registeredCount`, `currentRound`, and ordered `finalStandings` (at most 10 player IDs from the recorded final-round `ranked_result`). They are not paginated and never embed the full registration set. Durable standings reads use Tournament-owned Postgres one-statement snapshots (registration shard SUM + projection checkpoint + final ranked_result) without whole-aggregate hydration.

## Consequences
No normal API response or Kafka leaderboard snapshot grows with all registered players or all tournament slots. Clients traverse pages explicitly, a projection implementation may change its physical cursor/key encoding without breaking callers, and Redis can update or rebuild bounded chunks. The complete leaderboard remains queryable page by page even though the broadcast snapshot is the top-100 public view. Change bursts produce at most two snapshot events per 15-second interval—one per board—while incremental Redis reads may become current sooner. Public traversal avoids million-entry immutable Redis generations; callers accept page-local consistency and can detect cross-page drift explicitly. Tournament standings remain O(1) relative to registration size.
