# Tournament Performance Facts Feed Ranking

## Status
Accepted

## Context
Ranking owns tournament-placement rating, but `PlayersAdvanced` alone identifies only the players who advanced and the existing `TournamentCompleted` contract identifies at most the champion. Ranking needs authoritative advancement depth and complete final standings without querying Tournament Orchestration's database or accepting a producer-calculated rating delta.

## Decision
Tournament Orchestration publishes the facts and Ranking owns their interpretation. `tournament.players.advanced` remains the intermediate advancement fact: `roundNumber` is the achieved advancement depth for every listed `advancingPlayerId`. `tournament.completed` carries an ordered `finalStandings` list containing every final-room player from first to last. The first entry is the champion; there is no separate `championId` field that could contradict the ordering. Ranking consumes both topics, converts each affected player into a tournament-performance update, and uses the upstream event `eventId` as that player's `placementEventId`; durable idempotency remains `(playerId, tournamentId, placementEventId)`.

`PlayersAdvanced.advancingPlayerIds` contains one to three unique players. Three is the ordinary top-three advancement; one or two are valid only when an authoritative undersized/forfeit outcome leaves fewer eligible players. Empty, duplicate, or more-than-three advancement lists are contract failures.

The published facts never contain a rating delta. Ranking receives either advancement depth or final placement and applies its own rating policy. The numerical weighting of advancement depth versus final placement is a separate Ranking-owned decision and is not selected by this ADR.

Ranking applies each upstream fact atomically across every affected player. A `PlayersAdvanced` event updates all listed advancing players or none; a `TournamentCompleted` event updates every final standing or none. The transaction locks player rows in stable player-ID order and commits rating changes, per-player history, per-player idempotency records, stable event outcome, and all resulting outbox rows together. Any malformed player, conflicting duplicate, or failed write rejects/quarantines the whole event without partial ratings or publication.

The transaction also persists the AsyncAPI source-event business key and an immutable payload fingerprint before Kafka offset commit. `PlayersAdvanced` uses `(tournamentId, roundNumber, sourceSlotId)`; `TournamentCompleted` uses `eventId`. An exact source duplicate returns its stable prior outcome without repeating any per-player award. Reusing the business key with different players, standings, or other immutable fields is an event-local conflict sent unchanged to the applicable Ranking DLQ.

Failure isolation is event-local, not tournament-wide. These awards are non-negative additions with independent idempotency keys, so they are commutative: a later valid fact may apply while one failed fact waits in its consumer-owned DLQ, and replaying the repaired fact later produces the same totals. Ranking therefore does not quarantine the tournament aggregate or block later facts solely because an earlier performance event failed. The failed event remains uncommitted until its unchanged envelope is acknowledged by the appropriate Ranking DLQ under ADR-0017.

## Consequences
Tournament Orchestration remains authoritative for bracket advancement and final ordering, while Ranking remains authoritative for rating rules and history. Ranking must consume both tournament topics, and `TournamentCompleted` cannot be published without complete ordered final standings. Replaying either upstream event is safe per player, and changing the rating formula does not require changing Tournament Orchestration's published language. Stable lock ordering bounds deadlock risk, and the maximum transaction fanout remains three ordinary advancing players or ten final standings. Event-local recovery prevents one malformed match fact from stopping independent tournament achievements while preserving exact replay.
