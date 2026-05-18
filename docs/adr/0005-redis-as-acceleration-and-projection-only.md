# Redis as Acceleration and Projection Only

## Status
Accepted

## Context
Some workloads need fast access, but they do not justify making Redis authoritative for the underlying business data.

## Decision
Redis is used only where justified as acceleration or projection storage: timers, rate limits, cache entries, spectator projections, tournament bracket projections, and ranking leaderboard caches. Redis is not authoritative for sessions or Room Gameplay.

## Consequences
Fast-path use cases can stay responsive without shifting ownership away from the durable stores. Rebuildability remains important because Redis data is disposable. Session and room authority continue to live outside Redis.
