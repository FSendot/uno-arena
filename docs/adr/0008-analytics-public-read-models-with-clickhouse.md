# Analytics Public Read Models with ClickHouse

## Status
Accepted

## Context
The system needs public analytics and read models that can scale independently from transactional gameplay and ranking decisions.

## Decision
Analytics and public read models use ClickHouse. They consume sanitized events and public tournament metrics only. They remain derived and non-authoritative.

## Consequences
Analytics can serve public reporting without affecting game integrity, Elo, advancement, privacy, or audit authority. ClickHouse is a consumer of published facts, not a decision owner.
