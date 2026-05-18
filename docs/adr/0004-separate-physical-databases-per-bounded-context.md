# Separate Physical Databases per Bounded Context

## Status
Accepted

## Context
Each bounded context has different consistency, ownership, and storage needs, so a single shared database would blur boundaries.

## Decision
Physical databases are separated by bounded context: Room Gameplay uses Postgres snapshots, Tournament uses Postgres, Identity uses Postgres, Ranking uses Postgres, Game Integrity uses EventStoreDB, Analytics uses ClickHouse, and Redis is reserved only for specific acceleration or projection needs.

## Consequences
Each context owns its persistence boundary and operational profile. Cross-context coupling through shared tables is avoided. Redis remains an auxiliary store, not a general-purpose system of record.
