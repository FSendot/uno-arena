# Separate Physical Databases per Bounded Context

## Status
Accepted

## Context
Each bounded context has different consistency, ownership, and storage needs, so a single shared database would blur boundaries.

## Decision
Physical databases are separated by bounded context: Room Gameplay uses one Postgres database, Tournament uses one Postgres database, Identity uses one Postgres database, Ranking uses one Postgres database, Game Integrity uses KurrentDB, Analytics uses ClickHouse, and Redis is reserved only for specific acceleration or projection needs. Under ADR-0033, primary/standby replicas form one HA copy set of the same context database and do not create additional shards. Native table partitioning inside a context-owned database is allowed, but application-managed routing across multiple physical databases is outside this architecture.

## Consequences
Each context owns its persistence boundary and operational profile. Cross-context coupling through shared tables is avoided. Redis remains an auxiliary store, not a general-purpose system of record.
