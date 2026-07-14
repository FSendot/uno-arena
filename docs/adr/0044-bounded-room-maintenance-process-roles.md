# Bounded Room Maintenance Process Roles

## Status

Accepted

## Decision

Dedicated `room-runtime` processes execute only commands and active reads for their pinned Room; they never rebuild global indexes or scan global reconciliation state. The stable `room-router` owns authorization, routing, and stable reads and also runs no global maintenance.

The `room-timer` role owns Redis timer-index rebuild and lease reaping. A Room-context lease permits only one timer replica to perform a global rebuild at a time while ordinary partitioned timer claims remain horizontally scalable. The lease row carries a Postgres-owned completion generation: a worker snapshots it before claiming, completion increments it atomically, and contemporaneous siblings stop when they observe the advance. No maintenance fence compares application-host timestamps. A separate `room-integrity-reconciler` role claims append-confirmed/local-commit-failed repair work from Room Postgres with bounded batches and `FOR UPDATE SKIP LOCKED`; it does not repeat a full-table scan per runtime.

## Consequences

Global Room maintenance scales with explicitly configured worker replicas rather than active-room count. Every role remains packaged with and owned by Room Gameplay under ADR-0013, but runtime isolation no longer multiplies context-wide work by up to 100,000 pods. Database-owned generations keep completion fencing correct under pod or node clock skew; a worker that starts after a completed generation may initiate a fresh guarded rebuild.

## Rejected alternatives

- Starting global maintenance in every durable process causes rebuild and reconciliation work to grow with runtime pods.
- Running maintenance in the router couples request-serving scale to background database scans.
- Scoping reconciliation to one pinned Room leaves cross-Room repair throughput tied to active runtime placement and cannot repair terminal Rooms after teardown.
