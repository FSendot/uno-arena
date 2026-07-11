# Room Commands Use Pessimistic Row Locking

## Status
Accepted

## Context
Room commands can arrive concurrently from the turn owner, jump-in players, reconnecting clients, and timer workers. The command must validate against one Room version and receive Game Integrity confirmation before the operational snapshot and outboxes commit. An optimistic Postgres compare-and-swap after the integrity append would allow multiple commands to reach Game Integrity from the same apparent room state and would turn ordinary contention into integrity-log reconciliation.

## Decision
For every mutation of an existing Room, use Room Gameplay's context-owned Postgres database, begin a `READ COMMITTED` transaction, and load the Room root with `SELECT ... FOR UPDATE`. While retaining that row lock, validate command idempotency, `expectedSequenceNumber`, session/membership, timers, and game rules; reserve or append through Game Integrity; then commit the snapshot, deadlines, command outcome, integration outbox, realtime outbox, and integrity log offset together.

Lock wait, statement, Game Integrity call, and overall transaction deadlines are bounded below the external request deadline. A stale or duplicate command is resolved before a new integrity append. Game Integrity failure rolls back the Postgres transaction. If Game Integrity confirms but the Postgres commit fails, the existing integrity-log reconciliation path repairs the Room from the confirmed log offset. Room creation, which has no existing row to lock, relies on its deterministic identity/idempotency key and unique insert constraints.

## Consequences
Competing actions for one Room serialize at the authoritative aggregate row, while unrelated Room rows and native table partitions continue independently inside the same context database. The design intentionally holds one Room lock across a synchronous internal call, so Game Integrity latency directly affects that Room's queue and requires strict timeouts and contention metrics. Repository code must use a consistent lock order for Room-owned rows.
