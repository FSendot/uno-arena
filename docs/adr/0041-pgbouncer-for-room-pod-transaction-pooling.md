# PgBouncer for Room Pod Transaction Pooling

## Status

Accepted

## Context

Dedicated room pods cannot each carry an ordinary service-sized Postgres connection pool. At tournament scale, idle per-pod connections would exhaust Room Gameplay's single context-owned physical database before compute or gameplay becomes the bottleneck. Room mutations already serialize per room and use short explicit transactions.

## Decision

All dedicated room pods connect to Room Gameplay Postgres through a Room-owned PgBouncer Deployment in transaction-pooling mode.

- A room pod opens two independently bounded lazy client pools with `min=0` and `max=1`: one connection for the locked aggregate transaction and one for the autonomous crash-recovery intent that must commit before Game Integrity append. Both release idle connections after 30 seconds and are multiplexed through PgBouncer.
- Router and controller processes use separate, explicitly bounded pools.
- Transaction-scoped aggregate locks, runtime-generation fencing, idempotency, Game Integrity coordination, and authoritative Room/outbox commits remain unchanged.
- PgBouncer is an adapter inside the Room bounded context. It does not create another database, own domain state, or permit cross-context SQL access.
- Local `kind` deploys the same transaction-pooling topology with disposable storage and local-only credentials.
- Replica count, client admission, default backend pool, and reserve pool are environment-owned capacity values. Production begins with six replicas, 50,000 client slots per replica, and a 32+8 backend pool per replica for the one Room database/user pair; these are load-test inputs bounded by PostgreSQL `max_connections`, not proof of target throughput. Kind uses one replica, 100 client slots, and four backend connections.
- Staging and production use SCRAM-SHA-256 authentication plus application-layer TLS for runtime-to-PgBouncer and PgBouncer-to-Postgres connections. Their Helm render fails if TLS material, secure auth, or strict mesh enforcement is absent. Kind alone permits local plaintext database credentials and disabled application-layer database TLS inside the disposable cluster; it still installs Istio Ambient strict mTLS, so east-west transport never uses a plaintext mesh path.

## Consequences

Database sessions scale with concurrent Room transactions instead of ordinary service-sized pools per active pod. Each runtime can use at most two client connections, while PgBouncer multiplexes their short transactions over a bounded backend pool. Session-level Postgres features cannot be assumed across transactions, and all required locking/fencing must remain transaction-scoped. PgBouncer availability becomes part of Room readiness and operational capacity planning.

## Rejected alternatives

- One direct service-sized pool per room pod: exhausts Postgres connection capacity.
- One physical database per room: violates the one-physical-database-per-bounded-context boundary and is operationally infeasible.
- Application-side outbox polling to reduce database pressure: contradicts the CDC delivery decision and does not solve connection fan-out.
