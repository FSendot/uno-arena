# Redis Sorted Sets for Room Timer Scheduling

## Status
Accepted

## Context
Uno challenge and reconnect deadlines must be dispatched at high volume, while Room Gameplay's Postgres database remains authoritative and workers can crash or Redis can lose its indexes. Process-local timers and Redis keyspace notifications do not provide durable, claimable scheduling semantics.

## Decision
Use Redis sorted sets as disposable scheduling indexes, bucketed by a stable `roomId` hash and timer family. The score is the absolute UTC deadline in milliseconds. The member is a stable timer identity containing the timer type and the documented Room/game/player/version key.

A Lua script atomically claims bounded batches of due timers by moving them from the scheduled set into an in-flight sorted set whose score is a visibility deadline. Workers submit the existing internal timer command to Room Gameplay. The Room service routes and locks the aggregate, reloads the authoritative Postgres deadline, and revalidates identity, version, sequence, and current state before applying an expiry or forfeit. Successful or definitively stale commands acknowledge and remove the lease. A lease reaper returns expired claims for retry. Redis indexes are rebuilt from open Postgres deadline rows after loss or explicit repair.

## Consequences
Workers can scale independently per timer bucket without duplicate effects or making Redis authoritative. Delivery is at least once; stable timer keys plus Room revalidation make duplicate and stale claims harmless. Redis/Lua latency, scheduled/in-flight depth, oldest overdue deadline, claim/retry counts, lease age, and Postgres-to-Redis rebuild lag become production signals. A Redis outage delays dispatch but cannot invent or erase the authoritative deadline.
