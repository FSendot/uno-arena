# Sharded Tournament Provisioning

## Status
Accepted

## Context
Tournament match creation can fan out across many rooms and needs an ownership model that scales without making workers stateful coordinators.

## Decision
Tournament Orchestration owns the workers and the fanout. Workers create rooms by calling Room Gameplay through the idempotent room-creation path, passing `tournamentId`, `roundNumber`, and `slotId`.

## Consequences
Tournament provisioning stays partitionable and recoverable. Room creation remains idempotent, which makes retries safe. Orchestration owns distribution; Room Gameplay owns the actual room records.
