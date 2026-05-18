# Projections Own Privacy Filtering and BFF Only Routes

## Status
Accepted

## Context
Privacy decisions must happen close to the data ownership boundary so the BFF does not become a policy engine for raw backend facts.

## Decision
Spectator View owns the spectator projection, and Room Gameplay owns the player feed and sanitized metrics. The BFF only routes requests and responses; it never filters raw backend facts.

## Consequences
Privacy filtering stays with the projection that understands the source data and audience. The BFF remains thin and non-authoritative. Player-private and spectator-safe views can evolve independently.
