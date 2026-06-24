# Infrastructure Workers Bundled with Their Owning Service

## Status
Accepted

## Context
The architecture names three worker types as separate containers: Room Timer Worker (owned by Room Gameplay), Tournament Provisioning Workers (owned by Tournament Orchestration), and Projection Rebuilders (owned by Spectator View and Analytics). The checkpoint rule is one service = one image = one Helm chart. However, workers that share a release lifecycle with their owning bounded context are a justified exception.

## Decision
Workers are bundled with their owning service's Helm chart as separate `Deployment` resources within the same chart, not as independent pipeline services. They do not have their own pipeline fragment, image, or coverage matrix row. The pipeline coverage matrix has 8 rows — one per bounded context service including the BFF — not 11.

| Worker | Owning service |
|---|---|
| Room Timer Worker | Room Gameplay Service |
| Tournament Provisioning Workers | Tournament Orchestration Service |
| Projection Rebuilders | Spectator Projection Service / Analytics Service |

## Consequences
Workers are deployed and versioned together with their owning service. This is architecturally correct: a Timer Worker without Room Gameplay is operationally meaningless, and deploying them independently would create partial-state failure modes with no benefit. If a worker's operational lifecycle diverges from its owning service in the future (e.g., it needs independent scaling or a separate release cadence), it should be extracted into its own pipeline service at that point.
