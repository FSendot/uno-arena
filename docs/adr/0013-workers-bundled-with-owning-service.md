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
Workers are versioned with their owning service chart, but **default/staging/production keep worker Deployments disabled** (`timerWorker.enabled` / `provisioningWorker.enabled` / `projectionRebuilder.enabled` = `false`). Current service binaries do not implement `WORKER_ROLE` loops, and production timer/Kafka/rebuild adapters are absent; enabling the templates today would launch duplicate HTTP servers rather than worker loops. Re-enable only after those loops and durable adapters exist. If a worker's operational lifecycle later diverges from its owning service (e.g., independent scaling), extract it into its own pipeline service at that point.
