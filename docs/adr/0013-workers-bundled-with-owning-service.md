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
| Tournament Seeding Workers | Tournament Orchestration Service |
| Projection Rebuilders | Spectator Projection Service / Analytics Service |

## Consequences
Workers are versioned with their owning service chart, but **default/staging/production keep worker Deployments disabled** (`timerWorker.enabled` / `provisioningWorker.enabled` / `seedingWorker.enabled` / `projectionRebuilder.enabled` = `false`). Kind enables provisioning and seeding workers (2 replicas) to exercise `FOR UPDATE SKIP LOCKED`. Spectator/Analytics projection rebuilders implement ADR-0039 rebuild-request consumers and context-owned snapshot/backfill callers, but stay disabled in default/staging/production/`kind` until real end-to-end worker live recovery tests pass. Re-enable other workers outside kind only after those loops and durable adapters exist. If a worker's operational lifecycle later diverges from its owning service (e.g., independent scaling), extract it into its own pipeline service at that point.
