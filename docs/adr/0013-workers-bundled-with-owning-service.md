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
| Tournament Completion Workers | Tournament Orchestration Service |
| Projection Rebuilders | Spectator Projection Service / Analytics Service |

## Consequences
Workers are versioned with their owning service chart, but **default/staging/production keep worker Deployments disabled** (`timerWorker.enabled` / `provisioningWorker.enabled` / `seedingWorker.enabled` / `completionWorker.enabled` / `projectionRebuilder.enabled` = `false`). The Room timer worker owns both leased deadline dispatch and the Postgres-authoritative best-of-three continuation queue; a non-terminal game completion creates the deterministic continuation atomically and worker lease expiry recovers crashes. Kind enables Tournament provisioning, all-round seeding, and completion workers (2 replicas each) to exercise `FOR UPDATE SKIP LOCKED`. Registration close (explicit or capacity-triggered) atomically creates round 1; seeding finalization schedules provisioning; non-final round completion creates the next seeding job; final completion marks the tournament completed and publishes `TournamentCompleted`. Spectator/Analytics projection rebuilders implement ADR-0039 rebuild-request consumers and context-owned snapshot/backfill callers; their live recovery lanes passed, so kind now enables them while default/staging/production remain disabled. Re-enable workers outside kind only after environment-specific operational readiness. If a worker's operational lifecycle later diverges from its owning service (e.g., independent scaling), extract it into its own pipeline service at that point.
