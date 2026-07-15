# Dedicated State-Machine Pod per Room

## Status

Accepted

## Context

Room Gameplay must execute each active room as an independently schedulable state machine while supporting tournament bursts that can create more than 100,000 concurrent games. The Room bounded context still owns its aggregate, lifecycle, Postgres database, commands, timers, and integration facts. Kubernetes scheduling cannot become the source of truth, and creating a Service, Deployment, StatefulSet, or CRD per room would multiply control-plane objects beyond the required unit of isolation.

## Decision

Each `waiting`, `locked`, or `in_progress` Room has one lifecycle-bound bare Kubernetes Pod as its exclusive mutation executor.

- `CreateRoom` and tournament provisioning atomically persist the Room aggregate and a desired runtime assignment in Room Postgres. Kubernetes admission is asynchronous.
- When a tournament Room runtime first becomes Ready, the controller atomically records the `RoomRuntimeReady` integration fact in the Room outbox. Tournament Orchestration keeps the match internally provisioning and withholds its `roomId` from players until that fact is consumed; replacement generations do not revoke an assignment that was already visible.
- A stable Room router forwards existing-room commands and snapshots directly to the Ready pod IP. There is no per-room Service.
- Until the current assignment is Ready, the router returns `503 room_starting` with `Retry-After`. A full mutation queue returns `429 room_busy` before admission.
- Two or more Room-owned controller replicas claim bounded assignment batches with `FOR UPDATE SKIP LOCKED`, expiring leases, deterministic pod names, and a bounded reconcile cadence.
- Controller replica count, claim batch, Kubernetes-operation concurrency, database pool, and readiness timeout are explicit deployment admission budgets. The production starting envelope uses 32 replicas, 64 claims and 16 concurrent Kubernetes operations per replica at the bounded one-second cadence; it must be validated against API-server, scheduler, and node-provisioning capacity before a 100,000-room surge is accepted. Local `kind` uses two replicas, four claims, and two concurrent operations as topology evidence only.
- Pods use a safe hash and generation in Kubernetes labels. Raw `roomId` remains in Room Postgres and the pod environment, not Kubernetes labels.
- Every replacement advances a monotonically increasing generation. The dedicated pod verifies `(roomId, generation)` inside the same authoritative Room transaction used for the mutation, fencing stale pods before Game Integrity append or Room commit.
- Each pod has one active mutation worker and a bounded queue shared by player, timer, reconnect/forfeit, and automatic lifecycle commands. Read-only snapshots may use committed state concurrently.
- Dedicated pods perform no context-wide timer rebuild, lease reaping, or Game Integrity reconciliation scan. Those responsibilities remain in bounded Room-owned worker roles under ADR-0044.
- Router, controller, and runtime pods have separate service accounts. The router may observe pods, the controller may create/delete pods in the namespace, and runtime pods have no Kubernetes API token.
- The controller resolves only its context-owned database URL. Dedicated pod credentials are carried through the controller as Secret name/key references and are resolved by kubelet in the runtime pod; API, timer, recovery, and projection credentials are not granted to dedicated runtimes.
- Direct pod-IP traffic carries a scoped runtime credential and remains inside the service-mesh mTLS boundary.
- The stable Room boundary authenticates the original route-specific caller before pod lookup and credential upgrade. A runtime accepts only the authorized router hop for its pinned `roomId` and generation; raw caller identity fields never grant player-private access by themselves.
- Terminal Room state is committed before teardown. Final reads remain available from Postgres/projections, and SSE closure follows the committed terminal event.
- Every durable Kubernetes environment, including local `kind`, uses this topology. Offline capability-mode and fake-BFF checks may collapse Room into one process only for semantic tests and are not topology or deployment evidence.

## Consequences

Room failures are isolated to one active game, and replacements reconstruct from authoritative context-owned state. The router and controllers remain horizontally scalable, while the Kubernetes control plane handles one bare Pod per active room rather than several objects per room. Tournament assignment visibility joins asynchronously on Room readiness instead of polling or blocking provisioning. Clients must tolerate explicit startup, replacement, and overload responses, and operators must capacity-plan pod admission, database connection multiplexing, and controller API budgets for tournament bursts.

## Rejected alternatives

- One shared Room Deployment executing every room: does not provide the required room-state-pod isolation.
- One Deployment, StatefulSet, Service, or CRD per room: multiplies control-plane objects without adding authority beyond Room Postgres.
- Kubernetes object state as room authority: violates bounded-context persistence ownership and makes recovery dependent on cluster object survival.
