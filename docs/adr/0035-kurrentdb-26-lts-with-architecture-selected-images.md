# KurrentDB 26.0.3 LTS with Architecture-Selected Images

## Status
Accepted

## Context
ADR-0002 selected EventStoreDB for Game Integrity, but the pinned `eventstore/eventstore:24.10.14` image is Linux AMD64-only while development and deployment may use either AMD64 or ARM64 nodes. Omitting the store and using memory would prevent the local cluster from proving the durable Game Integrity path.

EventStoreDB was renamed KurrentDB beginning with version 25. The current 26.0 line is LTS and retains the gRPC stream API, event identifiers, immutable append semantics, reads/subscriptions, and expected-revision concurrency required by Game Integrity.

## Decision
Use KurrentDB 26.0.3 LTS as the Game Integrity event store and use the canonical `KurrentDB` name in current architecture and configuration.

- Linux AMD64 image: `docker.kurrent.io/kurrent-lts/kurrentdb:26.0.3@sha256:b4d0665a78269cd7184971c4d1fad38265277901f3d3730d89dcfba8f3d37fe9`.
- Linux ARM64 image: `kurrentplatform/kurrentdb:26.0.3-experimental-arm64-10.0-noble@sha256:8498556a8ba7a74f8d4ea31a149b1e5216e167d6884b630a68b3e1eb9e6e870e`.
- Go adapter: official `github.com/kurrent-io/KurrentDB-Client-Go` v1.4.0 over gRPC.
- Canonical connection configuration: `KURRENTDB_URL` using the `kurrentdb://` scheme. No legacy alias is required because the durable adapter has not deployed.

KurrentDB does not publish these artifacts behind one suitable stable multi-architecture index, so deployment detects the Kubernetes node architecture and selects the matching reviewed digest before applying the workload. The experimental ARM64 build remains non-production even though it is the same 26.0.3 server version. Both paths must pass real append/read, event-ID idempotency, exact expected-revision conflict, encrypted-payload inspection, volume persistence, process restart, and readiness tests before the local cluster is accepted. Failure blocks Game Integrity readiness; local deployment never silently substitutes the memory repository.

The Game Integrity storage contract remains unchanged: one append-only stream per room/game, exact expected revisions, application-level envelope encryption, internal replay/audit access, and no use of KurrentDB as the cross-context integration bus. A first-append deadline, connection, internal-server, aborted, or unavailable response is reconciled only through an authenticated read of the committed deterministic event; the repository never blindly retries the uncertain append. Exact logical identity plus plaintext confirms success, a competing payload returns `conflicting_duplicate`, and unprovable state fails closed.

## Consequences
Local AMD64 and ARM64 development can exercise the same durable gRPC semantics without emulation. KurrentDB is an explicit exception to the generic multi-architecture-index policy: deployment renders an architecture-specific pinned digest from the reviewed map and fails on an unknown architecture. The ARM64 image's experimental status is an explicit local risk and cannot satisfy production support or HA claims. KurrentDB's current license and any enterprise-only features must be reviewed before production procurement; this project relies only on the basic stream/gRPC capabilities needed by Game Integrity.

## Verification sources

- KurrentDB release schedule: https://docs.kurrent.io/server/latest/release-schedule/
- Official image tags: https://hub.docker.com/r/kurrentplatform/kurrentdb/tags
- Official Go client: https://github.com/kurrent-io/KurrentDB-Client-Go
- Expected-revision append semantics: https://docs.kurrent.io/clients/golang/v1.1/appending-events
