# Local kind foundation (Slice 0)

Disposable single-node `kind` cluster plus context-owned datastore bootstrap.
Not production. No HA, backup, PVC, or durability claim.

## Layout

- `cluster.yaml` — kind cluster + documented ingress host-port mappings (no ingress installed here)
- `manifests/` — namespaces, local-only Secrets, Postgres×4, Kafka RF1, Redis
  (local AOF under disposable emptyDir; not authoritative), KurrentDB ARM digest,
  ClickHouse, Keycloak start-dev
- `generated/` — Kafka topics Job/plan rendered from `contracts/asyncapi/kafka-v1.yaml`
  (kind-short retention.ms, DLQ scaffolding, Connect compacted internals)
- `scripts/` — render / validate / create / build-load / apply / wait / reset
- `../bootstrap/` — repo-root-buildable image embedding `services/*/migrations` + generated fingerprints + CDC role/publication SQL

## Make targets

| Target | Effect |
|---|---|
| `make kind-render` | Offline AsyncAPI → Kafka topic artifacts |
| `make kind-validate` | Offline checks (no pull, no cluster) |
| `make kind-test-redis-aof-structure` | Offline Redis AOF manifest + acceptance-script checks |
| `make kind-test-redis-aof` | Explicit/networked: DB15 prefix survives Redis PID 1 restart |
| `make kind-create-cluster` | Explicit: create `kind` cluster `uno-arena` |
| `make kind-build-load-bootstrap` | Explicit: build + `kind load` bootstrap image |
| `make kind-apply` | Explicit: datastores → wait ready → bootstrap Jobs → wait complete |
| `make kind-wait` | Re-check Deployments + bootstrap Jobs |
| `make kind-reset` | Explicit: delete **only** cluster `uno-arena` (rejects name/context overrides) |

Create / apply / reset are never implied by validate.

## Apply ordering

`kind-apply` applies namespace/secrets/datastores first, waits for Postgres /
ClickHouse / Kafka / Kurrent / Redis / Keycloak readiness, then applies bootstrap
and topic Jobs and waits for completion so Job backoff is not spent on datastore
startup.

## Discovery

Foundation Services live in namespace `uno-arena` to match Helm FQDNs such as
`kafka.uno-arena.svc.cluster.local`. Context Postgres instances remain physically
separate Services/databases.

## Credentials

`uno-arena-local-credentials` holds obvious local-only placeholders, scoped per
context for admin/bootstrap/runtime/CDC. Never reuse in staging/production.
