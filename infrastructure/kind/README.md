# Local kind foundation (Slice 0)

Disposable single-node `kind` cluster plus context-owned datastore bootstrap.
Not production. No HA, backup, PVC, or durability claim.

## Layout

- `cluster.yaml` — kind cluster + documented ingress host-port mappings (no ingress installed here)
- `manifests/` — namespaces, local-only Secrets, Postgres×4, Kafka RF1, Redis
  (local AOF under disposable emptyDir; not authoritative), KurrentDB ARM digest,
  ClickHouse, Keycloak start-dev, Debezium Kafka Connect (four outbox routers;
  exact quay ARM64 digest via node-native crictl pull; imagePullPolicy Never), Debezium
  Server (Room realtime → Redis DB2; separate from Kafka Connect; same staging pattern)
- `generated/` — Kafka topics Job/plan rendered from `contracts/asyncapi/kafka-v1.yaml`
  (kind-short retention.ms, DLQ scaffolding, Connect compacted internals)
- `scripts/` — render / validate / create / build-load / load-debezium-connect /
  load-debezium-server / apply / wait / reset / connector status helpers
- `../bootstrap/` — repo-root-buildable image embedding `services/*/migrations` + generated fingerprints + CDC role/publication SQL

## Make targets

| Target | Effect |
|---|---|
| `make kind-render` | Offline AsyncAPI → Kafka topic artifacts |
| `make kind-validate` | Offline checks (no pull, no cluster) |
| `make kind-test-redis-aof-structure` | Offline Redis AOF manifest + acceptance-script checks |
| `make kind-test-redis-aof` | Explicit/networked: DB15 prefix survives Redis PID 1 restart |
| `make kind-test-debezium-connect-structure` | Offline Connect config/wiring checks (no delivery claim) |
| `make kind-test-debezium-connectors` | Explicit/networked: Connect REST status for 4 connectors (no delivery claim) |
| `make kind-test-debezium-postgres-to-kafka-live` | Explicit: Identity outbox insert → Kafka observe (consumers still PENDING) |
| `make kind-test-debezium-server-structure` | Offline Debezium Server config/wiring checks (no delivery claim) |
| `make kind-test-debezium-postgres-to-redis-live` | Explicit: Room realtime outbox insert → Redis DB2 observe (SSE E2E not claimed) |
| `make kind-create-cluster` | Explicit: create `kind` cluster `uno-arena` |
| `make kind-build-load-bootstrap` | Explicit: build + `kind load` bootstrap image |
| `make kind-load-debezium-connect` | Explicit/networked: node-native `crictl pull` Connect ARM64 digest into kind node |
| `make kind-load-debezium-server` | Explicit/networked: node-native `crictl pull` Server ARM64 digest into kind node |
| `make kind-status-debezium-server` | Read-only Deployment/health (no Postgres→Redis delivery claim) |
| `make kind-apply` | Explicit: datastores → bootstrap Jobs → Connect + Server |
| `make kind-wait` | Re-check Deployments + bootstrap Jobs + Connect/Server |
| `make kind-reset` | Explicit: delete **only** cluster `uno-arena` (rejects name/context overrides) |

Create / apply / reset are never implied by validate.

## Apply ordering

`kind-apply` applies namespace/secrets/datastores first, waits for Postgres /
ClickHouse / Kafka / Kurrent / Redis / Keycloak readiness, then applies bootstrap
and topic Jobs and waits for completion so Job backoff is not spent on datastore
startup. After Kafka topics (including compacted `connect-*`) and Postgres CDC
publications exist, it deletes `job/register-debezium-connectors` (ignore-not-found)
so connector config changes re-register, applies Debezium Kafka Connect, waits for the
connector registration Job, then applies Debezium Server (Room realtime → Redis),
rollout-restarts only `deployment/debezium-server-room-realtime` (ConfigMap apply does
not bump the pod template; a controlled restart reloads mounted startup config), and
waits for that rollout. Stage images first via `make kind-load-debezium-connect` and
`make kind-load-debezium-server` (identify the `uno-arena` control-plane node, clear
prior broken Connect/Server refs, `docker exec` + `crictl pull` the exact approved
quay.io ARM64 digest, verify arch + digest via `crictl inspecti`, and fail closed if
any `repoDigest` is a stale `docker.io/library/import-*` / `/import-` kind-import
alias). These loaders do **not** use `kind load` — import metadata for these upstream
OCI/multiarch images is unreliable in this local containerd setup. If a loader reports
stale kind-import metadata, `make kind-reset`, recreate the cluster, then rerun the
node-native loaders — do not attempt containerd content surgery. Deployments and the
Connect registration Job reference those exact quay.io digests with
`imagePullPolicy: Never` so a skipped stage fails closed with no runtime registry
fallback (workload never pulls).
Connectors and Server use `snapshot.mode=no_data` (streaming only). Structure/status
checks do not claim live CDC delivery.

## Discovery

Foundation Services live in namespace `uno-arena` to match Helm FQDNs such as
`kafka.uno-arena.svc.cluster.local`. Context Postgres instances remain physically
separate Services/databases.

## Credentials

`uno-arena-local-credentials` holds obvious local-only placeholders, scoped per
context for admin/bootstrap/runtime/CDC. Never reuse in staging/production.
