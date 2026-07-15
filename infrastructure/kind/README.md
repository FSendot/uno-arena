# Local kind foundation (Slice 0)

Disposable single-node `kind` cluster plus context-owned datastore bootstrap.
Not production. Application datastores remain disposable. The observability stack's
MinIO PVC survives pod replacement only; there is no HA, backup, cross-cluster
durability, RPO, or RTO claim, and cluster deletion intentionally removes its logs.
The shared MinIO PVC requests 5 GiB for separate Loki and Tempo buckets. Loki's
Compactor and Tempo each enforce 24-hour local retention; MinIO has no competing
lifecycle rule. Tempo additionally requests a 1 GiB WAL/work PVC.
Loki WAL backpressure remains fail-closed at 99 percent on the disposable kind
node, whose filesystem includes the local engine's image cache; production keeps
the chart's conservative 90 percent threshold.

This is the canonical deployment-acceptance environment defined by ADR-0045.
The acceptance contract starts from no `uno-arena` cluster, creates it, and must
install the complete infrastructure, application, and observability stack before
running functional and business-metric proofs. The required observability stages and
fail-closed probes are implemented below; they become live deployment evidence only
when the clean empty-cluster lane passes on the current machine.
It installs Istio Ambient, enrolls both `uno-arena` and `observability`, and enforces
strict east-west mTLS. An L4 authorization policy allows only the Prometheus service
account to scrape application port `9090`; acceptance proves that allow and an
unauthorized-workload denial. Local plaintext exceptions cover only documented
application-layer database TLS and disposable credentials, never mesh transport.
Per ADR-0046, that observability stack includes Alloy log collection, monolithic
Loki with TSDB, Grafana, and a local MinIO S3-compatible bucket. A lightweight
standalone Prometheus scrapes service-owned `/metrics` endpoints on internal port
`9090`, exposed through the OpenTelemetry Prometheus exporter, and Grafana is
provisioned with Prometheus and Loki data sources. Alloy is not part of this
pull-based metrics path. Per ADR-0047, instrumented services send OTLP to an Alloy
gateway and monolithic Tempo stores traces in a dedicated MinIO bucket; Grafana
provisions Tempo as its third data source. Production keeps the same Loki S3 storage
contract but defaults to an external S3 bucket; local credentials
and endpoint settings are not production defaults.

## Layout

- `cluster.yaml` — kind cluster + documented ingress host-port mappings (no ingress installed here)
- `manifests/` — namespaces, local-only Secrets, Postgres×4, Kafka RF1, Redis
  (local AOF under disposable emptyDir; not authoritative), architecture-selected
  KurrentDB digest,
  ClickHouse, Keycloak start-dev, Debezium Kafka Connect (four outbox routers;
  exact multi-architecture OCI index), Debezium Server (Room realtime → Redis DB2;
  separate from Kafka Connect; same index-pinning policy)
- observability delivery — local-only MinIO, its shared 5 GiB PVC, and the
  idempotent `loki-logs`/`tempo-traces` bucket Job are kind foundation manifests. Once
  ready, the repository-owned
  `infrastructure/observability/helm/uno-arena-observability` chart installs Alloy,
  monolithic Loki TSDB, Tempo, Prometheus, kube-state-metrics, Grafana, policies,
  data sources, and dashboards with `values.kind.yaml`. Tempo's 1 GiB WAL/work PVC and
  MinIO storage survive pod replacement but are deleted with the cluster; Loki and
  Tempo each enforce 24-hour retention. Offline renders and structure checks are
  implemented; the clean lane provides the corresponding live evidence
- metrics resources — the OpenTelemetry Go Meter API, per-process custom
  Prometheus registries and OpenTelemetry Prometheus-exporter `:9090/metrics`
  endpoints on named port `metrics`, standalone Prometheus discovery/scrape
  configuration, a provisioned Grafana data source, separate business and Kubernetes
  cluster dashboards, exposition-contract tests, and live metric probes. Stable APIs
  add `metrics` to their existing Services and use EndpointSlice discovery. Service-less
  workers and dedicated Room runtimes use direct pod discovery, including unready pods;
  mutually exclusive scrape-mode labels prevent duplicates, and validation rejects
  missing or ambiguous targets. Local jobs use a 15-second scrape/evaluation interval
  and 5-second scrape timeout; metric proofs poll for up to 45 seconds before failing.
  Production may override cadence. No metrics-only worker Services are created. Mimir,
  Prometheus Operator, Alertmanager, and the full `kube-prometheus-stack` are outside
  local acceptance
- Prometheus persistence — a 5 GiB PVC with 24-hour retention and a size ceiling
  below filesystem capacity. History survives Prometheus pod replacement but is
  intentionally deleted with the cluster; production storage remains environment-owned
- Grafana state/access — data sources and non-editable dashboard JSON are
  version-controlled ConfigMaps, local admin credentials come from the local Secret,
  and no Grafana PVC is required. The ClusterIP service is accessed only through a
  `127.0.0.1` port-forward; live acceptance verifies provisioning through its HTTP API
- namespace/RBAC — all observability workloads run in `observability`, separate from
  `uno-arena`. `alloy-logs` has only pod-metadata reads and host-log mounts;
  `alloy-otlp` disables token automount and has no Kubernetes API access. Prometheus
  and kube-state-metrics receive only scoped reads; Loki, Tempo, MinIO, and Grafana
  disable token automount. No observability identity may read application Secrets
- mesh security — kind installs Istio Ambient and enrolls both namespaces with strict
  mTLS and default-deny L4 authorization. The Prometheus service-account principal is
  explicitly allowed to reach application port `9090`; an unauthorized identity must
  fail the same scrape. No sidecars or L7 waypoints are introduced
- logging contract — repository Go processes emit `log/slog` JSON lines with
  required UTC RFC3339Nano `timestamp`, lowercase `level`, service/component/
  environment/version/instance identity, and bounded snake-case `event`; `message` is
  optional and valid active spans add `trace_id`/`span_id`. Domain fields retain
  canonical camelCase contract spelling. Sanitized failures use only `error`; callers
  cannot override base fields, and source paths, stack dumps, and shutdown-bypassing
  `log.Fatal` paths are removed. Alloy labels only bounded namespace/service/component/
  container/level dimensions; identifiers remain parsed fields, and sensitive
  credentials/private gameplay/payload data is forbidden
- fail-closed rejection audits — Gateway/Room/Tournament adapters construct
  `slog.Record` values and call the standard handler directly so encoding/stdout errors
  propagate to command handling while preserving context-owned idempotency/conflict
  rules. Required Kubernetes workloads use the Alloy-collected stdout stream and reject
  audit-file env values or volumes; explicit offline/tests may retain file sinks
- tracing contract — OpenTelemetry-instrumented Go processes propagate W3C Trace
  Context over HTTP and Kafka, export OTLP to a dedicated Alloy gateway, and store
  traces in monolithic Tempo's `tempo-traces` MinIO bucket. Grafana provisions Tempo
  and trace-to-log links; no separate OpenTelemetry Collector is deployed. The kind
  Gateway uses `x_parentbased_mutations`: command roots are always sampled while read
  roots use the production-like `0.001` ratio; parent decisions remain authoritative
  downstream. Production defaults to configurable `parentbased_traceidratio=0.001`;
  local Alloy batches without tail sampling
- OTLP security — a private Alloy gateway Service exposes `4317` and `4318`, with
  applications defaulting to gRPC. Strict Ambient mTLS plus default-deny authorization
  permits only dedicated `uno-arena` application service accounts, mirrored by
  production NetworkPolicy where enforced; kind proves the same SPIFFE-scoped Istio
  authorization without adding a second CNI. No bearer/shared secret exists. Loki and Tempo reject
  direct application ingestion and allow only the corresponding Alloy/backend identities
- instrumentation split — HTTP uses `otelhttp`, direct franz-go Kafka clients use
  `kotel`, Redis uses `redisotel` tracing, `pgxpool` uses `otelpgx`, and Analytics'
  ClickHouse HTTP transport uses `otelhttp`. Game Integrity defines meaningful manual
  KurrentDB adapter spans. Bounded contexts own command/commit/publication/worker spans;
  SQL text, Redis keys, payloads, credentials, and identifier metric labels are excluded
- Go telemetry pins — repository language/toolchain remain `go 1.26.0`/`go1.26.5`.
  OpenTelemetry core/SDK/OTLP are `v1.44.0`, Prometheus exporter `v0.66.0`,
  `otelhttp v0.69.0`, `kotel v1.7.0`, `redisotel v9.21.0`, `otelpgx v0.11.1`, and
  Prometheus client `v1.23.2`; all `go-redis` clients align on `v9.21.0`, while
  `pgx v5.10.0` and franz-go `v1.21.5` remain. Dependency Go declarations are minimums;
  repository images compile with Go 1.26.5 for the detected architecture and do not
  affect third-party multi-architecture image selection
- telemetry resource identity — every API/worker process uses stable canonical
  `service.name`, `service.namespace=uno-arena`, build/commit `service.version`,
  `deployment.environment.name`, pod UID `service.instance.id` (hostname fallback
  outside Kubernetes), and bounded `unoarena.component`. Roles share the service name.
  Domain/correlation identifiers never become resource attributes or metric labels;
  Prometheus Kubernetes target metadata carries pod/instance identity
- telemetry code boundary — an infrastructure-only `platform/telemetry` Go module owns
  OpenTelemetry resource, provider, propagation, exporter, bounded-queue, and shutdown
  setup plus the `log/slog` JSON handler. The handler applies shared identity and adds
  active `trace_id`/`span_id`; logs still go to stdout for Alloy, not through an OTLP
  log exporter. Bounded-context services own their instruments, span/attribute
  vocabulary, post-commit recording seams, log events/fields/sanitization, and log
  placement. The module contains no domain types or business metric catalog, and the
  existing `shared` module remains limited to stdlib helpers
- telemetry module interface — each process calls `Start(ctx, config)` once before
  constructing instrumented adapters and receives its logger, explicit providers, and
  propagator; it stops business work before one bounded `Shutdown(ctx)` flush. Required
  startup validates configuration and binds `:9090` synchronously. Disabled mode keeps
  JSON logging with no-op providers. The sole global provider is the same MeterProvider,
  registered once for official SDK self-observability; tracer/propagator remain explicit,
  and the global error handler emits sanitized structured exporter failures. Exported
  exporter/registry/queue factories are forbidden
- CDC trace transport — every Kafka-bound outbox and Room realtime outbox stores
  nullable `traceparent`/`tracestate` in the authoritative transaction. Connect maps
  them to Kafka headers, Server maps them to Redis metadata, and direct Kafka clients
  inject/extract the same headers. Outboxes never persist W3C baggage
- canonical trace proof — the accepted casual-game command that produces
  `GameCompleted` must form one causal trace across Gateway inbound and Gateway→Room
  HTTP, Room command/commit, Game Integrity append, Room outbox, Debezium Kafka context
  transport, and Ranking consume/commit. Debezium need not emit a span. Tempo returns
  the complete trace and Loki returns correlated logs by parsed `trace_id` and
  `correlationId`, never identifier labels
- independent business-flow sequencing — after the casual best-of-three, the
  single-node lane requires every observability collector/backend to regain
  readiness and then retains the 15-second settle window before tournament
  setup. This drains telemetry backlog without replaying an unknown mutation or
  changing production service timeouts
- telemetry failure semantics — OTLP batching, queues, retries, export timeouts, and
  shutdown flushes are bounded. Alloy/Tempo outages may drop observable spans but
  never fail application readiness or block business work; a live probe removes and
  restores Alloy while proving gameplay continuity and trace resumption
- trace batching/self-observability — required mode sets
  `OTEL_GO_X_OBSERVABILITY=true` and uses the official nonblocking processor with queue
  `2048`, batch `512`, `5s` schedule/export deadlines, and retry bounded by the export
  deadline. Prometheus exposes SDK queue size/capacity/processed/`queue_full` drops;
  a local wrapper counts and logs sanitized export failures. Experimental SDK metric
  names are internal, pinned, and exposition-tested rather than business contracts
- telemetry startup mode — direct/offline processes may explicitly disable telemetry,
  but every API and worker rendered by Helm must use `required`. Invalid resource or
  exporter configuration and metrics-port bind failures then fail process startup.
  Static acceptance rejects disabled or unspecified Kubernetes workloads; backend
  reachability remains outside application readiness after initialization
- telemetry configuration — Helm injects `TELEMETRY_MODE`, existing `SERVICE_NAME` and
  `DEPLOYMENT_ENV`, immutable `SERVICE_VERSION`, explicit bounded
  `UNOARENA_COMPONENT`, Downward-API `POD_UID`, standard
  `OTEL_EXPORTER_OTLP_ENDPOINT`, `OTEL_EXPORTER_OTLP_PROTOCOL`,
  `OTEL_TRACES_SAMPLER`, `OTEL_TRACES_SAMPLER_ARG`, and `METRICS_ADDR=:9090` into
  every API, worker, and generated Room runtime. Components are allowlisted and never
  inferred from pod names
- trace persistence — the existing 5 GiB MinIO PVC is shared by dedicated Loki and
  Tempo buckets, Tempo owns a 1 GiB WAL/work PVC, and both backends use 24-hour
  retention. A live probe proves an accepted trace survives Tempo and MinIO restarts.
  Pod-replacement mutations target the captured Ready pod name and use bounded,
  idempotent retries so an API timeout cannot make a retry delete the replacement

## Portable upstream image pins

Generic upstream images are pinned to verified OCI multi-architecture index digests
that contain Linux AMD64 and ARM64 children. Release tags are audit metadata only;
workloads reference the immutable index and the node's container runtime resolves its
matching child. The clean lane validates index membership and resolved platform. It
does not embed or pre-stage an ARM64 child in a generic manifest.

| Component | Source reference | Multi-architecture index digest |
|---|---|---|
| Alloy | `grafana/alloy:v1.17.1` | `sha256:4f6ddc56ffdcf8a6316748fc5162972e20cb301523cac1bb4a31957df733ae9b` |
| Loki | `grafana/loki:3.7.3` | `sha256:70b9f699fc9bb868b62f1cfd4f787dfa50242f1fd92e6089787d5d7daea75fe8` |
| Tempo | `grafana/tempo:3.0.2` | `sha256:cda87c212d8c584dc0b89e337e7ed648a5100feb657e5d528480ee4fa03dbbe3` |
| Grafana | `grafana/grafana:13.1.0` | `sha256:121a7a9ece6dc10b969f1f96eed64b4f07dfac0d0b8abc070f7cb83bbde86f63` |
| Prometheus | `prom/prometheus:v3.13.1` | `sha256:3c42b892cf723fa54d2f262c37a0e1f80aa8c8ddb1da7b9b0df9455a35a7f893` |
| kube-state-metrics | `registry.k8s.io/kube-state-metrics/kube-state-metrics:v2.19.1` | `sha256:85108987d044b18a098126732f98602df408888c0f7d456241f5abefb9744bc1` |
| MinIO | `quay.io/minio/minio:RELEASE.2025-04-22T22-12-26Z` | `sha256:a1ea29fa28355559ef137d71fc570e508a214ec84ff8083e39bc5428980b015e` |
| MinIO client | `quay.io/minio/mc:RELEASE.2025-04-16T18-13-26Z` | `sha256:aead63c77f9db9107f1696fb08ecb0faeda23729cde94b0f663edf4fe09728e3` |
| PostgreSQL | `postgres:18.4-alpine3.24` | `sha256:9a8afca54e7861fd90fab5fdf4c42477a6b1cb7d293595148e674e0a3181de15` |
| Apache Kafka | `apache/kafka:4.3.1` | `sha256:77e3df9054047a88b520d0cc46e16696d3b22022e1d580aeccd2632df6532837` |
| Redis | `redis:8.8.0-alpine` | `sha256:9d317178eceac8454a2284a9e6df2466b93c745529947f0cd42a0fa9609d7005` |
| ClickHouse | `clickhouse/clickhouse-server:26.6.1.1193` | `sha256:1d1f6508eba2dccce2cee9913907c5f7766327debc57a6b1991f2c9e3176c163` |
| Keycloak | `quay.io/keycloak/keycloak:26.7.0` | `sha256:2eb3cd316835c990e69e26ade292ffa78f6fb0db7d5fc6377463c162e1979ac0` |
| PgBouncer | `edoburu/pgbouncer:v1.24.1-p1` | `sha256:3db3d7223e93af52b4116f642951a1a5fa44702a88c2a59cf7562cac19320c9e` |
| Debezium Connect | `quay.io/debezium/connect:3.6.0.Final` | `sha256:61d29e5a0316de5dd0a564ec40eaa662d837a05217523e1a1745ecde3d790455` |
| Debezium Server | `quay.io/debezium/server:3.6.0.Final` | `sha256:adec18409dff7bcc2d00511f1d5aee5b7677cd5901ef729576ac02728d30ea9d` |
| Istio pilot | `docker.io/istio/pilot:1.30.2` | `sha256:d158739d5286f7899bc039589d248720c2a9b6622d54eeb7a3fdfbb65200c22c` |
| Istio CNI | `docker.io/istio/install-cni:1.30.2` | `sha256:b2eb80818fc345e3e9033f424ec7757cbf1a9d9a6494fea79648dab4887f2f7f` |
| Istio ztunnel | `docker.io/istio/ztunnel:1.30.2` | `sha256:64d7c4ea9621fdad66160744dcf76999995fcc0ac399d04aba76d6d0aae72242` |

MinIO is an explicit local-only exception: later source releases do not have a
corresponding official server container. Production deploys no MinIO image and uses
external S3.

KurrentDB 26.0.3 currently has no suitable common index. The deployment selects one
reviewed image from the detected node architecture and fails on any other value:

| Node architecture | Exact image digest | Support boundary |
|---|---|---|
| `linux/amd64` | `docker.kurrent.io/kurrent-lts/kurrentdb:26.0.3@sha256:b4d0665a78269cd7184971c4d1fad38265277901f3d3730d89dcfba8f3d37fe9` | Stable publisher artifact |
| `linux/arm64` | `kurrentplatform/kurrentdb:26.0.3-experimental-arm64-10.0-noble@sha256:8498556a8ba7a74f8d4ea31a149b1e5216e167d6884b630a68b3e1eb9e6e870e` | Experimental, local-only |

Istio uses the vendored official `base`, `istiod`, `cni`, and `ztunnel` Helm charts
at exact version `1.30.2`; their archive checksums are verified before installation.
The acceptance lane uses Kubernetes state and live traffic probes and does not require
`istioctl`.
- cluster metrics — kube-state-metrics supplies object state; Prometheus uses a
  scoped service account to scrape kubelet/cAdvisor pod and container resource
  metrics. Metrics Server is unnecessary, and node-exporter is excluded locally
  because kind's node is a Docker container rather than a representative host
- mandatory business counters — OpenTelemetry instruments
  `unoarena.players.created`, `unoarena.games.completed`, and
  `unoarena.tournaments.completed` export exactly as Identity
  `unoarena_players_created_total`, Room `unoarena_games_completed_total`, and
  Tournament `unoarena_tournaments_completed_total`; post-commit probes must produce
  and query nonzero values, while rejects, failed commits, and idempotent replays
  remain uncounted. They are process-local operational signals rather than
  authoritative accounting; each process records an unlabeled zero at telemetry startup
  to materialize a real empty-cluster baseline without claiming a transition. Durable
  context state/events remain truth, and dashboard
  totals use `sum(increase(<counter>[$__range]))` across replicas and resets. The live
  proof waits for a baseline scrape, runs each CLI flow, waits for the next scrape,
  and confirms one additional scrape interval before treating history-only progress
  as a process-reset result rather than instant-query lag,
  and asserts a positive bounded-interval increase. All three use unit `1` and define
  no instrument labels; Prometheus discovery alone supplies bounded service, namespace,
  component, pod, and instance labels. Contract tests pin exact names, counter types,
  unit, context-specific help text, and absence of instrument labels
- `generated/` — Kafka topics Job/plan rendered from `contracts/asyncapi/kafka-v1.yaml`
  (kind-short retention.ms, DLQ scaffolding, Connect compacted internals)
- `scripts/` — render / validate / create / build-load / portable-image validation /
  apply / deploy-services / probes / wait / reset helpers
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
| `make kind-test-debezium-postgres-to-kafka-live` | Explicit: Identity outbox insert → Kafka observe (consumer behavior is verified by its own lane) |
| `make kind-test-debezium-server-structure` | Offline Debezium Server config/wiring checks (no delivery claim) |
| `make kind-test-debezium-postgres-to-redis-live` | Explicit: Room realtime outbox insert → Redis DB2 observe (SSE E2E not claimed) |
| `make kind-create-cluster` | Explicit: create `kind` cluster `uno-arena` |
| `make kind-build-load-bootstrap` | Explicit: build + `kind load` bootstrap image |
| `make kind-status-debezium-server` | Read-only Deployment/health (no Postgres→Redis delivery claim) |
| `make kind-apply` | Explicit: datastores → bootstrap Jobs → Connect + Server |
| `make kind-wait` | Re-check Deployments + bootstrap Jobs + Connect/Server |
| `make kind-reset` | Explicit: delete **only** cluster `uno-arena` (rejects name/context overrides) |

The complete local lane is intentionally a script rather than an implicit Make
dependency. Preview it offline first, then provide the literal destructive
confirmation:

```bash
./infrastructure/kind/scripts/clean-deploy.sh --dry-run
./infrastructure/kind/scripts/clean-deploy.sh --confirm-reset uno-arena
```

The live invocation validates offline, checks the required tools and Docker/node
architecture before deletion, deletes only `uno-arena`, then requires at least
20 GiB of host filesystem headroom and 7 GiB of container-engine memory before
recreating it. These gates prevent a complete image set from exhausting the engine's
backing disk and prevent the full stack from entering reclaim/swap pressure; an
operator may raise the floors with `KIND_MIN_HOST_FREE_GIB` and
`KIND_MIN_ENGINE_MEMORY_GIB`, but lowering either is not a successful substitute for
the acceptance run. The lane then builds and
loads the bootstrap plus all eight application images for the detected node platform,
validates every upstream multi-architecture index, selects the reviewed KurrentDB
digest for AMD64 or ARM64, applies and waits for the foundation and Istio Ambient,
readies local MinIO and both buckets, installs and readies the observability Helm
release before the application Helm releases, and runs the CDC, adapter, integration, observability,
mesh, Redis, and Kafka-to-ClickHouse probes. Use `--skip-probes` only while diagnosing
deployment startup. Unknown node architectures fail closed.

The kind service overlays are deliberately sized for a busy single-node acceptance
run: repository-owned APIs and workers keep 50m CPU/64Mi memory requests, may burst
to 500m CPU/256Mi memory, and run their HTTP health checks every 20 seconds with a
three-second timeout and six consecutive failures required before kubelet restarts
them. Dedicated Room runtime Pods receive the same
kind-only CPU ceiling and six-failure tolerance, with a ten-second probe timeout so
an otherwise-live singleton is not removed from its Service during an observed
single-node contention spike that remains within the router request budget. The
four Postgres pods and Redis run readiness checks every 15 seconds and liveness
checks every 30 seconds; Debezium Connect uses 20- and 30-second checks. This avoids
continuous local exec-probe process churn while preserving bounded failure detection.
Because kind runs exactly one Connect worker, its scheduled-rebalance delay is ten
seconds rather than the distributed five-minute default; a brief broker stall cannot
leave all four local CDC connectors unassigned for five minutes.
The co-located Kafka, Keycloak, Debezium Connect/Server, ClickHouse, KurrentDB,
MinIO, and Grafana processes have kind-only runtime memory budgets. Kafka, Keycloak,
and both Debezium JVMs use explicit heaps; ClickHouse has a 768 MiB server ceiling
and bounded caches; MinIO and Grafana use 256 MiB Go memory targets. Heavy
single-replica disposable Deployments use `Recreate`, so a manifest re-apply cannot
temporarily duplicate them on the shared node. ClickHouse, KurrentDB, and Connect
retain 1 GiB container ceilings for native, mapped, metaspace, and plugin headroom;
these local controls do not alter production sizing.
The business-acceptance bots use a five-second polling cadence, and the live lane runs
their business/trace proof before adapter and projection-rebuilder probes accumulate
load. The Tournament integration package caps Go test parallelism at two and uses an
eight-minute package timeout because its 188 real-Postgres tests plus bounded
Redis-fallback cases reached the final test at 4:58 in a measured loaded-cluster run.
These values prevent local CFS throttling from turning concurrent bot and
telemetry traffic into false liveness failures; they do not replace production
capacity or probe tuning. Base, staging, and production application probes retain
their ten-second cadence.

The Alloy OTLP outage probe proves a Room mutation while the collector is absent.
When Alloy returns, all application exporters reconnect concurrently; the probe
therefore gives the complete observability stack a bounded five-minute readiness
window before it generates fresh recovery commands, then requires a complete new
trace within three minutes. Application readiness remains independent of telemetry.

The Loki persistence probe uses the supported ingester shutdown with full-process
termination, not a timed shipper sleep. Graceful process shutdown cuts Loki's active
15-minute TSDB head and performs the shipper's final upload; after kubelet restarts
that same Pod, the probe deletes its exact identity and requires a distinct Pod with
an empty work volume to retrieve the marker from MinIO before MinIO is replaced.

The complete ARM64 lane has been exercised with the foundation and all eight
services ready. Kafka Connect and Debezium Server CDC were live, and the full
Spectator Kafka→Room→Redis and Analytics Kafka→producer-backfill→ClickHouse
recovery-worker proofs passed. Consequently, `projectionRebuilder.enabled=true`
is scoped to Spectator and Analytics `values.kind.yaml`; default, staging, and
production values remain false. Stage B acceptance also completed a live casual
best-of-three and a live tournament from registration through assignment, Room
play, result consumption, advancement, and terminal `TournamentCompleted`.

`clean-deploy.sh --dry-run` is offline and non-mutating. The live form requires
Docker and registry/network access and must be started deliberately.

Create / apply / reset are never implied by validate. `kind-validate` also
checks the complete-lane structure and executes only its dry-run plan.

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
waits for that rollout. Foundation Deployments and the Connect registration Job
reference immutable OCI index digests rather than tags or architecture-specific
children. Containerd pulls the index reference and selects its native child.
Validation confirms that the declared index contains both required Linux platforms
and that the resolved image matches the node; an index mismatch, unexpected child,
mutable tag-only reference, or unsupported architecture fails closed.
Connectors and Server use `snapshot.mode=no_data` (streaming only). Structure/status
checks do not claim live CDC delivery.

Debezium `3.6.0.Final` is pinned to Connect index
`sha256:61d29e5a0316de5dd0a564ec40eaa662d837a05217523e1a1745ecde3d790455`
and Server index
`sha256:adec18409dff7bcc2d00511f1d5aee5b7677cd5901ef729576ac02728d30ea9d`.
The release tag remains documentation only and registry republishing cannot change
the workload reference.

## Discovery

Foundation Services live in namespace `uno-arena` to match Helm FQDNs such as
`kafka.uno-arena.svc.cluster.local`. Context Postgres instances remain physically
separate Services/databases.

## Credentials

`uno-arena-local-credentials` holds obvious local-only placeholders, scoped per
context for admin/bootstrap/runtime/CDC. Never reuse in staging/production.
