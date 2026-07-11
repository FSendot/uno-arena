# Local GitLab Runner and Staging Runbook

This runbook covers two local lanes:

1. **Kind foundation (Slice 0)** — disposable datastores + bootstrap Jobs under
   namespace `uno-arena`. Foundation datastores + bootstrap Jobs; service
   durable adapters (Identity Postgres/OIDC, Room Postgres/timer, GI Kurrent,
   Analytics ClickHouse HTTP) are separate explicit targets. CDC prerequisites
   (logical WAL, CDC roles/publications, kind-short retention + DLQ topic
   scaffolding) are implemented; Debezium Connect/Server connector delivery and
   Analytics Kafka ingestion and Spectator Kafka consumer remain pending.
2. **Identity staging smoke** — existing GitLab runner + Helm deploy of Identity
   into `staging` (unchanged checkpoint path).

## Prerequisites

- Docker running locally.
- `gitlab-runner` installed and registered to the GitLab project (for the Identity
  staging smoke path).
- `kind`, `kubectl`, `helm`, `docker`, `bash`, `ruby`, and `curl` available on the
  runner host.
- A GitLab token with `read_registry` permission if the project registry is
  private.

## Kind Foundation (Slice 0)

Authority: `docs/architecture/*`, ADR-0027/0028/0030/0031/0035, and
`infrastructure/kind/README.md`.

### What this lane provides

- Kind cluster name `uno-arena` / kubectl context `kind-uno-arena`.
- Namespace `uno-arena` aligned with Helm discovery
  (`*.uno-arena.svc.cluster.local`).
- Four physically separate disposable Postgres instances/databases:
  `identity`, `room_gameplay`, `tournament`, `ranking`.
- Single-node Kafka KRaft broker with **RF1 / minISR1** (explicit non-HA).
- Redis with local AOF (`appendonly yes`, `appendfsync everysec` under
  emptyDir `/data/appendonlydir`) so same-pod Redis container restart can
  reload recent keys; digest-pinned experimental ARM64 KurrentDB 26.0.3,
  ClickHouse, Keycloak 26.7.0 `start-dev` with a minimal `unoarena`
  realm/client/test user.
- Bootstrap Jobs for Identity/Room/Tournament/Ranking Postgres, Analytics
  ClickHouse, and AsyncAPI-derived Kafka topics (plus Connect internal topics,
  kind-short `retention.ms` by ADR-0032 class, and consumer-owned DLQ topic
  scaffolding; **no Connect/Debezium connectors yet**).
- Postgres instances enable `wal_level=logical` with bounded local replication
  slots/senders; bootstrap creates least-privilege CDC roles/publications per
  outbox (Room: separate Kafka vs realtime CDC identities). **No logical slots
  in bootstrap** — Debezium owns slot lifecycle when connectors land.
- Disposable `emptyDir` storage only — no PVC/backup claim. Redis AOF does
  **not** make Redis authoritative: pod replacement or `kind-reset` may lose
  emptyDir data and projections/timers must rebuild from Kafka/Postgres.
- ClusterIP Services only. Ingress host ports `8080→80` / `8443→443` are
  reserved on the kind node for a later local ingress controller; this slice
  does not install ingress or claim production ingress.

### Offline checks (no pull, no cluster)

```bash
make kind-render
make kind-validate
```

### Explicit cluster lifecycle

Create, apply, and reset are **never** implied by validate:

```bash
make kind-create-cluster
make kind-build-load-bootstrap
make kind-apply
make kind-wait
# ... local experiments ...
make kind-reset
```

Scripts refuse to run apply/wait against any kubectl context other than
`kind-uno-arena`. `make kind-reset` hard-pins cluster `uno-arena` and context
`kind-uno-arena` and rejects `KIND_CLUSTER_NAME` / `KIND_CONTEXT_NAME` overrides.

`make kind-apply` waits for datastore readiness before starting bootstrap Jobs
so Job backoff is not consumed by store startup.

### Local-only credentials

Kubernetes Secret `uno-arena-local-credentials` holds obvious placeholder
passwords for admin/bootstrap/runtime roles. They are **not** production
secrets. Runtime DB roles are created without DDL privilege; bootstrap Jobs use
dedicated DDL credentials and Postgres advisory locks (ADR-0027).

### Out of scope for this slice

- Debezium Kafka connectors and Redis sink delivery (CDC prerequisites are in
  place; shared Connect/Debezium Server images and manifests remain pending
  local image availability / network-pull approval).
- Istio, PVC/HA/backup.

### Explicit Room / Identity durable adapter targets (after kind-wait)

```bash
make kind-deploy-identity          # Identity durable (schema-gated /ready)
make kind-deploy-room-gameplay     # Room durable + timer worker (kind values)
make test-room-helm                # offline Helm lint/template
make kind-test-room-adapter-structure
make kind-test-room-integration    # ephemeral unoarena_room_gameplay_test_* only
```

Do **not** auto-reset the authoritative `room_gameplay` database when running
live integration later; the harness creates/drops only its ephemeral test DB.

## Runner Selection

Use a project runner with the shell executor for the local Identity checkpoint
run. In GitLab, open the project and go to **Settings -> CI/CD -> Runners**.

Recommended local setup:

- Enable the local project runner.
- Disable shared instance runners for this project while validating the local
  pipeline, or use a dedicated runner tag and add the same tag to the jobs.
- If the jobs are untagged, make sure the local runner is allowed to run
  untagged jobs.

Expected job log when the local runner is selected:

```text
Running on <local-runner-name>...
```

If the log shows a different host, GitLab selected a
shared SaaS runner instead of the local runner.

Start the local runner in a terminal:

```bash
gitlab-runner run
```

The runner stops at `Initializing executor providers`. At
that point it is waiting for GitLab to assign jobs.

## Local Kubernetes Context (Identity staging smoke)

For the Identity staging pipeline path, ensure the active context is the local
kind cluster (created via `make kind-create-cluster` or equivalent):

```bash
kubectl config use-context kind-uno-arena
kubectl config current-context
kubectl get nodes
```

The shell runner uses the host's active `kubectl` context. Before running a
pipeline, verify the active context points at `kind-uno-arena`.

Foundation datastores live in namespace `uno-arena`. The Identity Helm smoke
path below continues to use namespace `staging` and does not require the
foundation Jobs to be green for the current health-only Identity chart.

## GitLab Registry Pull Secret

The deploy job pins the image by digest, for example:
`registry.gitlab.com/<group>/<project>/identity@sha256:<digest>`.

If the GitLab registry is private, the local cluster needs credentials to pull
that image:

```bash
kubectl create namespace staging --dry-run=client -o yaml | kubectl apply -f -

kubectl -n staging create secret docker-registry gitlab-registry \
  --docker-server=registry.gitlab.com \
  --docker-username=<gitlab-username-or-deploy-token-user> \
  --docker-password=<read_registry_token> \
  --dry-run=client -o yaml | kubectl apply -f -

kubectl -n staging patch serviceaccount default \
  -p '{"imagePullSecrets":[{"name":"gitlab-registry"}]}'
```

Do not commit registry tokens or paste token values into project docs.

## GitLab CI Variables

Set the variables in **Settings -> CI/CD -> Variables**.

| Variable | Value for local checkpoint run | Notes |
|---|---|---|
| `STAGING_IDENTITY_URL` | `http://127.0.0.1:18080` | Not secret. For a first local proof, leave masked off and protected off unless the branch and runner are both configured for protected variables. |

The CI job maps `STAGING_IDENTITY_URL` into `UNOARENA_API_URL` for
`devops-checkpoint/smoke-test/run-smoke-test.sh`. Do not set
`UNOARENA_API_URL` directly in GitLab for the normal pipeline path.

`UNOARENA_CLI_BIN` is no longer needed. The integration job installs the
Client Checkpoint CLI artifact from `client-checkpoint/bin/unoarena`.

## Running The Identity Staging Pipeline

1. Start the runner:

   ```bash
   gitlab-runner run
   ```

2. Ensure the local cluster context is active:

   ```bash
   kubectl config use-context kind-uno-arena
   ```

3. Push to GitLab or run a manual web pipeline from the GitLab UI.

4. Keep a second terminal ready for the Identity service port-forward. Start it
   once `deploy-staging:identity` has created the service, or leave this wait
   loop running before retrying `integration-staging:identity`:

   ```bash
   until kubectl get svc identity -n staging >/dev/null 2>&1; do sleep 2; done
   kubectl port-forward svc/identity 18080:80 -n staging
   ```

   The port-forward must run on the same host as the shell runner. That is why
   the GitLab variable uses `http://127.0.0.1:18080`.

5. Confirm the Identity staging rollout and smoke test:

   ```bash
   kubectl rollout status deployment/identity -n staging --timeout=120s
   kubectl logs deployment/identity -n staging
   ```

6. After a green run, update `devops-checkpoint/README.md` with the real GitLab
   pipeline URL in the **Pipeline Run** section.

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `UNOARENA_API_URL: UNOARENA_API_URL must be set` | `STAGING_IDENTITY_URL` is missing, empty, or unavailable to the pipeline. | Add `STAGING_IDENTITY_URL=http://127.0.0.1:18080` in GitLab CI variables. Check protected-variable settings. |
| Job log shows `saas-linux-small-amd64` or `docker+machine` | GitLab selected a shared runner. | Disable shared runners for the project, or add matching tags to the local runner and jobs. |
| `ImagePullBackOff` in `staging` | `kind` cannot pull the private GitLab registry image. | Create or refresh the `gitlab-registry` image pull secret in the `staging` namespace. |
| Smoke test cannot connect to `127.0.0.1:18080` | The port-forward is not running on the runner host. | Start `kubectl port-forward svc/identity 18080:80 -n staging` before retrying the integration job. |
| `apk` command not found | The job is running under the shell executor, not inside Alpine. | This is expected; the script only runs `apk` when it exists. If it still fails, check the exact failing command in the job log. |
| kind script refuses context | Current kubectl context is not `kind-uno-arena`. | `kubectl config use-context kind-uno-arena` or create the cluster with `make kind-create-cluster`. |
| `make kind-validate` fails on Kafka plan | Generated artifacts missing or drifted. | Run `make kind-render` then `make kind-validate`. |
| Kafka broker logs STARTED but Service has no ready endpoints / probe timeouts | Readiness used heavy `kafka-topics.sh` exec probes that hang while plaintext 9092 is already listening. | Kind manifests use `tcpSocket:9092` readiness/liveness; topic drift stays in `bootstrap-kafka-topics`. Re-apply `30-kafka` or full kind apply. |
| KurrentDB FTL unknown options `ServicePortHttpGrpc` / `Port2113Tcp*` after successful startup | Service-link env vars (`KURRENTDB_*`) collide with Kurrent configuration. | Pod must set `enableServiceLinks: false` under `spec.template.spec`. Re-apply `40-kurrentdb`. |
| `bootstrap-clickhouse-analytics` fails with ClickHouse Code 62 `Multi-statements are not allowed` | Full migration SQL was POSTed as one HTTP body after the default sentinel. | Bootstrap splits `001_init.sql` and POSTs one statement per request (no `multiquery=1`). After a partial apply, run `make kind-reset` then re-apply. |
