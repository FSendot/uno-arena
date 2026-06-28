# DevOps Checkpoint — UnoArena

## Pipeline Run

> Link to green pipeline run (to be added after first successful execution on GitLab):
> `https://gitlab.com/<group>/uno-arena/-/pipelines/<id>`

---

## Overview

This document covers the pipeline design for the UnoArena DevOps checkpoint. All services from the Architecture Checkpoint are present as placeholders, wired through at least `test → build → deliver`. The Identity Service is the fully-wired service, going all the way through `deploy-staging → integration-staging`.

---

## 6.1 Repository Layout and Service Ownership

```
uno-arena/
├── .gitlab-ci.yml                        # Root orchestrator — includes all fragments
├── ci/
│   └── templates/
│       └── service.gitlab-ci.yml         # Shared stage template (rules:changes catch-all)
├── devops-checkpoint/
│   ├── README.md                         # This document
│   └── smoke-test/                       # CLI smoke test harness for Identity staging
│       └── run-smoke-test.sh
├── services/
│   ├── gateway/                          # Realtime Gateway / BFF
│   │   ├── src/
│   │   ├── Dockerfile
│   │   ├── ci/gateway.gitlab-ci.yml
│   │   └── helm/gateway/
│   ├── identity/                         # Identity Service ← FULLY WIRED
│   │   ├── src/
│   │   ├── Dockerfile
│   │   ├── ci/identity.gitlab-ci.yml
│   │   └── helm/identity/
│   ├── room-gameplay/                    # Room Gameplay Service (+ Timer Worker)
│   │   ├── src/
│   │   ├── Dockerfile
│   │   ├── ci/room-gameplay.gitlab-ci.yml
│   │   └── helm/room-gameplay/
│   ├── game-integrity/                   # Game Integrity Service
│   │   ├── src/
│   │   ├── Dockerfile
│   │   ├── ci/game-integrity.gitlab-ci.yml
│   │   └── helm/game-integrity/
│   ├── tournament-orchestration/         # Tournament Orchestration Service (+ Provisioning Workers)
│   │   ├── src/
│   │   ├── Dockerfile
│   │   ├── ci/tournament-orchestration.gitlab-ci.yml
│   │   └── helm/tournament-orchestration/
│   ├── ranking/                          # Ranking Service
│   │   ├── src/
│   │   ├── Dockerfile
│   │   ├── ci/ranking.gitlab-ci.yml
│   │   └── helm/ranking/
│   ├── spectator-view/                   # Spectator Projection Service (+ Projection Rebuilders)
│   │   ├── src/
│   │   ├── Dockerfile
│   │   ├── ci/spectator-view.gitlab-ci.yml
│   │   └── helm/spectator-view/
│   └── analytics/                        # Analytics / Public Read Models Service
│       ├── src/
│       ├── Dockerfile
│       ├── ci/analytics.gitlab-ci.yml
│       └── helm/analytics/
```

### Service → Image → Helm Release → Wiring Depth

| Service | Folder | Image | Helm Release | Wiring Depth |
|---|---|---|---|---|
| Realtime Gateway / BFF | `services/gateway` | `registry/gateway:<branch>-<sha>` | `gateway` | test → build → deliver |
| **Identity Service** | `services/identity` | `registry/identity:<branch>-<sha>` | `identity` | **test → build → deliver → deploy-staging → integration-staging** |
| Room Gameplay Service | `services/room-gameplay` | `registry/room-gameplay:<branch>-<sha>` | `room-gameplay` | test → build → deliver |
| Game Integrity Service | `services/game-integrity` | `registry/game-integrity:<branch>-<sha>` | `game-integrity` | test → build → deliver |
| Tournament Orchestration Service | `services/tournament-orchestration` | `registry/tournament-orchestration:<branch>-<sha>` | `tournament-orchestration` | test → build → deliver |
| Ranking Service | `services/ranking` | `registry/ranking:<branch>-<sha>` | `ranking` | test → build → deliver |
| Spectator Projection Service | `services/spectator-view` | `registry/spectator-view:<branch>-<sha>` | `spectator-view` | test → build → deliver |
| Analytics Service | `services/analytics` | `registry/analytics:<branch>-<sha>` | `analytics` | test → build → deliver |

### Workers

Room Timer Worker, Tournament Provisioning Workers, and Projection Rebuilders are not independent pipeline services. They are deployed as separate `Deployment` resources inside their owning service's Helm chart. See [ADR-0013](../docs/adr/0013-workers-bundled-with-owning-service.md).

### Fully-Wired Service: Identity

Identity is the fully-wired service because it has a self-contained public contract — `register` followed by `whoami` — with no cross-service dependency in the placeholder. The BFF was the alternative but its smoke test would require both BFF and Identity alive and talking to each other, adding two readiness gates and a cross-service dependency for no additional demonstration value. Identity's placeholder can skip real IdP integration and return a hardcoded canned response the CLI can assert against unconditionally.

---

## 6.2 Pipeline Narrative

### Structure

The root `.gitlab-ci.yml` includes each service's pipeline fragment via `include:`. Each fragment is scoped to its own source path using `rules: changes:` so only the touched service rebuilds on a given push.

```yaml
# .gitlab-ci.yml (root)
include:
  - local: services/identity/ci/identity.gitlab-ci.yml
  - local: services/gateway/ci/gateway.gitlab-ci.yml
  - local: services/room-gameplay/ci/room-gameplay.gitlab-ci.yml
  - local: services/game-integrity/ci/game-integrity.gitlab-ci.yml
  - local: services/tournament-orchestration/ci/tournament-orchestration.gitlab-ci.yml
  - local: services/ranking/ci/ranking.gitlab-ci.yml
  - local: services/spectator-view/ci/spectator-view.gitlab-ci.yml
  - local: services/analytics/ci/analytics.gitlab-ci.yml
```

### Stage Spine (per service)

| Stage | What runs | Failure semantics |
|---|---|---|
| `test` | Unit tests, linter, contract check (Room Gameplay + Spectator View only) | Fails the pipeline for that service; `build` does not start |
| `build` | Compiles the service binary inside Docker build context | Fails the pipeline; `deliver` does not start |
| `deliver` | Pushes image to GitLab Container Registry; packages and publishes the Helm chart to GitLab Package Registry; captures digest as CI artifact where downstream deploys need it | Fails the pipeline; no deploy starts |
| `deploy-staging` | `helm upgrade --install` against staging namespace; waits for `kubectl rollout status` | Fails if pods are not healthy; `integration-staging` does not start |
| `integration-staging` | CLI smoke test via `devops-checkpoint/smoke-test/run-smoke-test.sh` | Fails if service is unreachable or returns wrong response |
| `deliver-production` | *(optional, manual gate)* Promotes same digest to production tag | Must pin same digest from `deliver`; no rebuild |
| `deploy-production` | *(optional, manual gate)* `helm upgrade --install` against production namespace | Same chart, different values file |

### Fail-Fast Wiring

Each stage uses `needs:` to declare an explicit dependency on the previous stage for the same service. If `test:identity` fails, `build:identity` never starts. No cross-service stage can unblock a failed service.

Cross-service contract failures: if the schema-backed contract check between Room Gameplay and Spectator View fails, both services' `test` stages report red and neither proceeds to `build`. See [ADR-0014](../docs/adr/0014-async-contract-check-room-gameplay-to-spectator-view.md).

Retries: no silent retries. If a job is flaky, it reports red. An explicit `retry: 1` may be added to the `integration-staging` job only, and must be documented as such.

### Change Detection

Each service fragment uses `rules: changes:` scoped to its own path and to shared templates:

```yaml
rules:
  - changes:
    - services/identity/**
    - ci/templates/**
```

A change to `ci/templates/**` triggers all services. A change scoped to one service folder triggers only that service. See [ADR-0012](../docs/adr/0012-monorepo-change-detection-via-rules-changes.md).

---

## 6.5 Deploy Model

**Helm releases applied from the pipeline.** See [ADR-0010](../docs/adr/0010-helm-from-pipeline-as-deploy-model.md).

Each service's `deliver` job publishes two independently versioned artifacts:

- Container image: `$CI_REGISTRY_IMAGE/<service>:$CI_COMMIT_REF_SLUG-$CI_COMMIT_SHORT_SHA`
- Helm chart: GitLab Helm Package Registry channel `$CI_COMMIT_REF_SLUG`, chart version `0.1.$CI_PIPELINE_IID`, app version `$CI_COMMIT_SHORT_SHA`

For the fully-wired Identity service, `deploy-staging` consumes that published chart version and still pins the runtime image by digest:

```bash
helm repo add --force-update \
  --username gitlab-ci-token \
  --password $CI_JOB_TOKEN \
  uno-arena-$CI_COMMIT_REF_SLUG \
  $CI_API_V4_URL/projects/$CI_PROJECT_ID/packages/helm/$CI_COMMIT_REF_SLUG

helm upgrade --install $SERVICE_NAME uno-arena-$CI_COMMIT_REF_SLUG/$SERVICE_NAME \
  --version 0.1.$CI_PIPELINE_IID \
  --namespace staging \
  --values ./services/$SERVICE_NAME/helm/$SERVICE_NAME/values.staging.yaml \
  --set image.repository=$CI_REGISTRY_IMAGE/$SERVICE_NAME \
  --set image.digest=$IMAGE_DIGEST \
  --force-conflicts
kubectl rollout status deployment/$SERVICE_NAME -n staging --timeout=120s
```

`--force-conflicts` is intentional for Helm 4 server-side apply: the pipeline chart is the source of truth for the Deployment image, so CI reclaims fields that were previously changed with tools such as `kubectl set image`.

The kubeconfig is injected as a masked GitLab CI variable (`KUBE_CONFIG`). It is never committed to the repo.

### Environment Differences

Each service has two values files:

- `values.staging.yaml` — 1 replica, staging URLs, relaxed resource limits
- `values.production.yaml` — 2+ replicas, production URLs, tighter resource limits

At minimum, `replicaCount` and `env.API_URL` differ between environments.

### Secrets Management

Secrets (database credentials, IdP client secrets, registry pull secrets) are stored as masked GitLab CI variables and injected into the cluster as Kubernetes Secrets by the deploy job. Plaintext secrets are never committed to the repo. If the system grows, External Secrets Operator or Sealed Secrets is the natural next step.

### Readiness Gate

The deploy job calls `kubectl rollout status` after `helm upgrade --install`. This blocks the next stage until pods pass their readiness probes. A `sleep` is not used.

### Rollback Path

`helm rollback identity <previous-revision> -n staging` restores the last known good release.

### Promotion Model

The `deliver` job pushes the image, publishes the chart, and captures the image digest:

```bash
CHART_VERSION=0.1.$CI_PIPELINE_IID

helm package services/$SERVICE_NAME/helm/$SERVICE_NAME \
  --version $CHART_VERSION \
  --app-version $CI_COMMIT_SHORT_SHA

curl --fail-with-body --request POST \
  --user gitlab-ci-token:$CI_JOB_TOKEN \
  --form "chart=@$SERVICE_NAME-$CHART_VERSION.tgz" \
  "$CI_API_V4_URL/projects/$CI_PROJECT_ID/packages/helm/api/$CI_COMMIT_REF_SLUG/charts"

IMAGE_DIGEST=$(docker inspect --format='{{index .RepoDigests 0}}' $IMAGE_TAG | cut -d@ -f2)
echo "IMAGE_DIGEST=$IMAGE_DIGEST" >> deploy.env
```

Both `deploy-staging` and `deploy-production` consume `IMAGE_DIGEST` from the artifact. Production deploys the same digest tested in staging and the same packaged chart version for the pipeline — no rebuild. See [ADR-0011](../docs/adr/0011-image-versioning-branch-sha-with-digest-pinning.md).

---

## 6.6 Integration Smoke Test — Identity Service

The `integration-staging` stage runs `devops-checkpoint/smoke-test/run-smoke-test.sh` against the Identity placeholder deployed in staging.

### What it does

1. Calls `unoarena register --user smoketest-$CI_JOB_ID --pass testpass --json` and asserts the response contains `"status": "ok"`.
2. Calls `unoarena whoami --json` and asserts the response contains `"username": "smoketest-$CI_JOB_ID"`.
3. Cleans up the seeded account on exit (or namespaces it by `CI_JOB_ID` so repeated runs do not collide).

The staging URL is injected as `UNOARENA_API_URL` — never hardcoded.

### Failure semantics

The test fails the pipeline if:
- The Identity placeholder is unreachable.
- The canned response does not match the expected shape.
- The readiness gate was bypassed and pods are not yet healthy (the test would time out and fail explicitly).

One retry (`retry: 1`) is permitted for the `integration-staging` job and is documented as a flake budget, not a silent suppressor.

### Observability seam

The Identity placeholder emits one structured log line per request:

```json
{"level":"info","service":"identity","event":"request","path":"/whoami","correlationId":"..."}
```

An operator can retrieve it via `kubectl logs deployment/identity -n staging`. This log path is the seam where `correlationId` end-to-end tracing would be wired when the real service replaces the placeholder.

---

## 6.9 Service × Pipeline Coverage Matrix

| Service | test | build | deliver | deploy-staging | integration-staging | deliver-prod | deploy-prod | Notes |
|---|---|---|---|---|---|---|---|---|
| Realtime Gateway / BFF | ✅ | ✅ | ✅ | ⬜ | ⬜ | — | — | placeholder; deploy job present but `when: manual` |
| **Identity Service** | ✅ | ✅ | ✅ | ✅ | ✅ | ⬜ optional | ⬜ optional | **fully wired — end-to-end demonstrator** |
| Room Gameplay Service | ✅ | ✅ | ✅ | ⬜ | ⬜ | — | — | placeholder; includes Timer Worker in chart |
| Game Integrity Service | ✅ | ✅ | ✅ | ⬜ | ⬜ | — | — | placeholder |
| Tournament Orchestration Service | ✅ | ✅ | ✅ | ⬜ | ⬜ | — | — | placeholder; includes Provisioning Workers in chart |
| Ranking Service | ✅ | ✅ | ✅ | ⬜ | ⬜ | — | — | placeholder |
| Spectator Projection Service | ✅ | ✅ | ✅ | ⬜ | ⬜ | — | — | placeholder; includes Projection Rebuilders in chart; consumer side of contract check |
| Analytics Service | ✅ | ✅ | ✅ | ⬜ | ⬜ | — | — | placeholder; includes Projection Rebuilders in chart |

⬜ = job present in `.gitlab-ci.yml` but `when: manual` or skipped via `rules:` for this checkpoint.

### Contract Check

The async schema-backed contract check between Room Gameplay (producer) and Spectator View (consumer) on `room.spectator-safe.events` runs in the `test` stage of both services. Both tests load the producer-owned JSON Schema file and enforce the keywords this contract uses: required fields, field types, `sequenceNumber` minimum, and rejected extra top-level properties. A schema change by Room Gameplay that breaks Spectator View's consumer stub fails both `test` jobs and blocks both pipelines. See [ADR-0014](../docs/adr/0014-async-contract-check-room-gameplay-to-spectator-view.md).
