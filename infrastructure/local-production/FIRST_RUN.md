# First-run stateful platform contract

The platform ApplicationSet uses Argo CD RollingSync. Progressive Syncs must be
enabled before the ApplicationSet is applied; `bootstrap-argocd.sh` patches the
official `argocd-cmd-params-cm` key and restarts the controller.

Only CI promotion-created files under
`environments/local-production/platform/enabled/` are watched. The checked-in
root descriptors are disabled publication contracts, not deployable moving-Git
sources. Every enabled copy must name an immutable chart version and every
container image must use a registry digest.

## Ordered health gates

1. `00-foundation`: Retain StorageClass and ESO mappings. Continue only when
   `gitlab-registry`, platform credentials, service-specific workload Secrets,
   and observability Secrets report `ExternalSecret Ready=True`.
2. `10-stateful`: Postgres contexts, Redis, Kafka, KurrentDB, ClickHouse,
   Keycloak, and MinIO. Continue only when every Deployment is Available, every
   PVC is Bound, Kafka answers metadata, and each component-specific readiness
   probe passes.
3. `20-bootstrap`: the immutable bootstrap image runs the four Postgres and one
   ClickHouse migration Jobs. Each Job verifies that the live ledger is an exact
   checksum-matching prefix of the immutable, contiguous history from `001`,
   applies every pending migration in order, and confirms the latest packaged
   version; gaps, rewrites, and drift fail closed.
4. `30-cdc`: Debezium Connect registers exactly four Kafka connectors, then
   Debezium Server starts the Room realtime projection. Continue only when all
   connector/task states are RUNNING and the Server health endpoint is ready.
5. `40-observability`: the packaged observability chart starts Prometheus,
   Alertmanager, Redis exporter, the local webhook sink, logs, traces, and
   dashboards. Continue only when every self-scrape target is up, Prometheus
   loads all rule groups, and the retained Argo PostSync evidence Job proves a
   real Gateway request appears in the Gateway HTTP RED recording output and a
   uniquely labeled Alertmanager alert increases successful notification
   delivery without increasing failures.

RollingSync owns generated Application operations, so the template deliberately
does not configure per-Application automated sync. Stateful PVCs and the local
StorageClass carry both Helm keep and Argo `Prune=false`; stateful Deployments
also refuse Argo pruning. Manual data deletion is a separate operator action.

## First-pipeline prerequisites and promotion

- let CI publish each affected chart as the unique immutable package
  `0.1.<CI_PIPELINE_IID>` to the configured GitLab Helm channel and make the
  channel readable by Argo;
- let CI build and publish `infrastructure/bootstrap`; promotion records its
  real digest and the chart package coordinates before creating the released,
  enabled `context-bootstrap` descriptor;
- generate the non-secret seed template with
  `infrastructure/local-production/bin/render-secret-seed-template`, replace
  every placeholder outside the repository, then use `seed-secrets.sh` to seed
  `secret-seed/uno-arena-local-credentials`. Its required property inventory is
  checked against every local chart mapping and includes
  `internal-principal-hmac-key-current` and
  `internal-principal-hmac-key-previous` (initialize both to the same value),
  MinIO/Grafana values, the in-cluster `alertmanager-webhook-url`, and
  `gitlab-registry-dockerconfigjson`;
- select the KurrentDB package values matching the kind node architecture;
- let promotion update the canonical inventory and create its identical
  released/enabled descriptor copies. RollingSync then advances the platform
  stages, and service release promotion waits for the required platform
  Applications to become Healthy;
- keep the host-local runner on Git/registry/Argo HTTPS only. It receives a
  scoped read-only Argo token and CA, never kubeconfig.

Production does not install these simulator charts. Its contracts under
`environments/production/platform/` require external endpoints, TLS/auth and
semantic preflights before service promotion.
