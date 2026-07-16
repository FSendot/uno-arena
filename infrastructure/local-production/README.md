# Local production simulator foundations

This directory owns the reproducible platform foundation for the long-lived
`uno-arena-production` kind cluster. It is separate from the disposable
acceptance cluster under `infrastructure/kind`.

## Guardrails

- Every mutating command uses the exact active context
  `kind-uno-arena-production`; no script switches context.
- The cluster is one control-plane plus two explicitly labeled workers.
- Host ports are loopback-only: HTTP `8080`, application TLS `8443`, reserved
  local service port `8444`, and private Argo API TLS `9443`.
- The only public Gateway API backend is the BFF service named `gateway`.
  `http://uno-arena.local:8080` redirects to
  `https://uno-arena.local:8443`.
- Argo is not routed through the application Gateway. Its fixed NodePort 30445
  is mapped only to `127.0.0.1:9443` and uses a cert-manager certificate valid
  for that IP.
- Platform artifacts come only from the pinned official URLs and SHA-256 values
  in `vendor/sources.lock.tsv`. Installers are offline-only and fail closed.
- Secret values never belong in Git. ESO may read Secrets only from
  `secret-seed` through its dedicated service account and Role.
- `--kubelet-insecure-tls` is applied only to metrics-server in this kind
  profile, because kind node certificates do not include the routable node-IP
  SANs. It is forbidden in a real production overlay.
- `uno-arena` and `observability` use Istio Ambient, strict mTLS, and
  workload/service-account-specific L4 policies.

## Acquisition and offline checks

Artifact acquisition is an explicit networked operator step. See
`vendor/README.md`. The checked-in copies can be verified without network:

```bash
./infrastructure/local-production/vendor/verify.sh
./infrastructure/local-production/tests/structure.sh
./infrastructure/local-production/tests/foundations.sh
./infrastructure/local-production/tests/platform-charts.sh
./infrastructure/local-production/tests/seed-secrets.sh
./infrastructure/local-production/tests/service-chart-contracts.sh
./infrastructure/local-production/tests/argocd-operator-contracts.sh
```

## Install

Create the exact cluster first. When the private GitLab sources or credentials
are not available yet, install the credential-free foundation and Argo core:

```bash
./infrastructure/local-production/bin/create-cluster.sh
./infrastructure/local-production/bin/install-foundations.sh --defer-private-sources
```

That path ends with `acceptance.sh --pre-source`. It verifies the cluster,
Istio, TLS, private loopback Argo endpoint, Progressive Syncs, and scoped CI
account without requiring repository Secrets or Git-backed Applications. The
imperatively installed foundation is a one-time pre-source bridge; once the
private repository is configured, the root-owned foundation Application adopts
the exact generated state in `gitops/foundation/desired.yaml` and self-heals it.

When the sources are ready, supply separate read-only credentials for Git and
the Helm package registry, then bootstrap the private repositories and Argo
inventory. The helper sends Secret documents directly to `kubectl`; it does not
print credentials or write them to repository files. Prefer project deploy
tokens with only `read_repository` for Git and `read_package_registry` for Helm.
One deploy token may back both inputs when it has both scopes, but the Argo
Secrets stay separate:

```bash
export ARGOCD_GIT_REPO_URL='https://gitlab.com/FSendot/uno-arena.git'
export ARGOCD_HELM_REPO_URL='https://gitlab.com/api/v4/projects/PROJECT_ID/packages/helm/stable'
export ARGOCD_GIT_READ_USERNAME='git-read-deploy-token-username'
read -r -s -p 'Git read token: ' ARGOCD_GIT_READ_PASSWORD; echo
export ARGOCD_GIT_READ_PASSWORD
export ARGOCD_HELM_READ_USERNAME='helm-read-deploy-token-username'
read -r -s -p 'Helm read token: ' ARGOCD_HELM_READ_PASSWORD; echo
export ARGOCD_HELM_READ_PASSWORD
./infrastructure/local-production/bin/bootstrap-argocd.sh
./infrastructure/local-production/bin/acceptance.sh --foundation-only

unset ARGOCD_GIT_READ_PASSWORD ARGOCD_HELM_READ_PASSWORD
```

If the credentials are already available, running `install-foundations.sh`
without `--defer-private-sources` performs both phases and ends with
`acceptance.sh --foundation-only`. Foundation-only acceptance validates the
Git and Helm repository Secret structure, the root and foundation Applications,
all four narrowly scoped AppProjects, both ApplicationSets, and the health of
the Git-backed generators. That proves private Git access and repository
ownership of the post-bootstrap control plane. It
does not prove that a Helm package can be fetched; the first promoted Application
that references an immutable chart version is the Helm connectivity check.
Follow `FIRST_RUN.md` to publish platform artifacts, seed
`secret-seed/uno-arena-local-credentials`, and enable/reconcile the ordered
platform inventory before running full acceptance.

Generate the seed manifest outside the repository from the non-secret mapped
property inventory, replace every placeholder locally, apply it through the
guarded helper, and remove the temporary file:

```bash
umask 077
seed_manifest="$(mktemp /private/tmp/uno-arena-local-credentials.XXXXXX)"
./infrastructure/local-production/bin/render-secret-seed-template >"${seed_manifest}"
"${EDITOR:?set EDITOR before editing the seed manifest}" "${seed_manifest}"
SECRET_SEED_MANIFEST="${seed_manifest}" \
  ./infrastructure/local-production/bin/seed-secrets.sh
rm -f "${seed_manifest}"
unset seed_manifest
```

The seed helper requires the exact `secret-seed/uno-arena-local-credentials`
Secret and rejects missing chart-mapped properties, empty values, generated
placeholders, malformed `data`, and the wrong cluster context before applying
anything. Never create the seed manifest inside the repository or commit it.

Replace `PROJECT_ID` with the numeric project ID. Bootstrap refuses the
checked-in `example.invalid` placeholders and validates the fixed
`environments/local-production/services/enabled/*.yaml` generator path before
applying the rendered inventory. Promotion creates reviewed files in that
directory; disabled examples outside it cannot generate Applications.

The local CA is intentionally private to this simulator. A host operator can
add `uno-arena.local` to `/etc/hosts` as `127.0.0.1` and export the public CA
certificate for local clients. Do not commit the exported file.

## Host-local GitLab runner to Argo

The runner must not receive Kubernetes credentials or the kind kubeconfig. Its
deployment path is GitOps: publish immutable image/chart artifacts, update the
reviewed Git inventory, and optionally use only Argo's HTTPS API for read-only
status checks.

An operator—not the runner—exports only the public CA certificate:

```bash
kubectl --context kind-uno-arena-production -n cert-manager \
  get secret uno-arena-local-ca -o jsonpath='{.data.tls\.crt}' \
  | base64 --decode > uno-arena-local-ca.crt

openssl s_client -connect 127.0.0.1:9443 -verify_ip 127.0.0.1 \
  -verify_return_error -CAfile uno-arena-local-ca.crt </dev/null
```

Bootstrap enables a local `ci-readonly` Argo API account. Its only RBAC policies
are `applications,get` for the read-only bootstrap, foundation, workload, and
platform projects: `uno-arena-bootstrap/*`, `uno-arena-foundations/*`,
`uno-arena-workloads/*`, and `uno-arena-stateful-platform/*`. It cannot sync,
update, or delete Applications.
After obtaining a short-lived operator/admin Argo token by the normal Argo
bootstrap procedure, generate the CI account token into a new
mode-0600 file. The helper uses the loopback endpoint and normal CA validation;
it never passes either token as a command-line argument or prints either token:

```bash
# Requires the Argo CD 3.4.5 CLI to match the installed server.
read -r -s -p 'Operator Argo token: ' ARGOCD_OPERATOR_AUTH_TOKEN; echo
export ARGOCD_OPERATOR_AUTH_TOKEN
export ARGOCD_CA_FILE="$PWD/uno-arena-local-ca.crt"
export ARGOCD_CI_TOKEN_OUTPUT="$PWD/argocd-local-production-ci.token"
# Optional; positive minutes or hours. Defaults to 168h.
export ARGOCD_CI_TOKEN_TTL='168h'
./infrastructure/local-production/bin/generate-argocd-ci-token.sh
unset ARGOCD_OPERATOR_AUTH_TOKEN
```

Upload the token file contents once as a masked GitLab variable; while `main`
remains unprotected, leave the variable's **Protected** checkbox off. Then
remove the local token file. Renew it before its configured expiry through the
same operator-only flow; the helper refuses to overwrite an existing token file.

For a local-production-only pipeline, configure these GitLab CI/CD variables:

`main` is intentionally unprotected for the current dev-speed workflow, so
leave GitLab's **Protected** checkbox off for every variable below; otherwise
main pipelines cannot receive it. Keep the two token values **Masked**. When
branch protection is enabled later, protect the deployment variables at the
same time.

- `DEPLOY_ENVIRONMENTS=local-production`; the root pipeline defaults to this
  simulator-only target. Selecting `production` or
  `production,local-production` currently fails closed during impact planning:
  production remains blocked until external platform contracts have executable
  CI preflight evidence, and schema changes additionally require a production
  migration-ordering gate. Empty, duplicate, or unknown entries also fail
  before publication;
- `GITOPS_PUSH_TOKEN`: masked token with repository write authority;
- `GITLAB_USER_EMAIL` and `GITLAB_USER_NAME`: identity for promotion and
  rollback commits;
- `ARGOCD_LOCAL_PRODUCTION_SERVER=https://127.0.0.1:9443`;
- `ARGOCD_LOCAL_PRODUCTION_AUTH_TOKEN`: masked `ci-readonly` account
  token;
- `ARGOCD_LOCAL_PRODUCTION_CA_FILE`: **File** variable containing
  `uno-arena-local-ca.crt`;
- `POST_DEPLOY_LOCAL_PRODUCTION_CA_FILE`: **File** variable containing
  the local edge CA;
- `POST_DEPLOY_LOCAL_PRODUCTION_GATEWAY_URL=https://uno-arena.local:8443/health`:
  exact Gateway health endpoint;
- `POST_DEPLOY_LOCAL_PRODUCTION_GATEWAY_BASE_URL=https://uno-arena.local:8443`:
  public BFF origin used by the fixed live client-parity profiles.

`ARGOCD_WAIT_SECONDS` is optional and defaults to 600. Do not configure static
smoke usernames or passwords: the live parity suite self-registers unique users
through the public BFF. No separate package-registry credential is required;
the verification job re-fetches each reconciled
`<chart.repository>/charts/<name>-<version>.tgz` with its job-scoped
`CI_JOB_TOKEN` and compares the bytes with the promoted
`chart.packageSha256`.

Internal services and platform components are not exposed for probing: the
pipeline binds their evidence to the exact affected Argo CD Application and
requires its resource tree to be non-empty and free of unhealthy resources.
Any affected service also runs the repository's fixed
`client-checkpoint/tests/run-live-client-parity.sh` profiles against the public
BFF; the Gateway additionally proves the exact HTTPS `/health` endpoint. An
affected observability Application must expose the chart-owned exact
`batch/Job` named `uno-arena-observability-postsync-evidence` in its Argo
resource tree with Healthy and completed/succeeded evidence. Missing or failed
evidence blocks deployment; no external observability evidence URL variables
are used.

The GitLab runner must carry tag `local-runner-sendot`, resolve
`uno-arena.local` to `127.0.0.1`, and have the repository's existing Ruby,
Docker, Helm, and curl prerequisites. It receives no kubeconfig. Never disable
TLS verification.

Final operator acceptance may declare the exact promoted Applications that must
already be reconciled:

```bash
export EXPECTED_ARGO_APPLICATIONS='uno-arena-local-production-platform-secrets,uno-arena-local-production-gateway,uno-arena-local-production-identity'
./infrastructure/local-production/bin/acceptance.sh
```

When that variable is explicitly set, an empty list or any Application that is
not Synced and Healthy fails acceptance. Acceptance also always requires
structurally valid private Git/Helm repository Secrets, healthy Git-backed
ApplicationSets, the cross-project `ci-readonly` account policy, and the
`gitlab-registry` Docker config Secret produced by the platform secret inventory.
A promoted Helm Application must become Synced and Healthy to prove package
registry connectivity.

## Scope left for environment inventory

This foundation does not commit credentials, immutable service releases,
stateful simulator releases, or observability workloads. It does commit the
exact pinned cluster-operator/foundation desired state that Argo adopts after
bootstrap. The environment inventory and delivery pipeline still own workload
resources and the ordered Helm values
`values.production.yaml`, then `values.local-production.yaml`.

The observability chart no longer renders a namespace-wide application allow.
Application and local platform traffic are admitted only by the exact
service-account and destination-port policies in `manifests/40-ambient-security.yaml`.
