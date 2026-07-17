# From-scratch local-production deployment with GitLab CI and Argo CD

This is the authoritative procedure for recreating the long-lived local
production simulator from no `uno-arena-production` kind cluster. It stops
first at an **empty but CI-ready** cluster, then documents the separate first
publication.

Empty but CI-ready means the three-node cluster, Gateway API, cert-manager,
External Secrets Operator, metrics-server, Istio Ambient, local CA, Gateway,
Argo core, root/foundation Applications, private Git/Helm credentials, two
healthy but empty ApplicationSets, raw ESO source Secret, GitLab variables, and
runner are ready. No service or stateful-platform release Application exists.
The exact gate is:

~~~bash
./infrastructure/local-production/bin/acceptance.sh --foundation-only
~~~

Full acceptance is intentionally impossible until CI publishes and promotes
the releases.

## 1. Destructive and external boundaries

Review these separately before execution:

| Operation | Effect |
| --- | --- |
| `kind delete cluster --name uno-arena-production` | Permanently deletes the simulator and its local PVC data. |
| Removing `environments/local-production/**/enabled/*.yaml` from remote `main` | Removes release Applications from desired state. |
| `docker pull` or `vendor/fetch.sh` | Downloads pinned inputs. |
| Creating/rotating GitLab tokens, variables, or runners | Mutates the GitLab project. |
| Updating `/etc/hosts` | Uses `sudo` and changes shared host configuration. |
| Starting `run_component=all` | Publishes artifacts and pushes a CI-authored desired-state commit. |

Never run the disposable scripts under `infrastructure/kind` against
`kind-uno-arena-production`.

The checked-in installers verify source artifacts locally, but Kubernetes nodes
still pull referenced OCI images. A fully disconnected bootstrap is not
defined. Allow registry access or preload every referenced image separately.

## 2. Fixed contract and working directory

Run every command from:

~~~bash
cd /Users/martin.zahnd/Documents/microservicios/uno-arena
~~~

| Item | Fixed value |
| --- | --- |
| kind cluster/context | `uno-arena-production` / `kind-uno-arena-production` |
| topology | one control-plane and two workers |
| Docker capacity | at least 8 CPUs; each kind node quota matches Docker's CPU count |
| API cold-start budget | strict readiness; five-minute kube-apiserver liveness failure budget |
| host ports | HTTP 8080, edge TLS 8443, reserved 8444, private Argo TLS 9443 |
| public hostname | `uno-arena.local` |
| kind/node | kind 0.32.0; Kubernetes 1.36.1 digest pinned in `create-cluster.sh` |
| Argo | server and CLI 3.4.5 |
| Istio | 1.30.2 |
| runner | shell executor, tag `local-runner-sendot` |
| deployment target | `local-production` only |

The current checked-in KurrentDB descriptor is the reviewed ARM64 variant for
this Apple Silicon host. On an AMD64 machine, stop before cluster creation: the
repository does not currently define an equivalent exact-image selection for
this procedure.

This procedure uses personal remote `gitlab`
(`git@gitlab.com:FSendot/uno-arena.git`). If the runner and variables live in
`gitlab-catedra`, replace the project URL, numeric project ID, remote, registry
paths, and tokens consistently. Never bootstrap Argo from one project and run
CI in the other.

## 3. External credential bundle

Use the repo-adjacent bundle produced with this runbook:

~~~bash
export UNO_ARENA_CREDENTIAL_DIR='/Users/martin.zahnd/Documents/microservicios/uno-arena-local-production-credentials'
test -d "$UNO_ARENA_CREDENTIAL_DIR"
test "$(ruby -e 'printf "%04o", File.stat(ARGV.fetch(0)).mode & 0777' \
  "$UNO_ARENA_CREDENTIAL_DIR")" = 0700
~~~

Expected files:

| File | Use | Reusable after cluster recreation? |
| --- | --- | --- |
| `gitlab-read-deploy-username` | Deploy-token username | Yes, until rotation/expiry. |
| `gitlab-read-deploy-token` | Argo Git/Helm read and workload registry pull | Yes, until rotation/expiry. |
| `gitlab-access-token` | Candidate `GITOPS_PUSH_TOKEN`; verify `write_repository` | Yes, until rotation/expiry. |
| `gitlab-access-token.source` | Exact `/private/tmp` source snapshot, including its final newline; provenance only | Do not upload this copy. |
| `registry-dockerconfig.json` | Portable embedded Docker authentication | Yes, until deploy-token rotation/expiry. |
| `secret-seed.yaml` | Complete 79-property ESO source Secret | Yes; rotating it changes environment credentials. |
| `current-cluster-local-ca.crt` | Trust for the cluster that produced it | **No** after cluster recreation. |

Every secret file must be mode 0600. The enclosing directory must be 0700.
Do not commit it, attach it to tickets, or paste it into logs.

The operational `gitlab-access-token` is the extracted source token normalized
by removing its one trailing newline. `gitlab-access-token.source` is an exact
byte-for-byte extraction retained only to prove provenance. The temporary seed
manifest was not a usable credential source: 78 of its 79 values were literal
`REPLACE_WITH_SECRET` placeholders. The complete `secret-seed.yaml` was
therefore generated locally; its one non-placeholder registry value came from
the extracted portable Docker configuration. Do not substitute the placeholder
manifest for the generated seed.

Do not back up these as reusable credentials:

- Argo `ci-readonly` token: mint it after every Argo installation;
- Argo admin/session token: keep only long enough to mint the CI token;
- runner authentication token: keep it in runner configuration and rotate it
  if exposed;
- `CI_JOB_TOKEN`, `CI_REGISTRY_USER`, and `CI_REGISTRY_PASSWORD`: GitLab
  creates them per job.

## 4. GitLab project and existing artifacts

In the exact GitLab project, record the numeric **Project ID** and confirm
Container Registry, Package Registry, Pipelines, and project runners are
enabled.

Create or verify:

1. A deploy token with `read_repository`, `read_package_registry`, and
   `read_registry`. Its username/token back the three Argo/pull uses.
2. A project or personal access token with `write_repository` for
   `GITOPS_PUSH_TOKEN`. A deploy token cannot replace this write credential.

Verify scopes, issuer, and expiry in GitLab; an offline token file cannot prove
them.

Existing artifacts do **not** need deletion:

- images use `main-<short SHA>`, while desired state pins the returned digest;
- Helm packages use unique `0.1.<CI_PIPELINE_IID>` versions;
- promotion accepts only metadata for the current source SHA;
- chart verification downloads the package and checks `packageSha256`;
- old GitLab job artifacts are pipeline-scoped;
- shell-runner build/cache directories and Docker build cache are not implicit
  inputs to the generated child pipeline.

If a Helm upload succeeded and its job then failed, start a new pipeline; do
not retry that publish job with the same pipeline IID. Registry deletion is
optional storage maintenance, not bootstrap. Inventory it first through
**Deploy > Container Registry** and **Deploy > Package Registry**.

Do not delete the runner `config.toml` when cleaning runner artifacts.

## 5. Make remote main empty and authoritative

Argo reads remote `main`, not the local working tree:

~~~bash
git fetch gitlab main
git switch main
git merge --ff-only gitlab/main
test -z "$(git status --porcelain)"
git rev-parse HEAD
git rev-parse gitlab/main
~~~

List release descriptors:

~~~bash
git ls-files \
  'environments/local-production/services/enabled/*.yaml' \
  'environments/local-production/platform/enabled/*.yaml'
~~~

This must print nothing for an empty CI-ready bootstrap. If it prints files,
choose explicitly:

- retain them and accept immediate Argo disaster-recovery replay; or
- remove only the reviewed files in a reset commit:

~~~bash
REVIEWED_FILE='replace-with-one-reviewed-path-from-the-list-above'
git rm "$REVIEWED_FILE"
git commit -m "chore(local-production): reset enabled release inventory [skip ci]"
git push gitlab main
~~~

Do not delete images/packages with the descriptors. Confirm current CI and Argo
configuration are on remote main:

~~~bash
git show gitlab/main:.gitlab-ci.yml >/dev/null
git show gitlab/main:environments/local-production/argocd/Chart.yaml >/dev/null
git show gitlab/main:infrastructure/local-production/gitops/foundation/desired.yaml >/dev/null
~~~

## 6. Host and repository preflight

~~~bash
for command in \
  bash base64 curl docker git go helm kind kubectl openssl python3 rg ruby \
  shasum argocd gitlab-runner
do
  command -v "$command" >/dev/null || {
    echo "missing required command: $command" >&2
    exit 1
  }
done
docker info >/dev/null

ruby -Ici/lib ci/test/run.rb
ruby ci/bin/validate-inventories.rb --all
./infrastructure/local-production/vendor/verify.sh
./infrastructure/local-production/gitops/verify-foundation.sh
./infrastructure/local-production/tests/structure.sh
./infrastructure/local-production/tests/foundations.sh
./infrastructure/local-production/tests/platform-charts.sh
./infrastructure/local-production/tests/seed-secrets.sh
./infrastructure/local-production/tests/service-chart-contracts.sh
./infrastructure/local-production/tests/argocd-core-manifest-contracts.sh
./infrastructure/local-production/tests/argocd-control-plane-state-contracts.sh
./infrastructure/local-production/tests/argocd-operator-contracts.sh
~~~

Acquire the exact node image only if absent:

~~~bash
KIND_NODE_IMAGE='kindest/node:v1.36.1@sha256:3489c7674813ba5d8b1a9977baea8a6e553784dab7b84759d1014dbd78f7ebd5'
if ! docker image inspect "$KIND_NODE_IMAGE" >/dev/null 2>&1; then
  docker pull "$KIND_NODE_IMAGE" # explicit network step
fi
~~~

If vendored files fail verification:

~~~bash
./infrastructure/local-production/vendor/fetch.sh # explicit network step
./infrastructure/local-production/vendor/verify.sh
~~~

Before creation, ports must be free:

~~~bash
for port in 8080 8443 8444 9443; do
  if lsof -nP -iTCP:"$port" -sTCP:LISTEN | grep -q .; then
    echo "port $port is already in use" >&2
    lsof -nP -iTCP:"$port" -sTCP:LISTEN
    exit 1
  fi
done
~~~

## 7. Delete and recreate the simulator

This destroys the prior simulator and PVCs:

~~~bash
if kind get clusters 2>/dev/null | grep -qx uno-arena-production; then
  kind delete cluster --name uno-arena-production
fi
! kind get clusters 2>/dev/null | grep -qx uno-arena-production
~~~

Do not delete the separate disposable `uno-arena` cluster.

Create:

~~~bash
./infrastructure/local-production/bin/create-cluster.sh
~~~

Required terminal marker:

~~~text
ok local-production-create context=kind-uno-arena-production nodes=3 workers=2
~~~

Verify:

~~~bash
test "$(kubectl config current-context)" = kind-uno-arena-production
kubectl --context kind-uno-arena-production get nodes
~~~

The helper reuses a same-name cluster. Deletion is therefore required for a
genuine from-scratch proof.

## 8. Install credential-free foundations and Argo core

Use the safer two-phase flow:

~~~bash
./infrastructure/local-production/bin/install-foundations.sh \
  --defer-private-sources
~~~

This verifies vendors, creates namespaces, installs Gateway API, cert-manager,
ESO, metrics-server, Istio Ambient, local secret store, CA/certificates,
Gateway/routes, security policies, filtered Argo core, Progressive Syncs, and
the `ci-readonly` account; it also checks Argo controller DNS/API/Redis and runs
pre-source acceptance.

Required final markers:

~~~text
ok local-production-istio version=1.30.2 context=kind-uno-arena-production
ok local-production-argocd-controller-network context=kind-uno-arena-production
ok local-production-argocd-ci-account context=kind-uno-arena-production
ok local-production-argocd-core context=kind-uno-arena-production
ok local-production-acceptance mode=pre-source context=kind-uno-arena-production
ok local-production-foundations context=kind-uno-arena-production
~~~

If `secret-seed` is missing, rerun foundations. The namespace is
foundation-owned; a not-yet-published platform Application cannot create it.

## 9. Validate, generate, and apply the secret seed

Validate the Docker config without displaying credentials:

~~~bash
ruby -rbase64 -rjson -e '
  config = JSON.parse(File.read(ARGV.fetch(0)))
  abort "credential-store delegation is not portable" if
    config.key?("credsStore") || config.key?("credHelpers")
  auths = config.fetch("auths")
  abort "unrelated registry credentials present" unless
    auths.keys == ["registry.gitlab.com"]
  auth = auths.dig("registry.gitlab.com", "auth")
  username, password = Base64.strict_decode64(auth.to_s).split(":", 2)
  abort "embedded auth missing" if username.to_s.empty? || password.to_s.empty?
  puts "ok portable registry config"
' "$UNO_ARENA_CREDENTIAL_DIR/registry-dockerconfig.json"
~~~

If the seed does not exist, generate it:

~~~bash
./infrastructure/local-production/bin/generate-secret-seed \
  --registry-config "$UNO_ARENA_CREDENTIAL_DIR/registry-dockerconfig.json" \
  --output "$UNO_ARENA_CREDENTIAL_DIR/secret-seed.yaml"
~~~

The generator writes exactly 79 mapped properties, mode 0600, preserves all
username/password/DSN/PgBouncer relationships, initializes current and previous
internal-principal HMAC keys identically, embeds a 256-bit versioned Game
Integrity development key and portable registry JSON, rejects credential-store
delegation/symlinks, and refuses overwrite.

Validate without displaying values:

~~~bash
ruby -rjson -ryaml -e '
  schema = JSON.parse(File.read(ARGV.fetch(0)))
  document = YAML.load_file(ARGV.fetch(1))
  abort "wrong Secret" unless document["apiVersion"] == "v1" &&
    document["kind"] == "Secret" &&
    document.dig("metadata", "namespace") == "secret-seed" &&
    document.dig("metadata", "name") == "uno-arena-local-credentials"
  values = document.fetch("stringData")
  abort "keys differ from schema" unless values.keys == schema.fetch("required")
  abort "placeholder remains" if values.value?("REPLACE_WITH_SECRET")
  abort "empty value remains" if values.values.any?(&:empty?)
  puts "ok complete seed properties=#{values.length}"
' infrastructure/local-production/charts/platform-secrets/seed.schema.json \
  "$UNO_ARENA_CREDENTIAL_DIR/secret-seed.yaml"

test "$(ruby -e 'printf "%04o", File.stat(ARGV.fetch(0)).mode & 0777' \
  "$UNO_ARENA_CREDENTIAL_DIR/secret-seed.yaml")" = 0600
~~~

Apply:

~~~bash
SECRET_SEED_MANIFEST="$UNO_ARENA_CREDENTIAL_DIR/secret-seed.yaml" \
  ./infrastructure/local-production/bin/seed-secrets.sh
~~~

Expected:

~~~text
ok local-production-seed-secrets context=kind-uno-arena-production
~~~

Rerunning this command is the supported update/rotation path.

## 10. Bootstrap private Git, Helm, and the Argo root

Replace the numeric ID:

~~~bash
export GITLAB_PROJECT_ID='<numeric-project-id>'
export ARGOCD_GIT_REPO_URL='https://gitlab.com/FSendot/uno-arena.git'
export ARGOCD_HELM_REPO_URL="https://gitlab.com/api/v4/projects/$GITLAB_PROJECT_ID/packages/helm/stable"
export ARGOCD_GIT_READ_USERNAME="$(<"$UNO_ARENA_CREDENTIAL_DIR/gitlab-read-deploy-username")"
export ARGOCD_GIT_READ_PASSWORD="$(<"$UNO_ARENA_CREDENTIAL_DIR/gitlab-read-deploy-token")"
export ARGOCD_HELM_READ_USERNAME="$ARGOCD_GIT_READ_USERNAME"
export ARGOCD_HELM_READ_PASSWORD="$ARGOCD_GIT_READ_PASSWORD"

./infrastructure/local-production/bin/bootstrap-argocd.sh
unset ARGOCD_GIT_READ_PASSWORD ARGOCD_HELM_READ_PASSWORD
~~~

Expected:

~~~text
ok local-production-bootstrap-argocd context=kind-uno-arena-production
~~~

If remote `main` already contains promoted descriptors, the root may remain
`Progressing` while its bounded RollingSync replay runs. Bootstrap requires the
root and repository-owned foundation to be `Synced`; final acceptance owns the
aggregate health gate and must not be replaced by a fixed root-health timeout.

Gate the empty Git-owned state:

~~~bash
./infrastructure/local-production/bin/acceptance.sh --foundation-only
kubectl --context kind-uno-arena-production -n argocd get applications
~~~

Expected marker:

~~~text
ok local-production-acceptance mode=foundation-only context=kind-uno-arena-production
~~~

Only `uno-arena-local-production-root` and
`uno-arena-local-production-foundations` should exist. Both ApplicationSets
must be healthy but generate no release Applications. The expected missing BFF
backend is allowed. Helm Secret shape is checked here; actual Helm package read
is proved by the first promoted Application.

## 11. Export the new CA

An old-cluster CA is invalid after recreation:

~~~bash
ca_tmp="$(mktemp /private/tmp/uno-arena-local-ca.XXXXXX)"
kubectl --context kind-uno-arena-production -n cert-manager \
  get secret uno-arena-local-ca -o jsonpath='{.data.tls\.crt}' \
  | base64 --decode >"$ca_tmp"
chmod 0600 "$ca_tmp"
mv -f "$ca_tmp" "$UNO_ARENA_CREDENTIAL_DIR/current-cluster-local-ca.crt"
chmod 0600 "$UNO_ARENA_CREDENTIAL_DIR/current-cluster-local-ca.crt"
unset ca_tmp

openssl s_client -connect 127.0.0.1:9443 -verify_ip 127.0.0.1 \
  -verify_return_error \
  -CAfile "$UNO_ARENA_CREDENTIAL_DIR/current-cluster-local-ca.crt" \
  </dev/null
~~~

The same CA signs Argo loopback and the `uno-arena.local` edge certificate.

## 12. Obtain a temporary Argo operator token safely

Read the fresh initial admin password into memory:

~~~bash
argocd_admin_password="$(
  kubectl --context kind-uno-arena-production -n argocd \
    get secret argocd-initial-admin-secret -o jsonpath='{.data.password}' |
    base64 --decode
)"
test -n "$argocd_admin_password"
~~~

Exchange it for a session token without putting the password on the command
line or printing the token:

~~~bash
operator_token_file="$(mktemp /private/tmp/uno-arena-argocd-operator.XXXXXX)"
chmod 0600 "$operator_token_file"

if ! (
  set -o pipefail
  ARGOCD_ADMIN_PASSWORD="$argocd_admin_password" \
    ruby -rjson -e '
      STDOUT.write(JSON.generate({
        "username" => "admin",
        "password" => ENV.fetch("ARGOCD_ADMIN_PASSWORD")
      }))
    ' |
    curl --fail-with-body --silent --show-error \
      --cacert "$UNO_ARENA_CREDENTIAL_DIR/current-cluster-local-ca.crt" \
      --header 'Content-Type: application/json' \
      --data-binary @- \
      https://127.0.0.1:9443/api/v1/session |
    ruby -rjson -e '
      token = JSON.parse(STDIN.read).fetch("token")
      abort "empty token" if token.empty?
      STDOUT.write(token)
    ' >"$operator_token_file"
); then
  rm -f "$operator_token_file"
  unset argocd_admin_password operator_token_file
  echo 'failed to obtain Argo operator token' >&2
  return 1 2>/dev/null || exit 1
fi

unset argocd_admin_password
~~~

Never upload this admin token to GitLab.

## 13. Generate the cluster-specific Argo CI token

~~~bash
argocd_ci_token_file="$UNO_ARENA_CREDENTIAL_DIR/argocd-local-production-ci.token.$(date -u +%Y%m%dT%H%M%SZ)"
test ! -e "$argocd_ci_token_file"

export ARGOCD_OPERATOR_AUTH_TOKEN="$(<"$operator_token_file")"
export ARGOCD_CA_FILE="$UNO_ARENA_CREDENTIAL_DIR/current-cluster-local-ca.crt"
export ARGOCD_CI_TOKEN_OUTPUT="$argocd_ci_token_file"
export ARGOCD_CI_TOKEN_TTL='168h'

./infrastructure/local-production/bin/generate-argocd-ci-token.sh

unset ARGOCD_OPERATOR_AUTH_TOKEN
rm -f "$operator_token_file"
unset operator_token_file
~~~

Expected:

~~~text
ok local-production-argocd-ci-token output=<credential-directory>/argocd-local-production-ci.token.<UTC timestamp>
~~~

The token above expires after seven days. Generation refuses overwrite and
does not revoke older tokens. Keep the file from the destroyed cluster until
the new token has been uploaded to GitLab and the `argocd-preflight` job has
accepted it. That old-cluster token is already invalid because its Argo server
was deleted; after verification, delete its local file. For a same-cluster
rotation, revoke the old token explicitly with an operator session before
deleting its file. Never remove the old credential before its replacement is
verified.

## 14. Configure GitLab variables

In **Settings > CI/CD > Variables**, use environment scope `*`. Because `main`
is unprotected, leave **Protected** off. Mask tokens and disable token variable
expansion.

| Key | Type/value |
| --- | --- |
| `GITOPS_PUSH_TOKEN` | Masked variable; contents of `gitlab-access-token`; verified `write_repository` |
| `GITLAB_USER_EMAIL` | Promotion/rollback commit email |
| `GITLAB_USER_NAME` | Promotion/rollback commit name |
| `ARGOCD_LOCAL_PRODUCTION_SERVER` | `https://127.0.0.1:9443` |
| `ARGOCD_LOCAL_PRODUCTION_AUTH_TOKEN` | Masked variable; contents of the new timestamped `argocd-local-production-ci.token.*` file |
| `ARGOCD_LOCAL_PRODUCTION_CA_FILE` | File variable; new CA contents |
| `POST_DEPLOY_LOCAL_PRODUCTION_CA_FILE` | File variable; same CA contents |
| `POST_DEPLOY_LOCAL_PRODUCTION_GATEWAY_URL` | `https://uno-arena.local:8443/health` |
| `POST_DEPLOY_LOCAL_PRODUCTION_GATEWAY_BASE_URL` | `https://uno-arena.local:8443` |
| `ARGOCD_WAIT_SECONDS` | Optional `600` |

`DEPLOY_ENVIRONMENTS=local-production` is already in the root pipeline. Do not
override it. Do not add smoke users/passwords, kubeconfig, or GitLab’s built-in
job credentials.

Copy tokens without printing:

~~~bash
pbcopy <"$UNO_ARENA_CREDENTIAL_DIR/gitlab-access-token"
# paste GITOPS_PUSH_TOKEN, then:
printf '' | pbcopy

test -f "$argocd_ci_token_file"
pbcopy <"$argocd_ci_token_file"
# paste ARGOCD_LOCAL_PRODUCTION_AUTH_TOKEN, then:
printf '' | pbcopy
~~~

For each File variable, paste/upload the CA certificate; GitLab gives jobs a
temporary file path.

## 15. Configure and start the local runner

Rotate/re-register the runner token if it ever appeared in diagnostics, logs,
history, or screenshots.

Required GitLab runner settings:

- executor `shell`;
- tag `local-runner-sendot`;
- run untagged jobs disabled;
- Protected disabled while `main` is unprotected;
- locked to this project;
- concurrency one for the first full publication.

A normal Docker executor sees its own loopback and cannot reach host
`127.0.0.1:9443`/`8443`.

Register if necessary:

~~~bash
gitlab-runner register
~~~

Enter GitLab URL `https://gitlab.com`, the new runner authentication token,
description `uno-arena-local-production`, tag `local-runner-sendot`, and
executor `shell`. Do not run commands that print runner authentication
metadata. Verify only:

~~~bash
gitlab-runner verify
for command in bash curl docker git go helm openssl python3 ruby; do
  command -v "$command" >/dev/null || exit 1
done
docker version
~~~

Configure hostname resolution:

~~~bash
grep -Eq '^[[:space:]]*127\.0\.0\.1[[:space:]]+.*uno-arena\.local' /etc/hosts ||
  echo '127.0.0.1 uno-arena.local' | sudo tee -a /etc/hosts >/dev/null
dscacheutil -q host -a name uno-arena.local
~~~

Start one way:

~~~bash
gitlab-runner run                 # foreground first proof
# or:
brew services start gitlab-runner
~~~

Confirm GitLab reports the tagged runner online. It must receive no kubeconfig.

## 16. Prove and stop at empty CI-ready

~~~bash
./infrastructure/local-production/bin/acceptance.sh --foundation-only
kubectl --context kind-uno-arena-production -n argocd get applications
kubectl --context kind-uno-arena-production -n argocd get applicationsets \
  uno-arena-local-production-services uno-arena-local-production-platform
kubectl --context kind-uno-arena-production -n secret-seed \
  get secret uno-arena-local-credentials
~~~

Stop here if the requested endpoint is empty but ready. Do not start CI yet.

## 17. First publication through GitLab CI

Ensure no competing publication pipeline exists. If validated local commits
are intentionally ahead, push with CI skipped so manual all is the controlled
publication:

~~~bash
git fetch gitlab main
git status --short
git log --oneline gitlab/main..HEAD
git push -o ci.skip gitlab HEAD:main
~~~

In GitLab:

1. **Build > Pipelines > New pipeline**.
2. Select `main`.
3. Leave `run_component` at `all`.
4. Do not add `RUN_COMPONENT` separately.
5. Start.

The parent intentionally shows `plan:impact` and `dispatch:impact`.
Open the downstream pipeline linked from dispatch to see:

~~~text
validate -> test/build -> publish -> promote -> reconcile -> verify
~~~

Success must reach `promote:desired-state`, `reconcile:wait`, and
`verify:post-deploy`. Publication alone is not success. The promotion job
pushes a `[skip ci]` desired-state commit, and Argo creates Applications in
RollingSync order: secrets/foundation, stateful stores, schema bootstrap, CDC,
observability, then services/dependent verification. Within the platform
ApplicationSet, each stage advances one Application at a time and waits for
Healthy before starting the next. The service ApplicationSet independently
does the same, so a disaster-recovery replay cannot launch an unbounded service
wave; at most one platform and one service Application reconcile concurrently.

## 18. Final verification

~~~bash
git fetch gitlab main
git log --oneline HEAD..gitlab/main
git merge --ff-only gitlab/main

kubectl --context kind-uno-arena-production -n argocd get applications \
  -o custom-columns='NAME:.metadata.name,SYNC:.status.sync.status,HEALTH:.status.health.status'
~~~

Run full acceptance with all 21 first-publication Applications:

~~~bash
export EXPECTED_ARGO_APPLICATIONS='uno-arena-local-production-analytics,uno-arena-local-production-clickhouse,uno-arena-local-production-context-bootstrap,uno-arena-local-production-debezium-connect,uno-arena-local-production-debezium-server,uno-arena-local-production-game-integrity,uno-arena-local-production-gateway,uno-arena-local-production-identity,uno-arena-local-production-kafka,uno-arena-local-production-keycloak,uno-arena-local-production-kurrentdb,uno-arena-local-production-local-retain-storage,uno-arena-local-production-minio,uno-arena-local-production-observability,uno-arena-local-production-platform-secrets,uno-arena-local-production-postgres-contexts,uno-arena-local-production-ranking,uno-arena-local-production-redis,uno-arena-local-production-room-gameplay,uno-arena-local-production-spectator-view,uno-arena-local-production-tournament-orchestration'
./infrastructure/local-production/bin/acceptance.sh
unset EXPECTED_ARGO_APPLICATIONS
~~~

Expected:

~~~text
ok local-production-acceptance mode=full context=kind-uno-arena-production
~~~

## 19. Rerun rules

| Step | Behavior |
| --- | --- |
| Cluster creation | Reuses same-name cluster; delete first for a true empty proof. |
| Foundations | Apply/upgrade-convergent but restarts/patches controllers; after adoption prefer Git reconciliation. |
| Seed generation | Refuses overwrite; reuse or rotate deliberately. |
| Seed apply | Supported update/rotation path after full validation. |
| Argo bootstrap | Reinstalls/restarts core/repo-server and replaces repo Secrets; use for bootstrap/repair. |
| CA export | Mandatory after every cluster recreation. |
| CI token | Cluster-specific, expiring, refuses overwrite; replace GitLab variable on renewal. |
| Foundation acceptance | Read-only; repeat before publication. |
| Manual all | New Helm version per new pipeline; never retry a partially successful Helm upload in the same pipeline. |
| Full acceptance | Read-only; repeat after releases exist. |

## Appendix A: all 79 seed properties

Fixed/compound:

~~~text
alertmanager-webhook-url
game-integrity-envelope-dev-keys
gitlab-registry-dockerconfigjson
identity-database-url
ranking-database-url
room-database-url
room-pgbouncer-database-url
room-pgbouncer-userlist
tournament-database-url
internal-principal-hmac-key-current
internal-principal-hmac-key-previous
~~~

Database, CDC, and platform identities:

~~~text
identity-admin-user
identity-admin-password
identity-bootstrap-user
identity-bootstrap-password
identity-runtime-user
identity-runtime-password
identity-cdc-user
identity-cdc-password
room-admin-user
room-admin-password
room-bootstrap-user
room-bootstrap-password
room-runtime-user
room-runtime-password
room-cdc-kafka-user
room-cdc-kafka-password
room-cdc-realtime-user
room-cdc-realtime-password
tournament-admin-user
tournament-admin-password
tournament-bootstrap-user
tournament-bootstrap-password
tournament-runtime-user
tournament-runtime-password
tournament-cdc-user
tournament-cdc-password
ranking-admin-user
ranking-admin-password
ranking-bootstrap-user
ranking-bootstrap-password
ranking-runtime-user
ranking-runtime-password
ranking-cdc-user
ranking-cdc-password
analytics-bootstrap-user
analytics-bootstrap-password
analytics-runtime-user
analytics-runtime-password
clickhouse-admin-user
clickhouse-admin-password
grafana-admin-user
grafana-admin-password
keycloak-admin-user
keycloak-admin-password
minio-access-key-id
minio-secret-access-key
~~~

Internal, cursor, and operational credentials:

~~~text
analytics-ops-credential
analytics-ranking-credential
analytics-room-credential
analytics-tournament-credential
game-integrity-audit-credential
game-integrity-internal-credential
identity-internal-credential
ranking-analytics-backfill-cursor-secret
ranking-internal-credential
ranking-leaderboard-cursor-secret
room-analytics-backfill-cursor-secret
room-identity-credential
room-public-list-cursor-secret
room-runtime-router-credential
room-service-credential
room-spectator-recovery-service-credential
room-timer-service-credential
room-to-gateway-credential
spectator-view-internal-credential
tournament-analytics-backfill-cursor-secret
tournament-bracket-cursor-secret
tournament-internal-credential
~~~

The two HMAC keys start equal. During rotation, move old current to previous and
generate a new current. The schema and `generate-secret-seed` remain the
machine-readable authority.

## Appendix B: fail-closed checkpoints

- Cluster creation refuses a missing pinned node image.
- Vendor checksum mismatch blocks foundations.
- Mutating helpers refuse a current context other than
  `kind-uno-arena-production`.
- Pre-source acceptance does not require private repository Secrets.
- Foundation-only acceptance requires Git ownership and healthy generators.
- Seed validation rejects missing, empty, placeholder, malformed, wrong-name,
  wrong-namespace, and wrong-context inputs before apply.
- Empty state cannot prove Helm package read; first promoted chart does.
- CI uses the read-only Argo API token and no kubeconfig.
- Post-deploy fails on bad Argo trees, Gateway health, client parity,
  observability evidence, or chart checksum.
