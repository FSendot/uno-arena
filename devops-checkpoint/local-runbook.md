# Local GitLab Runner and Staging Runbook

This runbook captures the local execution path used for development:
a GitLab project runner on this machine, a local `k8s` cluster, Helm deploys to
the `staging` namespace, and the Identity smoke test through a local
port-forward.

## Prerequisites

- Docker running locally.
- `gitlab-runner` installed and registered to the GitLab project.
- `kind`, `kubectl`, `helm`, `docker`, `bash`, and `curl` available on the
  runner host.
- A GitLab token with `read_registry` permission if the project registry is
  private.

## Runner Selection

Use a project runner with the shell executor for the local checkpoint run. In
GitLab, open the project and go to **Settings -> CI/CD -> Runners**.

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

## Local Kubernetes Cluster

Create or select the local cluster:

```bash
kind create cluster --name uno-arena
kubectl config use-context kind-uno-arena
kubectl get nodes
```

The shell runner uses the host's active `kubectl` context. Before running a
pipeline, verify the active context points at `kind-uno-arena`:

```bash
kubectl config current-context
```

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
repo-owned CLI shim from `devops-checkpoint/smoke-test/unoarena`.

## Running The Pipeline

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
