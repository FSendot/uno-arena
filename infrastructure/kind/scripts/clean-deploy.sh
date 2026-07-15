#!/usr/bin/env bash
# Destructive end-to-end local lane: reset, create, build/load, deploy, and probe.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "${SCRIPT_DIR}/lib.sh"

usage() {
  cat <<'EOF'
Usage:
  clean-deploy.sh --confirm-reset uno-arena [--skip-probes]
  clean-deploy.sh --dry-run [--skip-probes]

The live lane deletes only kind cluster uno-arena. --confirm-reset uno-arena is
mandatory. --dry-run prints the exact plan and performs no Docker, network, or
Kubernetes operation.
EOF
}

dry_run=false
skip_probes=false
confirmation=""
while (($#)); do
  case "$1" in
    --dry-run) dry_run=true; shift ;;
    --skip-probes) skip_probes=true; shift ;;
    --confirm-reset)
      (($# >= 2)) || die "--confirm-reset requires the literal value uno-arena"
      confirmation="$2"
      shift 2
      ;;
    -h|--help) usage; exit 0 ;;
    *) usage >&2; die "unknown argument: $1" ;;
  esac
done

steps=(
  validate.sh
  verify-portable-images.sh
  reset.sh
  preflight-host-capacity.sh
  create-cluster.sh
  install-istio.sh
  build-load-bootstrap.sh
  build-load-services.sh
  load-debezium-connect.sh
  load-debezium-server.sh
  apply.sh
  wait.sh
  deploy-observability.sh
  deploy-services.sh
)
if [[ "${skip_probes}" == false ]]; then
  steps+=(run-live-probes.sh)
fi

if [[ "${dry_run}" == true ]]; then
  for step in "${steps[@]}"; do
    printf 'PLAN %q\n' "${SCRIPT_DIR}/${step}"
  done
  echo "ok kind-clean-deploy dry-run=true steps=${#steps[@]}"
  exit 0
fi

[[ "${confirmation}" == "uno-arena" ]] || die "refusing destructive lane without --confirm-reset uno-arena"
[[ "${KIND_CLUSTER_NAME}" == "uno-arena" ]] || die "clean deployment is pinned to cluster uno-arena"
[[ "${KIND_CONTEXT_NAME}" == "kind-uno-arena" ]] || die "clean deployment is pinned to context kind-uno-arena"

# Fail before deleting the existing disposable cluster when the host has no
# reviewed KurrentDB image or a required deployment tool is absent.
for command in bash curl docker helm kind kubectl ruby; do
  require_cmd "${command}"
done
docker_arch="$(docker info --format '{{.Architecture}}')"
case "${docker_arch}" in
  x86_64|amd64|aarch64|arm64) ;;
  *) die "Docker architecture '${docker_arch}' has no reviewed KurrentDB 26.0.3 image" ;;
esac

for step in "${steps[@]}"; do
  "${SCRIPT_DIR}/${step}"
done

echo "ok kind-clean-deploy cluster=uno-arena probes=$([[ "${skip_probes}" == false ]] && echo run || echo skipped)"
