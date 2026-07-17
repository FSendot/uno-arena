#!/usr/bin/env bash
# Explicit/NETWORKED: node-native stage of Debezium Connect (multiarch index) into kind.
# Uses docker exec + crictl pull on the uno-arena control-plane node.
# Does NOT use kind load (unreliable for these upstream OCI/multiarch images here).
# Does not claim Postgres→Kafka delivery — only stages the Connect image for kind-apply.
# Workloads reference the exact quay.io digest with imagePullPolicy Never (no runtime pull).
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "${SCRIPT_DIR}/lib.sh"

require_cmd docker
require_cmd kind
require_cmd python3
assert_kind_cluster_name
assert_kind_context

DEBEZIUM_CONNECT_VERSION="${DEBEZIUM_CONNECT_VERSION:-3.6.0.Final}"
DEBEZIUM_CONNECT_INDEX_DIGEST="${DEBEZIUM_CONNECT_INDEX_DIGEST:-sha256:8b6267563ceb0cbfe2c3aa5521c4653cbb8bab9d5042e609f2771283f906bada}"
DEBEZIUM_CONNECT_SOURCE_IMAGE="${DEBEZIUM_CONNECT_SOURCE_IMAGE:-quay.io/debezium/connect:3.6.0.Final@sha256:8b6267563ceb0cbfe2c3aa5521c4653cbb8bab9d5042e609f2771283f906bada}"

# Prior broken local runtime tag from kind-load staging (remove so crictl cannot reuse bad metadata).
DEBEZIUM_CONNECT_STALE_RUNTIME_TAG="docker.io/uno-arena/debezium-connect:3.6.0.Final-b7ca129320f4"

if [[ "${DEBEZIUM_CONNECT_SOURCE_IMAGE}" != *"@sha256:"* ]]; then
  echo "error: source image must be digest-qualified (got ${DEBEZIUM_CONNECT_SOURCE_IMAGE})" >&2
  exit 1
fi
if [[ "${DEBEZIUM_CONNECT_SOURCE_IMAGE}" != quay.io/debezium/connect:* ]]; then
  echo "error: source image must be quay.io/debezium/connect (got ${DEBEZIUM_CONNECT_SOURCE_IMAGE})" >&2
  exit 1
fi

# Exactly one control-plane node for cluster uno-arena.
_cp_nodes=()
while IFS= read -r _n; do
  [[ -n "${_n}" ]] || continue
  _cp_nodes+=("${_n}")
done < <(kind get nodes --name "${KIND_CLUSTER_NAME}" | grep 'control-plane' || true)
if [[ "${#_cp_nodes[@]}" -ne 1 ]]; then
  echo "error: expected exactly one control-plane node for cluster ${KIND_CLUSTER_NAME}, got ${#_cp_nodes[@]}: ${_cp_nodes[*]:-none}" >&2
  exit 1
fi
KIND_NODE="${_cp_nodes[0]}"
echo "kind node ${KIND_NODE} (cluster ${KIND_CLUSTER_NAME})"
node_arch="$(docker exec "${KIND_NODE}" uname -m)"
case "${node_arch}" in
  x86_64|amd64) expected_arch="amd64" ;;
  aarch64|arm64) expected_arch="arm64" ;;
  *) echo "error: unsupported kind node architecture ${node_arch}" >&2; exit 1 ;;
esac

node_rmi_tolerate() {
  local ref="$1"
  if docker exec "${KIND_NODE}" crictl rmi "${ref}" >/dev/null 2>&1; then
    echo "removed node image ${ref}"
  else
    echo "note: node image ${ref} absent or already gone (ok)"
  fi
}

# Clear known Connect refs only — do not touch other images.
echo "clearing prior Connect refs on node (tolerate absent)"
node_rmi_tolerate "${DEBEZIUM_CONNECT_STALE_RUNTIME_TAG}"
node_rmi_tolerate "${DEBEZIUM_CONNECT_SOURCE_IMAGE}"

echo "node-native crictl pull ${DEBEZIUM_CONNECT_SOURCE_IMAGE}"
docker exec "${KIND_NODE}" crictl pull "${DEBEZIUM_CONNECT_SOURCE_IMAGE}"

echo "verifying ${expected_arch} resolution + exact multiarch index via crictl inspecti (fail-closed on kind-import aliases)"
DIGEST_HEX="${DEBEZIUM_CONNECT_INDEX_DIGEST#sha256:}"
docker exec "${KIND_NODE}" crictl inspecti "${DEBEZIUM_CONNECT_SOURCE_IMAGE}" | python3 -c "
import json, sys
want_digest = sys.argv[1]
want_ref = sys.argv[2]
expected_arch = sys.argv[3]
data = json.load(sys.stdin)
status = data.get('status') or {}
info = data.get('info') or {}
spec = info.get('imageSpec') or info.get('image') or {}
arch = (spec.get('architecture') or spec.get('Architecture') or '').lower()
if arch != expected_arch:
    raise SystemExit('architecture must be %s, got %r' % (expected_arch, arch))
repo_digests = status.get('repoDigests') or []
# Prior kind load poisons the node: even after scoped rmi, a native pull can reconstruct
# docker.io/library/import-* repoDigests alongside the valid quay digest. Kubelet then
# follows the missing import alias and fails container creation. Do not attempt
# containerd content surgery — reset/recreate this disposable cluster instead.
stale_imports = [
    d for d in repo_digests
    if d.lower().startswith('docker.io/library/import-') or '/import-' in d.lower()
]
if stale_imports:
    raise SystemExit(
        'node contains stale kind-import metadata in repoDigests (%s); '
        'reset/recreate the disposable kind cluster, then rerun the node-native loader '
        '(do not attempt containerd content surgery)' % (stale_imports,)
    )
repo_tags = status.get('repoTags') or []
image_id = (status.get('id') or '').lower()
blob = ' '.join(list(repo_digests) + list(repo_tags) + [image_id, want_ref]).lower()
if want_digest.lower() not in blob and ('sha256:' + want_digest).lower() not in blob:
    raise SystemExit('exact digest %s not found in inspecti status' % want_digest)
if repo_digests and not any(want_digest.lower() in d.lower() for d in repo_digests):
    raise SystemExit('repoDigests missing exact digest %s: %s' % (want_digest, repo_digests))
# Exact source ref must resolve (inspecti already keyed by want_ref); require quay digest form when digests listed.
if not any('quay.io/debezium/connect' in d and want_digest.lower() in d.lower() for d in repo_digests) and want_digest.lower() not in image_id:
    raise SystemExit('exact source digest/ref not available for %s' % want_ref)
print('ok inspecti arch=%s index=%s' % (expected_arch, want_digest))
" "${DIGEST_HEX}" "${DEBEZIUM_CONNECT_SOURCE_IMAGE}" "${expected_arch}"

echo "ok node-native-stage-debezium-connect source=${DEBEZIUM_CONNECT_SOURCE_IMAGE} node=${KIND_NODE}"
echo "note: image stage only — run make kind-apply (after bootstrap) then make kind-test-debezium-connectors for live status"
