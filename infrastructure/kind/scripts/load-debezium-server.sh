#!/usr/bin/env bash
# Explicit/NETWORKED: node-native stage of Debezium Server 3.6.0.Final (ARM64 digest) into kind.
# Uses docker exec + crictl pull on the uno-arena control-plane node.
# Does NOT use kind load (unreliable for these upstream OCI/multiarch images here).
# Does not apply manifests or claim CDC delivery.
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

# Must match infrastructure/kind/manifests/80-debezium-server Deployment image.
# Approved local ARM64 source digest for 3.6.0.Final
# (multiarch index sha256:d70982832b59186e364ab616fee3f5aec84d419dea14f18df354b55ac0dd1984).
DEBEZIUM_SERVER_VERSION="${DEBEZIUM_SERVER_VERSION:-3.6.0.Final}"
DEBEZIUM_SERVER_ARM64_DIGEST="${DEBEZIUM_SERVER_ARM64_DIGEST:-sha256:65ba00e8de90c437fa4a3b34c6904b1f0235702350e1794006f774b7df1b826b}"
DEBEZIUM_SERVER_SOURCE_IMAGE="${DEBEZIUM_SERVER_SOURCE_IMAGE:-quay.io/debezium/server:3.6.0.Final@sha256:65ba00e8de90c437fa4a3b34c6904b1f0235702350e1794006f774b7df1b826b}"

# Prior broken local runtime tag from kind-load staging (remove so crictl cannot reuse bad metadata).
DEBEZIUM_SERVER_STALE_RUNTIME_TAG="docker.io/uno-arena/debezium-server:3.6.0.Final-65ba00e8de90"

if [[ "${DEBEZIUM_SERVER_SOURCE_IMAGE}" != *"@sha256:"* ]]; then
  echo "error: source image must be digest-qualified (got ${DEBEZIUM_SERVER_SOURCE_IMAGE})" >&2
  exit 1
fi
if [[ "${DEBEZIUM_SERVER_SOURCE_IMAGE}" != quay.io/debezium/server:* ]]; then
  echo "error: source image must be quay.io/debezium/server (got ${DEBEZIUM_SERVER_SOURCE_IMAGE})" >&2
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

node_rmi_tolerate() {
  local ref="$1"
  if docker exec "${KIND_NODE}" crictl rmi "${ref}" >/dev/null 2>&1; then
    echo "removed node image ${ref}"
  else
    echo "note: node image ${ref} absent or already gone (ok)"
  fi
}

# Clear known Server refs only — do not touch other images.
echo "clearing prior Server refs on node (tolerate absent)"
node_rmi_tolerate "${DEBEZIUM_SERVER_STALE_RUNTIME_TAG}"
node_rmi_tolerate "${DEBEZIUM_SERVER_SOURCE_IMAGE}"

echo "node-native crictl pull ${DEBEZIUM_SERVER_SOURCE_IMAGE}"
docker exec "${KIND_NODE}" crictl pull "${DEBEZIUM_SERVER_SOURCE_IMAGE}"

echo "verifying arm64 + exact source digest via crictl inspecti (fail-closed on kind-import aliases)"
DIGEST_HEX="${DEBEZIUM_SERVER_ARM64_DIGEST#sha256:}"
docker exec "${KIND_NODE}" crictl inspecti "${DEBEZIUM_SERVER_SOURCE_IMAGE}" | python3 -c "
import json, sys
want_digest = sys.argv[1]
want_ref = sys.argv[2]
data = json.load(sys.stdin)
status = data.get('status') or {}
info = data.get('info') or {}
spec = info.get('imageSpec') or info.get('image') or {}
arch = (spec.get('architecture') or spec.get('Architecture') or '').lower()
if arch != 'arm64':
    raise SystemExit('architecture must be arm64, got %r' % (arch,))
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
if not any('quay.io/debezium/server' in d and want_digest.lower() in d.lower() for d in repo_digests) and want_digest.lower() not in image_id:
    raise SystemExit('exact source digest/ref not available for %s' % want_ref)
print('ok inspecti arch=arm64 digest=%s' % want_digest)
" "${DIGEST_HEX}" "${DEBEZIUM_SERVER_SOURCE_IMAGE}"

echo "ok node-native-stage-debezium-server source=${DEBEZIUM_SERVER_SOURCE_IMAGE} node=${KIND_NODE}"
