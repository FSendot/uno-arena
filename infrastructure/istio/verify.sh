#!/usr/bin/env bash
# Offline integrity and Helm rendering checks for vendored Istio 1.30.2 charts.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
HELM="${HELM:-helm}"

command -v shasum >/dev/null 2>&1 || { echo "shasum required" >&2; exit 1; }
command -v "${HELM}" >/dev/null 2>&1 || { echo "helm required" >&2; exit 1; }

aggregate() {
  local chart="$1"
  (cd "${ROOT}" && find "charts/${chart}" -type f -print0 | LC_ALL=C sort -z | xargs -0 shasum -a 256 | shasum -a 256 | awk '{print $1}')
}

declare -A expected=(
  [base]=c7ba96fff51bdaf2f875adb68510965ecf3c66432d1a773d4163f7bcd90531b4
  [istiod]=8c8fbe372281b0287d1ce6e3a4b7e0c1f8b74bbd11595df090224567a6c1355c
  [cni]=df7b368ee1b9bf1e7aab0e6992f013eb044a6316e0f390601bcd49285a06ab83
  [ztunnel]=26bb678de639fbc0262f903a3db13fdf862eea122c7ed870a471632f92137c07
)

for chart in base istiod cni ztunnel; do
  actual="$(aggregate "${chart}")"
  [[ "${actual}" == "${expected[${chart}]}" ]] || {
    echo "Istio chart integrity mismatch: ${chart}: ${actual}" >&2
    exit 1
  }
  grep -q '^version: 1.30.2$' "${ROOT}/charts/${chart}/Chart.yaml"
  "${HELM}" lint "${ROOT}/charts/${chart}" >/dev/null
done

istiod_render="$(${HELM} template istiod "${ROOT}/charts/istiod" -n istio-system -f "${ROOT}/values/istiod.kind.yaml")"
cni_render="$(${HELM} template istio-cni "${ROOT}/charts/cni" -n istio-system -f "${ROOT}/values/cni.kind.yaml")"
ztunnel_render="$(${HELM} template ztunnel "${ROOT}/charts/ztunnel" -n istio-system -f "${ROOT}/values/ztunnel.kind.yaml")"
grep -Fq 'docker.io/istio/pilot@sha256:d158739d5286f7899bc039589d248720c2a9b6622d54eeb7a3fdfbb65200c22c' <<<"${istiod_render}"
printf '%s' "${istiod_render}" | ruby -ryaml -e '
  docs = YAML.load_stream(STDIN.read).compact
  deploy = docs.find { |doc| doc["kind"] == "Deployment" && doc.dig("metadata", "name") == "istiod" }
  abort "local Istiod must render one replica" unless deploy&.dig("spec", "replicas") == 1
  abort "local Istiod must use spare control-plane capacity" unless
    deploy&.dig("spec", "template", "spec", "nodeSelector") == {"node-role.kubernetes.io/control-plane" => ""}
  tolerations = deploy&.dig("spec", "template", "spec", "tolerations") || []
  abort "local Istiod must tolerate the control-plane taint" unless tolerations.any? do |item|
    item == {"key" => "node-role.kubernetes.io/control-plane", "operator" => "Exists", "effect" => "NoSchedule"}
  end
  abort "local Istiod must not autoscale and amplify simulator CPU pressure" if
    docs.any? { |doc| doc["kind"] == "HorizontalPodAutoscaler" && doc.dig("metadata", "name") == "istiod" }
'
grep -Fq 'docker.io/istio/install-cni@sha256:b2eb80818fc345e3e9033f424ec7757cbf1a9d9a6494fea79648dab4887f2f7f' <<<"${cni_render}"
grep -Fq 'docker.io/istio/ztunnel@sha256:64d7c4ea9621fdad66160744dcf76999995fcc0ac399d04aba76d6d0aae72242' <<<"${ztunnel_render}"
printf '%s' "${cni_render}" | ruby -ryaml -e '
  docs = YAML.load_stream(STDIN.read).compact
  daemonset = docs.find { |doc| doc["kind"] == "DaemonSet" && doc.dig("metadata", "name") == "istio-cni-node" }
  configmap = docs.find { |doc| doc["kind"] == "ConfigMap" && doc.dig("metadata", "name") == "istio-cni-config" }
  resources = daemonset&.dig("spec", "template", "spec", "containers", 0, "resources")
  abort "local Istio CNI must reserve CPU for sandbox setup" unless
    resources&.dig("limits", "cpu").to_s == "2" &&
    resources&.dig("limits", "memory") == "512Mi" &&
    resources&.dig("requests", "cpu") == "500m" &&
    resources&.dig("requests", "memory") == "100Mi"
  abort "local Istio CNI must use native nftables on kind nodes" unless
    configmap&.dig("data", "NATIVE_NFTABLES") == "true"
  abort "local Istio CNI must not advertise IPv6 in the IPv4-only kind cluster" unless
    configmap&.dig("data", "AMBIENT_IPV6") == "false"
  abort "local Istio CNI must avoid optional DNS-proxy work on constrained kind nodes" unless
    configmap&.dig("data", "AMBIENT_DNS_CAPTURE") == "false"
  abort "local Istio CNI must not replay every pod network namespace after an agent-only restart" unless
    configmap&.dig("data", "AMBIENT_RECONCILE_POD_RULES_ON_STARTUP") == "false"
'
printf '%s' "${ztunnel_render}" | ruby -ryaml -e '
  docs = YAML.load_stream(STDIN.read).compact
  daemonset = docs.find { |doc| doc["kind"] == "DaemonSet" && doc.dig("metadata", "name") == "ztunnel" }
  resources = daemonset&.dig("spec", "template", "spec", "containers", 0, "resources")
  abort "local ztunnel must reserve CPU for ambient pod enrollment" unless
    resources&.dig("limits", "cpu").to_s == "1" &&
    resources&.dig("limits", "memory") == "512Mi" &&
    resources&.dig("requests", "cpu") == "500m" &&
    resources&.dig("requests", "memory") == "128Mi"
'
for component in istiod cni ztunnel; do
  render_var="${component}_render"
  grep -Fq 'imagePullPolicy: IfNotPresent' <<<"${!render_var}" || {
    echo "Istio ${component} render must explicitly use imagePullPolicy: IfNotPresent" >&2
    exit 1
  }
done

echo "ok istio-vendored version=1.30.2"
