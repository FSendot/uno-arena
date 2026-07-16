#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
LOCAL="${ROOT}/infrastructure/local-production"
SOURCE="${LOCAL}/vendor/argocd/install.yaml"
RENDERER="${LOCAL}/bin/render-argocd-core-manifest"

ruby -c "${RENDERER}" >/dev/null
tmp_dir="$(mktemp -d)"
trap 'rm -rf "${tmp_dir}"' EXIT
rendered="${tmp_dir}/argocd-core.yaml"
"${RENDERER}" "${SOURCE}" >"${rendered}"

ruby -ryaml -e '
  source = YAML.load_stream(File.read(ARGV.fetch(0))).compact
  rendered = YAML.load_stream(File.read(ARGV.fetch(1))).compact
  expected_policies = %w[
    argocd-application-controller-network-policy
    argocd-applicationset-controller-network-policy
    argocd-dex-server-network-policy
    argocd-notifications-controller-network-policy
    argocd-redis-network-policy
    argocd-repo-server-network-policy
    argocd-server-network-policy
  ].sort
  actual_policies = source.select { |doc| doc["kind"] == "NetworkPolicy" }
    .map { |doc| doc.dig("metadata", "name") }.sort
  abort "vendored Argo NetworkPolicy inventory changed" unless actual_policies == expected_policies
  expected_rendered = source.reject { |doc| doc["kind"] == "NetworkPolicy" }
  abort "renderer changed non-NetworkPolicy Argo resources" unless rendered == expected_rendered
  abort "renderer retained an Argo NetworkPolicy" if rendered.any? { |doc| doc["kind"] == "NetworkPolicy" }
' "${SOURCE}" "${rendered}"

grep -Fq 'render-argocd-core-manifest' "${LOCAL}/bin/install-argocd-core.sh"
grep -Fq -- '--ignore-not-found' "${LOCAL}/bin/install-argocd-core.sh"
for policy in \
  argocd-application-controller-network-policy \
  argocd-applicationset-controller-network-policy \
  argocd-dex-server-network-policy \
  argocd-notifications-controller-network-policy \
  argocd-redis-network-policy \
  argocd-repo-server-network-policy \
  argocd-server-network-policy; do
  grep -Fq "${policy}" "${LOCAL}/bin/install-argocd-core.sh"
done

echo "ok local-production-argocd-core-manifest-contracts"
