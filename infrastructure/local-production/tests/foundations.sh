#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
LOCAL="${ROOT}/infrastructure/local-production"

"${LOCAL}/vendor/verify.sh" >/dev/null

ruby -ryaml -e '
  docs = YAML.load_stream(File.read(ARGV.fetch(0))).compact
  expected = %w[argocd uno-arena observability secret-seed istio-system external-secrets cert-manager]
  actual = docs.select { |d| d["kind"] == "Namespace" }.map { |d| d.dig("metadata", "name") }
  abort "namespace inventory mismatch: #{actual.inspect}" unless actual == expected
  %w[uno-arena observability].each do |name|
    ns = docs.find { |d| d.dig("metadata", "name") == name }
    abort "#{name} must use ambient" unless ns.dig("metadata", "labels", "istio.io/dataplane-mode") == "ambient"
  end
' "${LOCAL}/manifests/00-namespaces.yaml"

ruby -ryaml -e '
  docs = YAML.load_stream(File.read(ARGV.fetch(0))).compact
  role = docs.find { |d| d["kind"] == "Role" }
  abort "ESO Role must be secret-seed scoped" unless role.dig("metadata", "namespace") == "secret-seed"
  secret_rule = role.fetch("rules").find { |r| r["resources"] == ["secrets"] }
  abort "ESO secret verbs must be read-only" unless secret_rule && secret_rule["verbs"] == %w[get list watch]
  abort "ESO Role grants extra rules" unless role.fetch("rules").length == 2
  store = docs.find { |d| d["kind"] == "ClusterSecretStore" }
  kube = store.dig("spec", "provider", "kubernetes")
  abort "wrong remote namespace" unless kube["remoteNamespace"] == "secret-seed"
  abort "wrong API server" unless kube.dig("server", "url") == "https://kubernetes.default.svc"
  ca = kube.dig("server", "caProvider")
  abort "store must use kube-root CA ConfigMap" unless ca == {"type"=>"ConfigMap", "name"=>"kube-root-ca.crt", "key"=>"ca.crt", "namespace"=>"external-secrets"}
  sa = kube.dig("auth", "serviceAccount")
  abort "wrong store service account" unless sa == {"name"=>"local-kubernetes-secret-reader", "namespace"=>"external-secrets"}
' "${LOCAL}/manifests/10-external-secrets-local.yaml"

ruby -ryaml -e '
  docs = ARGV.flat_map { |path| YAML.load_stream(File.read(path)).compact }
  issuers = docs.select { |d| d["kind"] == "ClusterIssuer" }.map { |d| d.dig("metadata", "name") }
  abort "local issuers missing" unless issuers.sort == %w[local-selfsigned-bootstrap uno-arena-local-ca]
  certs = docs.select { |d| d["kind"] == "Certificate" }
  edge = certs.find { |d| d.dig("metadata", "name") == "uno-arena-local-tls" }
  abort "edge DNS contract mismatch" unless edge.dig("spec", "dnsNames") == ["uno-arena.local"]
  argo = certs.find { |d| d.dig("metadata", "name") == "argocd-server-local-tls" }
  abort "Argo cert must verify loopback IP" unless argo.dig("spec", "ipAddresses") == ["127.0.0.1"]
  abort "Argo TLS secret contract mismatch" unless argo.dig("spec", "secretName") == "argocd-server-tls"
' "${LOCAL}/manifests/20-ca-bootstrap.yaml" "${LOCAL}/manifests/21-local-certificates.yaml"

ruby -ryaml -e '
  docs = YAML.load_stream(File.read(ARGV.fetch(0))).compact
  gateway = docs.find { |d| d["kind"] == "Gateway" }
  abort "GatewayClass mismatch" unless gateway.dig("spec", "gatewayClassName") == "istio"
  listeners = gateway.dig("spec", "listeners")
  abort "listener ports mismatch" unless listeners.map { |l| l["port"] } == [8080, 8443]
  redirect = docs.find { |d| d.dig("metadata", "name") == "uno-arena-http-redirect" }
  filter = redirect.dig("spec", "rules", 0, "filters", 0, "requestRedirect")
  abort "redirect contract mismatch" unless filter == {"scheme"=>"https", "port"=>8443, "statusCode"=>301}
  backend_names = docs.flat_map { |d| d.dig("spec", "rules") || [] }.flat_map { |r| r["backendRefs"] || [] }.map { |b| b["name"] }
  abort "only BFF may be public: #{backend_names.inspect}" unless backend_names == ["gateway"]
' "${LOCAL}/manifests/30-gateway.yaml"

ruby -ryaml -e '
  docs = YAML.load_stream(File.read(ARGV.fetch(0))).compact
  peers = docs.select { |d| d["kind"] == "PeerAuthentication" }
  abort "strict mTLS namespaces mismatch" unless peers.map { |d| d.dig("metadata", "namespace") }.sort == %w[observability uno-arena]
  abort "non-strict peer auth" unless peers.all? { |d| d.dig("spec", "mtls", "mode") == "STRICT" }
  policies = docs.select { |d| d["kind"] == "AuthorizationPolicy" }
  principals = policies.flat_map { |d| d.fetch("spec").fetch("rules", []) }.flat_map { |r| r.fetch("from", []) }.flat_map { |f| f.dig("source", "principals") || [] }
  abort "wildcard principal forbidden" if principals.any? { |p| p.include?("*") }
  abort "malformed principal" unless principals.all? { |p| p.match?(%r{\Acluster\.local/ns/[a-z0-9-]+/sa/[a-z0-9-]+\z}) }
  required = %w[
    cluster.local/ns/observability/sa/prometheus
    cluster.local/ns/uno-arena/sa/uno-arena-gateway-istio
    cluster.local/ns/uno-arena/sa/room-gameplay-runtime
    cluster.local/ns/uno-arena/sa/room-gameplay-integrity-reconciler
    cluster.local/ns/uno-arena/sa/tournament-orchestration-provisioning-worker
    cluster.local/ns/uno-arena/sa/spectator-view-projection-rebuilder
    cluster.local/ns/uno-arena/sa/analytics-projection-rebuilder
  ]
  missing = required - principals
  abort "required exact principals missing: #{missing.inspect}" unless missing.empty?
  metrics = policies.find { |d| d.dig("metadata", "name") == "application-metrics-from-prometheus" }
  abort "metrics port contract mismatch" unless metrics.dig("spec", "rules", 0, "to", 0, "operation", "ports") == ["9090"]
' "${LOCAL}/manifests/40-ambient-security.yaml"

grep -Fq -- '--kubelet-insecure-tls' "${LOCAL}/bin/install-foundations.sh"
grep -Fq 'gatewayClasses.istio.service.spec.type=NodePort' "${LOCAL}/bin/install-istio.sh"
grep -Fq 'nodePort":30080' "${LOCAL}/bin/install-foundations.sh"
grep -Fq 'nodePort":30443' "${LOCAL}/bin/install-foundations.sh"
grep -Fq 'nodePort":30445' "${LOCAL}/bin/install-argocd-core.sh"
grep -Fq "ARGOCD_GIT_REPO_URL" "${LOCAL}/bin/bootstrap-argocd.sh"
grep -Fq "ARGOCD_HELM_REPO_URL" "${LOCAL}/bin/bootstrap-argocd.sh"
grep -Fq 'configure-argocd-repositories.sh' "${LOCAL}/bin/bootstrap-argocd.sh"
grep -Fq 'configure-argocd-ci-account.sh' "${LOCAL}/bin/install-argocd-core.sh"
grep -Fq 'environments/local-production/services/enabled/*.yaml' \
  "${ROOT}/environments/local-production/argocd/services-applicationset.yaml"
grep -Fq -- '--show-only templates/bootstrap-project.yaml' "${LOCAL}/bin/bootstrap-argocd.sh"
grep -Fq -- '--show-only templates/root-application.yaml' "${LOCAL}/bin/bootstrap-argocd.sh"
grep -Fq 'gitops/verify-foundation.sh' "${LOCAL}/bin/bootstrap-argocd.sh"
grep -Fq 'path: environments/local-production/argocd' \
  "${ROOT}/environments/local-production/argocd/templates/root-application.yaml"
grep -Fq 'path: infrastructure/local-production/gitops/foundation' \
  "${ROOT}/environments/local-production/argocd/foundation-application.yaml"
grep -Fq 'only the BFF service may be exposed' "${LOCAL}/bin/acceptance.sh"
grep -Fq 'verify_ip 127.0.0.1' "${LOCAL}/bin/acceptance.sh"
grep -Fq 'uno-arena-stateful-platform/*, allow' "${LOCAL}/bin/configure-argocd-ci-account.sh"
grep -Fq 'uno-arena-bootstrap/*, allow' "${LOCAL}/bin/configure-argocd-ci-account.sh"
grep -Fq 'uno-arena-foundations/*, allow' "${LOCAL}/bin/configure-argocd-ci-account.sh"
grep -Fq '"${SCRIPT_DIR}/acceptance.sh" --foundation-only' "${LOCAL}/bin/install-foundations.sh"

if rg -n 'kubectl config use-context|--insecure|--insecure-skip-tls-verify|KUBECONFIG=' "${LOCAL}/bin"; then
  echo "installer violates exact-context/TLS/kubeconfig guardrails" >&2
  exit 1
fi

echo "ok local-production-foundations"
