#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
LOCAL="${ROOT}/infrastructure/local-production"

"${LOCAL}/vendor/verify.sh" >/dev/null

grep -Fq 'patch deployment cert-manager-webhook' "${LOCAL}/bin/install-foundations.sh"
grep -Fq '"startupProbe":{"httpGet":{"path":"/livez","port":"healthcheck"}' \
  "${LOCAL}/bin/install-foundations.sh"
grep -Fq '"readinessProbe":{"httpGet":{"path":"/healthz","port":"healthcheck"}' \
  "${LOCAL}/bin/install-foundations.sh"
grep -Fq '"requests":{"cpu":"100m","memory":"128Mi"}' \
  "${LOCAL}/bin/install-foundations.sh"
[[ "$(grep -Fc -- '--timeout=600s' "${LOCAL}/bin/install-foundations.sh")" -ge 1 ]]

rendered_foundation="$(mktemp)"
rendered_root="$(mktemp)"
trap 'rm -f "${rendered_foundation}" "${rendered_root}"' EXIT
"${LOCAL}/gitops/render-foundation.sh" "${rendered_foundation}"
ruby -ryaml -e '
  documents = YAML.load_stream(File.read(ARGV.fetch(0))).compact
  identities = documents.map do |document|
    api_version = document.fetch("apiVersion")
    group = api_version.include?("/") ? api_version.split("/", 2).first : ""
    [group, document.fetch("kind"), document.dig("metadata", "namespace") || "", document.dig("metadata", "name")]
  end
  duplicates = identities.group_by(&:itself).select { |_identity, matches| matches.length > 1 }.keys
  abort "foundation renders duplicate resources: #{duplicates.inspect}" unless duplicates.empty?
  metrics_server = documents.find do |document|
    document["kind"] == "Deployment" && document.dig("metadata", "name") == "metrics-server"
  end
  container = metrics_server&.dig("spec", "template", "spec", "containers", 0)
  abort "metrics-server liveness must not amplify local kubelet contention" unless
    container&.dig("livenessProbe", "timeoutSeconds") == 10 &&
    container&.dig("livenessProbe", "failureThreshold").to_i >= 12
  abort "metrics-server startup probe must protect a cold page cache" unless
    container&.dig("startupProbe", "timeoutSeconds") == 10 &&
    container&.dig("startupProbe", "failureThreshold").to_i >= 60
  abort "metrics-server readiness must tolerate local kubelet contention" unless
    container&.dig("readinessProbe", "timeoutSeconds") == 10
' "${rendered_foundation}"

helm template uno-arena-local-production-root \
  "${ROOT}/environments/local-production/argocd" \
  --set-string gitRepoURL=https://gitlab.test/group/project.git \
  --set-string helmRepoURL=https://gitlab.test/api/v4/projects/17/packages/helm/stable \
  >"${rendered_root}"
ruby -ryaml -e '
  applications = YAML.load_stream(File.read(ARGV.fetch(0))).compact
    .select { |document| document["kind"] == "Application" }
  explicit_empty = applications.select do |application|
    application.fetch("metadata").key?("finalizers") && application.dig("metadata", "finalizers") == []
  end.map { |application| application.dig("metadata", "name") }
  abort "Applications must omit empty finalizers: #{explicit_empty.inspect}" unless explicit_empty.empty?

  foundation = applications.find do |application|
    application.dig("metadata", "name") == "uno-arena-local-production-foundations"
  end
  ignored_webhooks = Array(foundation.dig("spec", "ignoreDifferences")).select do |rule|
    rule["group"] == "admissionregistration.k8s.io" &&
      rule["kind"] == "ValidatingWebhookConfiguration" &&
      rule["jqPathExpressions"] == [".webhooks[]?.failurePolicy"]
  end.map { |rule| rule["name"] }.sort
  expected_webhooks = %w[istio-validator-istio-system istiod-default-validator]
  abort "Istio validator drift must be ignored: #{ignored_webhooks.inspect}" unless ignored_webhooks == expected_webhooks
  sync_options = Array(foundation.dig("spec", "syncPolicy", "syncOptions"))
  abort "foundation must respect ignored differences during sync" unless sync_options.include?("RespectIgnoreDifferences=true")
' "${rendered_root}"

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
  certificate_ref = listeners.fetch(1).dig("tls", "certificateRefs", 0)
  expected_certificate_ref = {"group"=>"", "kind"=>"Secret", "name"=>"uno-arena-local-tls"}
  abort "Gateway certificate reference defaults must be explicit" unless certificate_ref == expected_certificate_ref
  redirect = docs.find { |d| d.dig("metadata", "name") == "uno-arena-http-redirect" }
  expected_parent = {"group"=>"gateway.networking.k8s.io", "kind"=>"Gateway", "name"=>"uno-arena-gateway"}
  docs.select { |d| d["kind"] == "HTTPRoute" }.each do |route|
    parent = route.dig("spec", "parentRefs", 0).reject { |key, _value| key == "sectionName" }
    abort "HTTPRoute parent reference defaults must be explicit" unless parent == expected_parent
  end
  filter = redirect.dig("spec", "rules", 0, "filters", 0, "requestRedirect")
  abort "redirect contract mismatch" unless filter == {"scheme"=>"https", "port"=>8443, "statusCode"=>301}
  redirect_match = redirect.dig("spec", "rules", 0, "matches", 0, "path")
  abort "redirect match defaults must be explicit" unless redirect_match == {"type"=>"PathPrefix", "value"=>"/"}
  bff = docs.find { |d| d.dig("metadata", "name") == "uno-arena-bff" }
  expected_backend = {"group"=>"", "kind"=>"Service", "name"=>"gateway", "port"=>8080, "weight"=>1}
  abort "BFF backend reference defaults must be explicit" unless bff.dig("spec", "rules", 0, "backendRefs", 0) == expected_backend
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
