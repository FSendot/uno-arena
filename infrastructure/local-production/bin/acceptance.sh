#!/usr/bin/env bash
# Read-only foundation acceptance for the exact long-lived simulator context.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "${SCRIPT_DIR}/lib.sh"

foundation_only=false
pre_source=false
case "${1:-}" in
  "") ;;
  --foundation-only) foundation_only=true ;;
  --pre-source) pre_source=true ;;
  *) die "usage: $0 [--foundation-only|--pre-source]" ;;
esac
[[ "$#" -le 1 ]] || die "usage: $0 [--foundation-only|--pre-source]"

for command in base64 docker kubectl openssl ruby; do
  require_cmd "${command}"
done
assert_simulator_cluster_exists
assert_exact_context

nodes="$(kubectl --context "${LOCAL_PRODUCTION_CONTEXT}" get nodes --no-headers)"
[[ "$(wc -l <<<"${nodes}" | tr -d ' ')" == "3" ]] || die "simulator must have exactly 3 nodes"
workers="$(kubectl --context "${LOCAL_PRODUCTION_CONTEXT}" get nodes \
  -l node-role.kubernetes.io/worker -o name)"
[[ "$(wc -w <<<"${workers}" | tr -d ' ')" == "2" ]] ||
  die "simulator must have exactly two nodes labeled node-role.kubernetes.io/worker"
control_planes="$(kubectl --context "${LOCAL_PRODUCTION_CONTEXT}" get nodes \
  -l node-role.kubernetes.io/control-plane -o name)"
[[ "$(wc -w <<<"${control_planes}" | tr -d ' ')" == "1" ]] ||
  die "simulator must have exactly one control-plane node"
if kubectl --context "${LOCAL_PRODUCTION_CONTEXT}" get nodes \
  -l 'node-role.kubernetes.io/control-plane,node-role.kubernetes.io/worker' -o name | grep -q .; then
  die "control-plane must not carry the worker-role label"
fi

for namespace in argocd uno-arena observability secret-seed istio-system external-secrets cert-manager; do
  kubectl --context "${LOCAL_PRODUCTION_CONTEXT}" get namespace "${namespace}" >/dev/null
done
for namespace in uno-arena observability; do
  [[ "$(kubectl --context "${LOCAL_PRODUCTION_CONTEXT}" get namespace "${namespace}" \
    -o jsonpath='{.metadata.labels.istio\.io/dataplane-mode}')" == "ambient" ]] ||
    die "${namespace} is not enrolled in ambient mode"
done

for crd in gateways.gateway.networking.k8s.io httproutes.gateway.networking.k8s.io \
  certificates.cert-manager.io clustersecretstores.external-secrets.io; do
  kubectl --context "${LOCAL_PRODUCTION_CONTEXT}" get "crd/${crd}" >/dev/null
done
kubectl --context "${LOCAL_PRODUCTION_CONTEXT}" wait --for=condition=Ready \
  clustersecretstore/local-kubernetes --timeout=30s
kubectl --context "${LOCAL_PRODUCTION_CONTEXT}" -n cert-manager wait --for=condition=Ready \
  certificate/uno-arena-local-ca --timeout=30s
kubectl --context "${LOCAL_PRODUCTION_CONTEXT}" -n uno-arena wait --for=condition=Ready \
  certificate/uno-arena-local-tls --timeout=30s
kubectl --context "${LOCAL_PRODUCTION_CONTEXT}" -n argocd wait --for=condition=Ready \
  certificate/argocd-server-local-tls --timeout=30s

kubectl --context "${LOCAL_PRODUCTION_CONTEXT}" -n istio-system rollout status deployment/istiod --timeout=30s
kubectl --context "${LOCAL_PRODUCTION_CONTEXT}" -n istio-system rollout status daemonset/istio-cni-node --timeout=30s
kubectl --context "${LOCAL_PRODUCTION_CONTEXT}" -n istio-system rollout status daemonset/ztunnel --timeout=30s
kubectl --context "${LOCAL_PRODUCTION_CONTEXT}" -n external-secrets rollout status deployment/external-secrets --timeout=30s
kubectl --context "${LOCAL_PRODUCTION_CONTEXT}" -n kube-system rollout status deployment/metrics-server --timeout=30s
metrics_args="$(kubectl --context "${LOCAL_PRODUCTION_CONTEXT}" -n kube-system \
  get deployment metrics-server -o jsonpath='{.spec.template.spec.containers[0].args}')"
[[ "${metrics_args}" == *--kubelet-insecure-tls* ]] || die "kind-only metrics-server TLS override missing"

kubectl --context "${LOCAL_PRODUCTION_CONTEXT}" -n uno-arena wait --for=condition=Programmed \
  gateway/uno-arena-gateway --timeout=30s
for route in uno-arena-http-redirect uno-arena-bff; do
  kubectl --context "${LOCAL_PRODUCTION_CONTEXT}" -n uno-arena wait \
    --for=jsonpath='{.status.parents[0].conditions[?(@.type=="Accepted")].status}'=True \
    "httproute/${route}" --timeout=30s
done
backend_names="$(kubectl --context "${LOCAL_PRODUCTION_CONTEXT}" -n uno-arena get httproutes \
  -o jsonpath='{range .items[*].spec.rules[*].backendRefs[*]}{.name}{"\n"}{end}')"
[[ "${backend_names}" == "gateway" ]] || die "only the BFF service may be exposed through HTTPRoute"
[[ "$(kubectl --context "${LOCAL_PRODUCTION_CONTEXT}" -n uno-arena get service uno-arena-gateway-istio \
  -o jsonpath='{.spec.ports[?(@.port==8080)].nodePort}')" == "30080" ]] || die "Gateway HTTP NodePort mismatch"
[[ "$(kubectl --context "${LOCAL_PRODUCTION_CONTEXT}" -n uno-arena get service uno-arena-gateway-istio \
  -o jsonpath='{.spec.ports[?(@.port==8443)].nodePort}')" == "30443" ]] || die "Gateway HTTPS NodePort mismatch"

kubectl --context "${LOCAL_PRODUCTION_CONTEXT}" -n uno-arena get peerauthentication strict-mtls >/dev/null
kubectl --context "${LOCAL_PRODUCTION_CONTEXT}" -n observability get peerauthentication strict-mtls >/dev/null
kubectl --context "${LOCAL_PRODUCTION_CONTEXT}" -n uno-arena get authorizationpolicy \
  application-metrics-from-prometheus gateway-callers identity-callers room-gameplay-callers \
  spectator-view-callers ranking-callers analytics-callers tournament-orchestration-callers \
  game-integrity-callers postgres-identity-callers postgres-room-gameplay-callers \
  postgres-tournament-callers postgres-ranking-callers redis-callers kafka-callers \
  kurrentdb-callers clickhouse-callers keycloak-callers debezium-connect-callers >/dev/null

[[ "$(kubectl --context "${LOCAL_PRODUCTION_CONTEXT}" -n argocd get service argocd-server \
  -o jsonpath='{.spec.ports[?(@.port==443)].nodePort}')" == "30445" ]] || die "Argo API NodePort mismatch"
"${SCRIPT_DIR}/check-argocd-controller-network.sh"

validate_repository_secret() {
  local secret_name="$1"
  local expected_type="$2"
  local secret_json
  secret_json="$(kubectl --context "${LOCAL_PRODUCTION_CONTEXT}" -n argocd \
    get secret "${secret_name}" -o json 2>/dev/null)" || return 1
  EXPECTED_REPOSITORY_TYPE="${expected_type}" ruby -rbase64 -rjson -e '
      document = JSON.parse(STDIN.read)
      label = document.dig("metadata", "labels", "argocd.argoproj.io/secret-type")
      abort "wrong Argo repository label" unless label == "repository"
      data = document.fetch("data")
      %w[type url username password].each do |key|
        abort "missing Argo repository credential key #{key}" if data.fetch(key, "").empty?
      end
      type = Base64.strict_decode64(data.fetch("type"))
      abort "wrong Argo repository type" unless type == ENV.fetch("EXPECTED_REPOSITORY_TYPE")
    ' <<<"${secret_json}"
}
if [[ "${pre_source}" == false ]]; then
  validate_repository_secret uno-arena-git-repository git || die "private Git repository credential is not ready"
  validate_repository_secret uno-arena-helm-repository helm || die "private Helm repository credential is not ready"
fi

kubectl --context "${LOCAL_PRODUCTION_CONTEXT}" -n argocd get configmap argocd-cm -o json |
  ruby -rjson -e '
    document = JSON.parse(STDIN.read)
    data = document.fetch("data")
    abort "ci-readonly API account missing" unless data["accounts.ci-readonly"] == "apiKey"
    abort "ci-readonly API account disabled" unless data["accounts.ci-readonly.enabled"] == "true"
  ' || die "least-privilege Argo CI account is not ready"
kubectl --context "${LOCAL_PRODUCTION_CONTEXT}" -n argocd get configmap argocd-rbac-cm -o json |
  ruby -rjson -e '
    document = JSON.parse(STDIN.read)
    actual = document.dig("data", "policy.csv").to_s.lines.map(&:strip).reject(&:empty?).sort
    expected = [
      "p, ci-readonly, applications, get, uno-arena-bootstrap/*, allow",
      "p, ci-readonly, applications, get, uno-arena-foundations/*, allow",
      "p, ci-readonly, applications, get, uno-arena-stateful-platform/*, allow",
      "p, ci-readonly, applications, get, uno-arena-workloads/*, allow"
    ].sort
    abort "ci-readonly RBAC policy mismatch" unless actual == expected
  ' || die "least-privilege cross-project Argo CI policy is not ready"

wait_for_applicationset() {
  local applicationset_name="$1"
  local applicationset_ready=false
  for _attempt in $(seq 1 30); do
    if kubectl --context "${LOCAL_PRODUCTION_CONTEXT}" -n argocd get applicationset \
      "${applicationset_name}" -o json 2>/dev/null |
      ruby -rjson -e '
        document = JSON.parse(STDIN.read)
        conditions = Array(document.dig("status", "conditions"))
        ready = conditions.any? { |c| c["type"] == "ParametersGenerated" && c["status"] == "True" } &&
          conditions.any? { |c| c["type"] == "ResourcesUpToDate" && c["status"] == "True" } &&
          conditions.none? { |c| c["type"] == "ErrorOccurred" && c["status"] == "True" }
        exit(ready ? 0 : 1)
      '; then
      applicationset_ready=true
      break
    fi
    sleep 2
  done
  [[ "${applicationset_ready}" == true ]] || die "ApplicationSet is not healthy: ${applicationset_name}"
}
if [[ "${pre_source}" == false ]]; then
  for application in uno-arena-local-production-root uno-arena-local-production-foundations; do
    kubectl --context "${LOCAL_PRODUCTION_CONTEXT}" -n argocd get application "${application}" -o json |
      ruby -rjson -e '
        document = JSON.parse(STDIN.read)
        abort "root/foundation Application is not Synced" unless document.dig("status", "sync", "status") == "Synced"
        abort "root/foundation Application is not Healthy" unless document.dig("status", "health", "status") == "Healthy"
      ' || die "repository-owned Argo control plane is not ready: ${application}"
  done
  kubectl --context "${LOCAL_PRODUCTION_CONTEXT}" -n argocd get applicationprojects.argoproj.io \
    uno-arena-bootstrap uno-arena-foundations uno-arena-workloads uno-arena-stateful-platform >/dev/null
  kubectl --context "${LOCAL_PRODUCTION_CONTEXT}" -n argocd get applicationsets.argoproj.io \
    uno-arena-local-production-services uno-arena-local-production-platform >/dev/null
  wait_for_applicationset uno-arena-local-production-services
  wait_for_applicationset uno-arena-local-production-platform
fi

if [[ "${foundation_only}" == false && "${pre_source}" == false ]]; then
  kubectl --context "${LOCAL_PRODUCTION_CONTEXT}" -n uno-arena get secret gitlab-registry -o json |
    ruby -rjson -e '
      document = JSON.parse(STDIN.read)
      abort "gitlab-registry must be a Docker config Secret" unless document["type"] == "kubernetes.io/dockerconfigjson"
      abort "gitlab-registry Docker config is missing" if document.dig("data", ".dockerconfigjson").to_s.empty?
    ' || die "private workload image-pull credential is not ready"

  if [[ -n "${EXPECTED_ARGO_APPLICATIONS+x}" ]]; then
    [[ -n "${EXPECTED_ARGO_APPLICATIONS}" ]] ||
      die "EXPECTED_ARGO_APPLICATIONS was provided but contains no Applications"
    IFS=',' read -r -a expected_applications <<<"${EXPECTED_ARGO_APPLICATIONS}"
    ((${#expected_applications[@]} > 0)) || die "no promoted Applications were expected"
    for application in "${expected_applications[@]}"; do
      application="${application//[[:space:]]/}"
      [[ "${application}" =~ ^uno-arena-local-production-[a-z0-9-]+$ ]] ||
        die "invalid expected local-production Application: ${application}"
      kubectl --context "${LOCAL_PRODUCTION_CONTEXT}" -n argocd get application "${application}" -o json |
        ruby -rjson -e '
          document = JSON.parse(STDIN.read)
          synced = document.dig("status", "sync", "status") == "Synced"
          healthy = document.dig("status", "health", "status") == "Healthy"
          exit(synced && healthy ? 0 : 1)
        ' || die "expected Application is not Synced and Healthy: ${application}"
    done
  fi
fi

port_bindings="$(docker inspect "${LOCAL_PRODUCTION_CLUSTER}-control-plane" \
  --format '{{json .HostConfig.PortBindings}}')"
[[ "${port_bindings}" == *'"30445/tcp":[{"HostIp":"127.0.0.1","HostPort":"9443"}]'* ]] ||
  die "Argo API must bind only to 127.0.0.1:9443"

tmp_dir="$(mktemp -d)"
trap 'rm -rf "${tmp_dir}"' EXIT
kubectl --context "${LOCAL_PRODUCTION_CONTEXT}" -n cert-manager get secret uno-arena-local-ca \
  -o jsonpath='{.data.tls\.crt}' | base64 --decode >"${tmp_dir}/local-ca.crt"
openssl s_client -connect 127.0.0.1:9443 -verify_ip 127.0.0.1 \
  -verify_return_error -CAfile "${tmp_dir}/local-ca.crt" </dev/null >/dev/null 2>&1 ||
  die "Argo loopback API failed CA and IP verification"

kubectl --context "${LOCAL_PRODUCTION_CONTEXT}" -n argocd get applications.argoproj.io >/dev/null
[[ "$(kubectl --context "${LOCAL_PRODUCTION_CONTEXT}" -n argocd get configmap argocd-cmd-params-cm \
  -o jsonpath='{.data.applicationsetcontroller\.enable\.progressive\.syncs}')" == "true" ]] ||
  die "Argo Progressive Syncs must be explicitly enabled"
kubectl --context "${LOCAL_PRODUCTION_CONTEXT}" -n uno-arena get deployments,statefulsets,pods
mode=full
[[ "${foundation_only}" == true ]] && mode=foundation-only
[[ "${pre_source}" == true ]] && mode=pre-source
echo "ok local-production-acceptance mode=${mode} context=${LOCAL_PRODUCTION_CONTEXT}"
