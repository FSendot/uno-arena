#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
LOCAL="${ROOT}/infrastructure/local-production"

for script in "${LOCAL}"/bin/*.sh; do
  bash -n "${script}"
done

ruby -ryaml -e '
  doc = YAML.load_file(ARGV.fetch(0))
  abort "wrong cluster name" unless doc["name"] == "uno-arena-production"
  nodes = doc.fetch("nodes")
  roles = Hash.new(0)
  nodes.each { |node| roles[node["role"]] += 1 }
  abort "expected one control-plane and two workers" unless roles == {"control-plane"=>1, "worker"=>2}
  workers = nodes.select { |node| node["role"] == "worker" }
  abort "reserved worker-role labels must be assigned by kind, not kubelet flags" if workers.any? { |node| node.fetch("labels", {}).key?("node-role.kubernetes.io/worker") }
  abort "control-plane must not carry configured labels" unless nodes.first.fetch("labels", {}).empty?
  kubeadm_patches = Array(nodes.first["kubeadmConfigPatches"]).map { |patch| YAML.safe_load(patch) }
  cluster_patch = kubeadm_patches.find { |patch| patch["kind"] == "ClusterConfiguration" }
  abort "control-plane leader-election patch missing" unless cluster_patch
  expected_leader_election = {
    "leader-elect-lease-duration" => "60s",
    "leader-elect-renew-deadline" => "40s",
    "leader-elect-retry-period" => "10s"
  }
  %w[controllerManager scheduler].each do |component|
    actual = Array(cluster_patch.dig(component, "extraArgs")).to_h do |argument|
      [argument.fetch("name"), argument.fetch("value")]
    end
    abort "#{component} leader-election budget mismatch" unless
      actual.slice(*expected_leader_election.keys) == expected_leader_election
  end
  mappings = nodes.first.fetch("extraPortMappings")
  expected = [[8080,30080],[8443,30443],[8444,30444],[9443,30445]]
  actual = mappings.map { |m| [m["hostPort"], m["containerPort"]] }
  abort "host mappings mismatch: #{actual.inspect}" unless actual == expected
  abort "host mappings must be loopback-only" unless mappings.all? { |m| m["listenAddress"] == "127.0.0.1" }
' "${LOCAL}/kind/cluster.yaml"

grep -Fq 'node-role.kubernetes.io/worker= --overwrite' "${LOCAL}/bin/create-cluster.sh" || {
  echo "create-cluster must assign worker roles through the Kubernetes API" >&2
  exit 1
}

printf '%s' '{"data":{"Corefile":".:53 {\n    forward . /etc/resolv.conf {\n       max_concurrent 1000\n    }\n}\n"}}' |
  "${LOCAL}/bin/render-coredns-ipv4-patch" |
  ruby -rjson -e '
    patch = JSON.parse(STDIN.read)
    corefile = patch.dig("data", "Corefile")
    abort "IPv4-only CoreDNS template missing" unless corefile.include?("template IN AAAA .")
    abort "IPv4-only template must precede forwarding" unless
      corefile.index("template IN AAAA .") < corefile.index("forward . /etc/resolv.conf")
  '

printf '%s' 'apiVersion: v1
kind: Pod
spec:
  containers:
    - name: kube-apiserver
      livenessProbe:
        failureThreshold: 8
        httpGet: {path: /livez, port: 6443}
      readinessProbe:
        failureThreshold: 3
        httpGet: {path: /readyz, port: 6443}
' |
  "${LOCAL}/bin/render-kube-apiserver-probe-manifest" |
  ruby -ryaml -e '
    pod = YAML.safe_load(STDIN.read)
    container = pod.dig("spec", "containers", 0)
    abort "API liveness budget mismatch" unless container.dig("livenessProbe", "failureThreshold") == 30
    abort "API readiness must remain strict" unless container.dig("readinessProbe", "failureThreshold") == 3
  '

grep -q 'LOCAL_PRODUCTION_CONTEXT="kind-' "${LOCAL}/bin/lib.sh"
grep -q 'kind get kubeconfig --name' "${LOCAL}/bin/lib.sh"
grep -q 'certificate-authority-data' "${LOCAL}/bin/lib.sh"
grep -q 'assert_exact_context' "${LOCAL}/bin/bootstrap-argocd.sh"
grep -q 'missing pre-vendored Argo CD manifest' "${LOCAL}/bin/install-argocd-core.sh"
grep -q 'vendor/verify.sh' "${LOCAL}/bin/install-argocd-core.sh"
grep -q 'kindest/node:v1.36.1@sha256:3489c7674813ba5d8b1a9977baea8a6e553784dab7b84759d1014dbd78f7ebd5' "${LOCAL}/bin/create-cluster.sh"
grep -q 'docker image inspect' "${LOCAL}/bin/create-cluster.sh"
grep -Fq "docker info --format '{{.NCPU}}'" "${LOCAL}/bin/create-cluster.sh"
grep -Fq 'requires at least 8 CPUs assigned to Docker' "${LOCAL}/bin/create-cluster.sh"
grep -Fq "docker info --format '{{.MemTotal}}'" "${LOCAL}/bin/create-cluster.sh"
grep -Fq 'minimum_memory_bytes="$((10 * 1024 * 1024 * 1024))"' "${LOCAL}/bin/create-cluster.sh"
grep -Fq 'requires at least 10 GiB assigned to Docker' "${LOCAL}/bin/create-cluster.sh"
grep -q -- '--image "${KIND_NODE_IMAGE}"' "${LOCAL}/bin/create-cluster.sh"
grep -Fq 'docker update --cpus "${docker_cpu_count}" "${LOCAL_PRODUCTION_CLUSTER}-control-plane"' "${LOCAL}/bin/create-cluster.sh"
grep -Fq 'docker update --cpus "${docker_cpu_count}" "${LOCAL_PRODUCTION_CLUSTER}-worker"' "${LOCAL}/bin/create-cluster.sh"
grep -Fq 'docker update --cpus "${docker_cpu_count}" "${LOCAL_PRODUCTION_CLUSTER}-worker2"' "${LOCAL}/bin/create-cluster.sh"
grep -Fq 'docker update --cpu-shares 4096 "${LOCAL_PRODUCTION_CLUSTER}-control-plane"' "${LOCAL}/bin/create-cluster.sh"
grep -Fq 'docker update --cpu-shares 1024 "${LOCAL_PRODUCTION_CLUSTER}-worker"' "${LOCAL}/bin/create-cluster.sh"
grep -Fq 'docker update --cpu-shares 1024 "${LOCAL_PRODUCTION_CLUSTER}-worker2"' "${LOCAL}/bin/create-cluster.sh"
grep -Fq 'render-kube-apiserver-probe-manifest' "${LOCAL}/bin/create-cluster.sh"
grep -Fq 'get --raw=/readyz' "${LOCAL}/bin/create-cluster.sh"
grep -Fq 'patch deployment coredns' "${LOCAL}/bin/create-cluster.sh"
grep -Fq '"replicas":3' "${LOCAL}/bin/create-cluster.sh"
grep -Fq 'render-coredns-ipv4-patch' "${LOCAL}/bin/create-cluster.sh"
grep -Fq 'rollout restart deployment/coredns' "${LOCAL}/bin/create-cluster.sh"
grep -Fq '"topologyKey":"kubernetes.io/hostname"' "${LOCAL}/bin/create-cluster.sh"
grep -Fq '"requiredDuringSchedulingIgnoredDuringExecution"' "${LOCAL}/bin/create-cluster.sh"
grep -Fq '"k8s-app":"kube-dns"' "${LOCAL}/bin/create-cluster.sh"
grep -Fq '"name":"coredns","startupProbe"' "${LOCAL}/bin/create-cluster.sh"
grep -Fq '"port":"readiness-probe"' "${LOCAL}/bin/create-cluster.sh"
grep -Fq '"readinessProbe":{"timeoutSeconds":5,"failureThreshold":10}' "${LOCAL}/bin/create-cluster.sh"
grep -Fq '"resources":{"limits":{"cpu":"1"}}' "${LOCAL}/bin/create-cluster.sh"
grep -Fq 'rollout status' "${LOCAL}/bin/create-cluster.sh"
grep -Fq 'deployment/coredns --timeout=300s' "${LOCAL}/bin/create-cluster.sh"
grep -q 'environments/local-production/argocd' "${LOCAL}/bin/bootstrap-argocd.sh"
grep -q 'templates/bootstrap-project.yaml' "${LOCAL}/bin/bootstrap-argocd.sh"
grep -q 'templates/root-application.yaml' "${LOCAL}/bin/bootstrap-argocd.sh"
test -f "${ROOT}/environments/local-production/argocd/app-project.yaml"
test -f "${ROOT}/environments/local-production/argocd/services-applicationset.yaml"
test -f "${ROOT}/environments/local-production/argocd/platform-applicationset.yaml"
test -f "${ROOT}/environments/local-production/argocd/foundation-application.yaml"
grep -q 'ARGOCD_GIT_REPO_URL' "${LOCAL}/bin/bootstrap-argocd.sh"
grep -q 'ARGOCD_HELM_REPO_URL' "${LOCAL}/bin/bootstrap-argocd.sh"
grep -q 'configure-argocd-repositories.sh' "${LOCAL}/bin/bootstrap-argocd.sh"
grep -q 'configure-argocd-ci-account.sh' "${LOCAL}/bin/install-argocd-core.sh"
grep -q 'generate-argocd-ci-token.sh' "${LOCAL}/README.md"
if grep -q 'ARGOCD_ROOT_MANIFEST' "${LOCAL}/bin/bootstrap-argocd.sh"; then
  echo "Argo bootstrap must not accept a caller-selected root manifest" >&2
  exit 1
fi
if grep -Eq 'app-project\.yaml|services-applicationset\.yaml|platform-applicationset\.yaml' \
  "${LOCAL}/bin/bootstrap-argocd.sh"; then
  echo "Argo bootstrap must seed only the root; child projects/ApplicationSets are root-owned" >&2
  exit 1
fi
grep -q 'namespace.*secret-seed' "${LOCAL}/bin/seed-secrets.sh"
ruby -c "${LOCAL}/bin/render-secret-seed-template" >/dev/null
grep -Fq 'seed.schema.json' "${LOCAL}/bin/seed-secrets.sh"

if rg -n 'infrastructure/kind/scripts/(reset|clean-deploy|apply)\.sh|kubectl config use-context|curl |wget |helm repo (add|update)|KUBECONFIG=' "${LOCAL}/bin"; then
  echo "local-production scripts contain a forbidden destructive/context/network path" >&2
  exit 1
fi

echo "ok local-production-structure"
