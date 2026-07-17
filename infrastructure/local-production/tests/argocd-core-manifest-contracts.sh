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
  expected_repo_server = expected_rendered.find do |doc|
    doc["kind"] == "Deployment" && doc.dig("metadata", "name") == "argocd-repo-server"
  end
  expected_container = expected_repo_server&.dig("spec", "template", "spec", "containers")&.find do |item|
    item["name"] == "argocd-repo-server"
  end
  abort "vendored repo-server missing" unless expected_container
  expected_container["startupProbe"] = {
    "httpGet" => { "path" => "/healthz", "port" => 8084 },
    "periodSeconds" => 10,
    "timeoutSeconds" => 10,
    "failureThreshold" => 60
  }
  expected_container.fetch("livenessProbe")["timeoutSeconds"] = 10
  expected_container.fetch("livenessProbe")["failureThreshold"] = 20
  expected_container.fetch("readinessProbe")["timeoutSeconds"] = 10
  expected_container.fetch("readinessProbe")["failureThreshold"] = 10
  expected_container.fetch("env").concat([
    { "name" => "ARGOCD_EXEC_TIMEOUT", "value" => "300s" },
    { "name" => "ARGOCD_EXEC_FATAL_TIMEOUT", "value" => "330s" }
  ])
  expected_container["resources"] = {
    "requests" => { "cpu" => "500m", "memory" => "256Mi" },
    "limits" => { "cpu" => "2", "memory" => "1Gi" }
  }
  expected_server = expected_rendered.find do |doc|
    doc["kind"] == "Deployment" && doc.dig("metadata", "name") == "argocd-server"
  end
  expected_server_container = expected_server&.dig("spec", "template", "spec", "containers")&.find do |item|
    item["name"] == "argocd-server"
  end
  abort "vendored server missing" unless expected_server_container
  expected_server_container["startupProbe"] = {
    "httpGet" => { "path" => "/healthz", "port" => 8080 },
    "periodSeconds" => 10,
    "timeoutSeconds" => 10,
    "failureThreshold" => 60
  }
  expected_server_container.fetch("livenessProbe")["timeoutSeconds"] = 10
  expected_server_container.fetch("livenessProbe")["failureThreshold"] = 10
  expected_server_container.fetch("readinessProbe")["timeoutSeconds"] = 10
  expected_server_container["resources"] = {
    "requests" => { "cpu" => "100m", "memory" => "128Mi" },
    "limits" => { "cpu" => "1", "memory" => "512Mi" }
  }
  expected_application_controller = expected_rendered.find do |doc|
    doc["kind"] == "StatefulSet" && doc.dig("metadata", "name") == "argocd-application-controller"
  end
  expected_application_controller_container = expected_application_controller&.dig("spec", "template", "spec", "containers")&.find do |item|
    item["name"] == "argocd-application-controller"
  end
  abort "vendored application-controller missing" unless expected_application_controller_container
  expected_application_controller_container.fetch("readinessProbe")["timeoutSeconds"] = 10
  expected_application_controller_container.fetch("readinessProbe")["failureThreshold"] = 10
  expected_application_controller_container["resources"] = {
    "requests" => { "cpu" => "250m", "memory" => "256Mi" },
    "limits" => { "cpu" => "1", "memory" => "1Gi" }
  }
  expected_redis = expected_rendered.find do |doc|
    doc["kind"] == "Deployment" && doc.dig("metadata", "name") == "argocd-redis"
  end
  expected_redis_container = expected_redis&.dig("spec", "template", "spec", "containers")&.find do |item|
    item["name"] == "redis"
  end
  abort "vendored Redis missing" unless expected_redis_container
  expected_redis_container["startupProbe"] = {
    "tcpSocket" => { "port" => 6379 },
    "periodSeconds" => 10,
    "timeoutSeconds" => 5,
    "failureThreshold" => 60
  }
  expected_redis_container["readinessProbe"] = {
    "tcpSocket" => { "port" => 6379 },
    "periodSeconds" => 10,
    "timeoutSeconds" => 5,
    "failureThreshold" => 10
  }
  expected_redis_container["livenessProbe"] = {
    "tcpSocket" => { "port" => 6379 },
    "periodSeconds" => 30,
    "timeoutSeconds" => 5,
    "failureThreshold" => 10
  }
  expected_redis_container["resources"] = {
    "requests" => { "cpu" => "250m", "memory" => "64Mi" },
    "limits" => { "cpu" => "1", "memory" => "256Mi" }
  }
  local_control_path = {
    "argocd-repo-server" => "Deployment",
    "argocd-server" => "Deployment",
    "argocd-application-controller" => "StatefulSet",
    "argocd-redis" => "Deployment",
    "argocd-applicationset-controller" => "Deployment"
  }
  expected_rendered.each do |doc|
    name = doc.dig("metadata", "name")
    next unless local_control_path[name] == doc["kind"]
    pod_spec = doc.dig("spec", "template", "spec")
    pod_spec["nodeSelector"] ||= {}
    pod_spec["nodeSelector"]["kubernetes.io/hostname"] = "uno-arena-production-worker"
  end
  abort "renderer changed non-NetworkPolicy Argo resources" unless rendered == expected_rendered
  abort "renderer retained an Argo NetworkPolicy" if rendered.any? { |doc| doc["kind"] == "NetworkPolicy" }

  repo_server = rendered.find do |doc|
    doc["kind"] == "Deployment" && doc.dig("metadata", "name") == "argocd-repo-server"
  end
  container = repo_server&.dig("spec", "template", "spec", "containers")&.find do |item|
    item["name"] == "argocd-repo-server"
  end
  abort "rendered repo-server missing" unless container
  abort "repo-server startup probe does not protect cold startup for ten minutes" unless
    container.dig("startupProbe", "httpGet", "path") == "/healthz" &&
    container.dig("startupProbe", "httpGet", "port") == 8084 &&
    container.dig("startupProbe", "periodSeconds") == 10 &&
    container.dig("startupProbe", "timeoutSeconds") == 10 &&
    container.dig("startupProbe", "failureThreshold") == 60
  abort "repo-server liveness budget is too small for local load" unless
    container.dig("livenessProbe", "timeoutSeconds") == 10 &&
    container.dig("livenessProbe", "failureThreshold").to_i >= 20
  abort "repo-server readiness timeout is too small for local startup pressure" unless
    container.dig("readinessProbe", "timeoutSeconds") == 10 &&
    container.dig("readinessProbe", "failureThreshold").to_i >= 10
  abort "repo-server CPU request missing" unless container.dig("resources", "requests", "cpu") == "500m"
  abort "repo-server memory request missing" unless container.dig("resources", "requests", "memory") == "256Mi"
  abort "repo-server CPU limit missing" unless container.dig("resources", "limits", "cpu") == "2"
  abort "repo-server memory limit missing" unless container.dig("resources", "limits", "memory") == "1Gi"
  environment = container.fetch("env").to_h { |entry| [entry.fetch("name"), entry["value"]] }
  abort "repo-server command timeout is too short for cold local chart pulls" unless
    environment["ARGOCD_EXEC_TIMEOUT"] == "300s" && environment["ARGOCD_EXEC_FATAL_TIMEOUT"] == "330s"

  server = rendered.find do |doc|
    doc["kind"] == "Deployment" && doc.dig("metadata", "name") == "argocd-server"
  end
  server_container = server&.dig("spec", "template", "spec", "containers")&.find do |item|
    item["name"] == "argocd-server"
  end
  abort "rendered server missing" unless server_container
  abort "server startup probe does not protect cold startup for ten minutes" unless
    server_container.dig("startupProbe", "httpGet", "path") == "/healthz" &&
    server_container.dig("startupProbe", "httpGet", "port") == 8080 &&
    server_container.dig("startupProbe", "periodSeconds") == 10 &&
    server_container.dig("startupProbe", "timeoutSeconds") == 10 &&
    server_container.dig("startupProbe", "failureThreshold") == 60
  abort "server liveness budget is too small for local node pressure" unless
    server_container.dig("livenessProbe", "timeoutSeconds") == 10 &&
    server_container.dig("livenessProbe", "failureThreshold").to_i >= 10
  abort "server readiness timeout is too small for local node pressure" unless
    server_container.dig("readinessProbe", "timeoutSeconds") == 10
  abort "server CPU request missing" unless server_container.dig("resources", "requests", "cpu") == "100m"
  abort "server memory request missing" unless server_container.dig("resources", "requests", "memory") == "128Mi"
  abort "server CPU limit missing" unless server_container.dig("resources", "limits", "cpu") == "1"
  abort "server memory limit missing" unless server_container.dig("resources", "limits", "memory") == "512Mi"

  application_controller = rendered.find do |doc|
    doc["kind"] == "StatefulSet" && doc.dig("metadata", "name") == "argocd-application-controller"
  end
  application_controller_container = application_controller&.dig("spec", "template", "spec", "containers")&.find do |item|
    item["name"] == "argocd-application-controller"
  end
  abort "rendered application-controller missing" unless application_controller_container
  abort "application-controller readiness budget is too small for local node pressure" unless
    application_controller_container.dig("readinessProbe", "timeoutSeconds") == 10 &&
    application_controller_container.dig("readinessProbe", "failureThreshold").to_i >= 10
  abort "application-controller CPU request missing" unless
    application_controller_container.dig("resources", "requests", "cpu") == "250m"
  abort "application-controller memory request missing" unless
    application_controller_container.dig("resources", "requests", "memory") == "256Mi"
  abort "application-controller CPU limit missing" unless
    application_controller_container.dig("resources", "limits", "cpu") == "1"
  abort "application-controller memory limit missing" unless
    application_controller_container.dig("resources", "limits", "memory") == "1Gi"

  redis = rendered.find do |doc|
    doc["kind"] == "Deployment" && doc.dig("metadata", "name") == "argocd-redis"
  end
  redis_container = redis&.dig("spec", "template", "spec", "containers")&.find do |item|
    item["name"] == "redis"
  end
  abort "rendered Redis missing" unless redis_container
  %w[startupProbe readinessProbe livenessProbe].each do |probe|
    abort "Redis #{probe} must check TCP 6379" unless redis_container.dig(probe, "tcpSocket", "port") == 6379
    abort "Redis #{probe} timeout is too small" unless redis_container.dig(probe, "timeoutSeconds") == 5
  end
  abort "Redis startup budget is too small" unless redis_container.dig("startupProbe", "failureThreshold") == 60
  abort "Redis CPU request missing" unless redis_container.dig("resources", "requests", "cpu") == "250m"
  abort "Redis memory request missing" unless redis_container.dig("resources", "requests", "memory") == "64Mi"
  abort "Redis CPU limit missing" unless redis_container.dig("resources", "limits", "cpu") == "1"
  abort "Redis memory limit missing" unless redis_container.dig("resources", "limits", "memory") == "256Mi"

  local_control_path.each do |name, kind|
    workload = rendered.find { |doc| doc["kind"] == kind && doc.dig("metadata", "name") == name }
    abort "local Argo workload #{name} missing" unless workload
    abort "local Argo workload #{name} must use the deterministic management worker" unless
      workload.dig("spec", "template", "spec", "nodeSelector", "kubernetes.io/hostname") ==
        "uno-arena-production-worker"
  end
' "${SOURCE}" "${rendered}"

grep -Fq 'render-argocd-core-manifest' "${LOCAL}/bin/install-argocd-core.sh"
grep -Fq -- '--ignore-not-found' "${LOCAL}/bin/install-argocd-core.sh"
grep -Fq 'controller.repo.server.timeout.seconds' "${LOCAL}/bin/install-argocd-core.sh"
grep -Fq 'controller.status.processors' "${LOCAL}/bin/install-argocd-core.sh"
grep -Fq 'controller.operation.processors' "${LOCAL}/bin/install-argocd-core.sh"
grep -Fq 'reposerver.parallelism.limit' "${LOCAL}/bin/install-argocd-core.sh"
grep -Fq '\"controller.status.processors\":\"2\"' "${LOCAL}/bin/install-argocd-core.sh"
grep -Fq '\"controller.operation.processors\":\"1\"' "${LOCAL}/bin/install-argocd-core.sh"
grep -Fq '\"reposerver.parallelism.limit\":\"1\"' "${LOCAL}/bin/install-argocd-core.sh"
grep -Fq '\"applicationsetcontroller.repo.server.timeout.seconds\":\"300\"' "${LOCAL}/bin/install-argocd-core.sh"
grep -Fq '\"server.repo.server.timeout.seconds\":\"300\"' "${LOCAL}/bin/install-argocd-core.sh"
grep -Fq '\"reposerver.git.request.timeout\":\"300s\"' "${LOCAL}/bin/install-argocd-core.sh"
[[ "$(grep -Fc -- '--timeout=600s' "${LOCAL}/bin/install-argocd-core.sh")" -eq 6 ]]
grep -Fq 'get service argocd-redis' "${LOCAL}/bin/install-argocd-core.sh"
grep -Fq "jsonpath='{.spec.clusterIP}'" "${LOCAL}/bin/install-argocd-core.sh"
grep -Fq 'redis_server="${redis_cluster_ip}:6379"' "${LOCAL}/bin/install-argocd-core.sh"
grep -Fq '\"redis.server\":\"${redis_server}\"' "${LOCAL}/bin/install-argocd-core.sh"
grep -Fq 'rollout restart deployment/argocd-repo-server' "${LOCAL}/bin/install-argocd-core.sh"
grep -Fq 'rollout restart deployment/argocd-server' "${LOCAL}/bin/install-argocd-core.sh"
grep -Fq 'rollout restart statefulset/argocd-application-controller' "${LOCAL}/bin/install-argocd-core.sh"
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
