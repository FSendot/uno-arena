#!/usr/bin/env bash
# Offline contract checks for kind and external-S3 renders.
set -euo pipefail
CHART="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
HELM="${HELM:-helm}"
command -v "${HELM}" >/dev/null 2>&1 || { echo "helm required" >&2; exit 1; }

ruby -ryaml -e '
  values = YAML.safe_load(File.read(ARGV.fetch(0)))
  s3 = values.dig("storage", "s3") || {}
  abort "production Tempo bucket must be nested under storage.s3" unless s3.key?("tempoBucket")
  abort "production values contain a stray storage.tempoBucket" if values.fetch("storage", {}).key?("tempoBucket")
' "${CHART}/values.production.yaml"

"${HELM}" lint "${CHART}" -f "${CHART}/values.kind.yaml" >/dev/null
kind_out="$(${HELM} template uno-observability "${CHART}" -n observability -f "${CHART}/values.kind.yaml")"

if grep -Fq 'name: allow-application-internal' <<<"${kind_out}"; then
  echo "observability must not broaden application mesh access by namespace" >&2
  exit 1
fi

for digest in \
  4f6ddc56ffdcf8a6316748fc5162972e20cb301523cac1bb4a31957df733ae9b \
  70b9f699fc9bb868b62f1cfd4f787dfa50242f1fd92e6089787d5d7daea75fe8 \
  cda87c212d8c584dc0b89e337e7ed648a5100feb657e5d528480ee4fa03dbbe3 \
  3c42b892cf723fa54d2f262c37a0e1f80aa8c8ddb1da7b9b0df9455a35a7f893 \
  51a825c2a40acc3e338fdd00d622e01ec090f72be2b3ea46be0839cd47a4d286 \
  9ca6f929a8abc518d78158757922444f1cf807661a2238f44b28c3cd08a1b652 \
  65645c7bb6a0661892a8b03b89d0743208a18dd2f3f17a54ef4b76fb8e2f2a10 \
  85108987d044b18a098126732f98602df408888c0f7d456241f5abefb9744bc1 \
  121a7a9ece6dc10b969f1f96eed64b4f07dfac0d0b8abc070f7cb83bbde86f63
do
  grep -Fq "@sha256:${digest}" <<<"${kind_out}"
done

for required in loki-logs tempo-traces 'storage: 5Gi' 'storage: 1Gi' '--storage.tsdb.retention.time=24h' '--storage.tsdb.retention.size=4GB' \
  unoarena_players_created_total unoarena_games_completed_total unoarena_tournaments_completed_total \
  http_server_request_duration_seconds_count http_server_request_duration_seconds_bucket http_response_status_code \
  'alert: UnoArenaServiceTargetDown' 'alert: UnoArenaPrometheusRuleEvaluationFailures' \
  'alert: UnoArenaObservabilityDeploymentUnavailable' 'alert: UnoArenaAlertmanagerNotificationFailures' \
  'alert: UnoArenaRedisMemoryPressure' \
  'kind: DaemonSet' 'name: alloy-logs' 'name: alloy-otlp' 'name: kube-state-metrics' \
  'name: alertmanager' 'name: redis-exporter' 'name: alert-webhook-sink' 'type: ClusterIP' \
  'automountServiceAccountToken: false' 'kind: PeerAuthentication' 'kind: AuthorizationPolicy'
do
  grep -Fq -- "${required}" <<<"${kind_out}" || { echo "missing render contract: ${required}" >&2; exit 1; }
done
grep -Fq 'name: GF_PLUGINS_PREINSTALL_DISABLED' <<<"${kind_out}" && grep -Fq 'value: "true"' <<<"${kind_out}" || {
  echo "Grafana must not download optional plugins during deployment" >&2
  exit 1
}

grep -Fq 'regex = "uno-arena"' <<<"${kind_out}" || {
  echo "Alloy must collect only the application namespace and avoid observability feedback loops" >&2
  exit 1
}
grep -Fq 'names = ["uno-arena"]' <<<"${kind_out}" || {
  echo "Alloy must scope Kubernetes discovery at the API source" >&2
  exit 1
}
[[ "$(grep -c 'timeoutSeconds: 10' <<<"${kind_out}")" -eq 10 ]] || {
  echo "kind observability readiness probes must tolerate single-node contention" >&2
  exit 1
}
ruby -ryaml -e '
  deployment = YAML.load_stream($stdin.read).compact.find { |document| document["kind"] == "Deployment" && document.dig("metadata", "name") == "grafana" }
  abort "kind Grafana must avoid a resource-doubling rolling update" unless deployment&.dig("spec", "strategy", "type") == "Recreate"
' <<<"${kind_out}"

! grep -Fq 'kind: NetworkPolicy' <<<"${kind_out}" || {
  echo "kind values must use Ambient authorization without production-CNI NetworkPolicy" >&2
  exit 1
}

ruby -ryaml -e '
  documents = YAML.load_stream($stdin.read).compact
  %w[tempo prometheus].each do |name|
    deployment = documents.find { |document| document["kind"] == "Deployment" && document.dig("metadata", "name") == name }
    abort "#{name} must use Recreate with its single-writer PVC" unless deployment&.dig("spec", "strategy", "type") == "Recreate"
  end
' <<<"${kind_out}"

grep -Fq -- '--resources=pods,deployments' <<<"${kind_out}" || {
  echo "kube-state-metrics must watch only the resources used by the cluster dashboard" >&2
  exit 1
}
grep -Fq 'path: /readyz' <<<"${kind_out}" || {
  echo "kube-state-metrics readiness must use its telemetry ready endpoint" >&2
  exit 1
}
production_out="$(${HELM} template production "${CHART}" -n observability -f "${CHART}/values.production.yaml" \
  --set storage.s3.region=us-east-1 --set storage.s3.lokiBucket=prod-loki --set storage.s3.tempoBucket=prod-tempo)"
local_production_out="$(${HELM} template local-production "${CHART}" -n observability -f "${CHART}/values.local-production.yaml")"

if grep -Fq 'kind: PeerAuthentication' <<<"${local_production_out}"; then
  echo "local production foundations must remain the sole PeerAuthentication owner" >&2
  exit 1
fi
ruby -ryaml -e '
  documents = YAML.load_stream($stdin.read).compact
  workloads = documents.select { |document| %w[Deployment DaemonSet].include?(document["kind"]) }
  expected_waves = {
    "loki" => "1", "tempo" => "1",
    "alloy-logs" => "2", "alloy-otlp" => "2",
    "prometheus" => "3", "kube-state-metrics" => "3", "redis-exporter" => "3",
    "grafana" => "4"
  }
  expected_waves.each do |name, wave|
    workload = workloads.find { |document| document.dig("metadata", "name") == name }
    abort "local production observability sync wave mismatch: #{name}" unless
      workload&.dig("metadata", "annotations", "argocd.argoproj.io/sync-wave") == wave
  end
  expected_pvc_waves = {"tempo-wal" => "1", "prometheus-data" => "3"}
  expected_pvc_waves.each do |name, wave|
    pvc = documents.find { |document| document["kind"] == "PersistentVolumeClaim" && document.dig("metadata", "name") == name }
    abort "local production observability PVC sync wave mismatch: #{name}" unless
      pvc&.dig("metadata", "annotations", "argocd.argoproj.io/sync-wave") == wave
  end
  expected_cpu_limits = {"alloy-logs" => "2", "alloy-otlp" => "2", "tempo" => "2", "grafana" => "1"}
  expected_cpu_limits.each do |name, limit|
    workload = workloads.find { |document| document.dig("metadata", "name") == name }
    abort "local production startup CPU limit mismatch: #{name}" unless
      workload&.dig("spec", "template", "spec", "containers", 0, "resources", "limits", "cpu").to_s == limit
  end
  %w[alloy-logs alloy-otlp].each do |name|
    workload = workloads.find { |document| document.dig("metadata", "name") == name }
    abort "local production Alloy CPU request must prevent startup starvation: #{name}" unless
      workload&.dig("spec", "template", "spec", "containers", 0, "resources", "requests", "cpu").to_s == "250m"
    abort "local production Alloy memory limit must fit the executable working set: #{name}" unless
      workload&.dig("spec", "template", "spec", "containers", 0, "resources", "limits", "memory").to_s == "512Mi"
  end
  local_workloads = workloads.select { |document| %w[Deployment DaemonSet].include?(document["kind"]) }
  abort "local production readiness probes must tolerate laptop contention" unless
    local_workloads.all? do |workload|
      workload.dig("spec", "template", "spec", "containers", 0, "readinessProbe", "timeoutSeconds") == 10
    end
  grafana = workloads.find { |document| document.dig("metadata", "name") == "grafana" }
  grafana_data = grafana&.dig("spec", "template", "spec", "volumes")&.find { |volume| volume["name"] == "data" }
  abort "local production Grafana data must avoid Btrfs migration stalls" unless
    grafana_data&.dig("emptyDir") == {"medium" => "Memory", "sizeLimit" => "128Mi"}
' <<<"${local_production_out}"

if grep -Fq 'name: uno-arena-observability-postsync-evidence' <<<"${kind_out}"; then
  echo "disposable kind must not render the Argo PostSync evidence Job" >&2
  exit 1
fi
assert_postsync_render() {
  local profile="$1"
  ruby -ryaml -e '
    profile=ARGV.fetch(0)
    documents=YAML.load_stream(STDIN.read).compact
    name="uno-arena-observability-postsync-evidence"
    job=documents.find { |document| document["kind"] == "Job" && document.dig("metadata", "name") == name }
    abort "#{profile} PostSync evidence Job missing" unless job
    annotations=job.dig("metadata", "annotations") || {}
    abort "#{profile} evidence Job must be PostSync" unless annotations["argocd.argoproj.io/hook"] == "PostSync"
    abort "#{profile} evidence Job must be retained until the next hook creation" unless annotations["argocd.argoproj.io/hook-delete-policy"] == "BeforeHookCreation"
    abort "#{profile} evidence Job must not use TTL deletion" if job.dig("spec").key?("ttlSecondsAfterFinished")
    abort "#{profile} evidence deadline must tolerate independent service reconciliation" unless job.dig("spec", "activeDeadlineSeconds").to_i >= 1800
    labels=job.dig("metadata", "labels") || {}
    expected_labels={"app.kubernetes.io/name"=>name, "app.kubernetes.io/component"=>"deployment-evidence", "unoarena.io/evidence"=>"postsync"}
    abort "#{profile} evidence labels mismatch" unless expected_labels.all? { |key, value| labels[key] == value }
    pod=job.dig("spec", "template", "spec")
    abort "#{profile} evidence Job must use its dedicated identity" unless pod["serviceAccountName"] == name
    abort "#{profile} evidence Job must not automount a token" unless pod["automountServiceAccountToken"] == false
    container=pod.fetch("containers").fetch(0)
    expected_image="prom/prometheus@sha256:3c42b892cf723fa54d2f262c37a0e1f80aa8c8ddb1da7b9b0df9455a35a7f893"
    abort "#{profile} evidence Job must reuse the pinned Prometheus image" unless container["image"] == expected_image
    script=container.fetch("args").join("\n")
    %w[/health /v1/rooms?limit=1 unoarena%3Ahttp_requests%3Arate5m alertmanager_notifications_total alertmanager_notifications_failed_total evidence_id /api/v2/alerts].each do |contract|
      abort "#{profile} evidence script missing #{contract}" unless script.include?(contract)
    end
    abort "#{profile} evidence Job must not use cluster credentials" if script.match?(/kubectl|kubeconfig/i)
    service_account=documents.find { |document| document["kind"] == "ServiceAccount" && document.dig("metadata", "name") == name }
    abort "#{profile} evidence ServiceAccount missing" unless service_account && service_account["automountServiceAccountToken"] == false
    bindings=documents.select { |document| %w[RoleBinding ClusterRoleBinding].include?(document["kind"]) }
    privileged=bindings.any? { |binding| (binding["subjects"] || []).any? { |subject| subject["kind"] == "ServiceAccount" && subject["name"] == name } }
    abort "#{profile} evidence identity must not receive Kubernetes RBAC" if privileged
    principal="cluster.local/ns/observability/sa/#{name}"
    expected_auth={
      "allow-postsync-evidence-gateway" => ["uno-arena", "gateway", {"ports"=>["8080"]}],
      "allow-postsync-evidence-prometheus" => ["observability", "prometheus", {"ports"=>["9090"]}],
      "allow-postsync-evidence-alertmanager" => ["observability", "alertmanager", {"ports"=>["9093"]}]
    }
    expected_auth.each do |policy_name, (namespace, target, operation)|
      policy=documents.find { |document| document["kind"] == "AuthorizationPolicy" && document.dig("metadata", "name") == policy_name }
      abort "#{profile} evidence mesh policy missing: #{policy_name}" unless policy
      abort "#{profile} evidence mesh target mismatch: #{policy_name}" unless policy.dig("metadata", "namespace") == namespace && policy.dig("spec", "selector", "matchLabels", "app.kubernetes.io/name") == target
      rule=policy.dig("spec", "rules", 0)
      abort "#{profile} evidence principal mismatch: #{policy_name}" unless rule.dig("from", 0, "source", "principals") == [principal]
      abort "#{profile} evidence operation mismatch: #{policy_name}" unless rule.dig("to", 0, "operation") == operation
    end
    if profile == "production"
      network=documents.select { |document| document["kind"] == "NetworkPolicy" }
      gateway_policy=network.find { |policy| policy.dig("metadata", "name") == "allow-postsync-evidence-gateway" }
      gateway_source=gateway_policy&.dig("spec", "ingress", 0, "from", 0)
      abort "production evidence Gateway NetworkPolicy must use exact namespace and pod labels" unless gateway_source == {"namespaceSelector"=>{"matchLabels"=>{"kubernetes.io/metadata.name"=>"observability"}}, "podSelector"=>{"matchLabels"=>{"app.kubernetes.io/name"=>name}}}
      {"allow-prometheus-client"=>9090, "allow-alertmanager-client"=>9093}.each do |policy_name, port|
        policy=network.find { |candidate| candidate.dig("metadata", "name") == policy_name }
        evidence_rule=(policy.dig("spec", "ingress") || []).find { |rule| rule.dig("from", 0, "podSelector", "matchLabels", "app.kubernetes.io/name") == name }
        abort "production evidence NetworkPolicy rule missing: #{policy_name}" unless evidence_rule && evidence_rule["ports"] == [{"protocol"=>"TCP", "port"=>port}]
      end
    end
  ' "${profile}"
}
printf '%s' "${local_production_out}" | assert_postsync_render local-production
printf '%s' "${production_out}" | assert_postsync_render production

grep -Fq 'measure_interval: 1h' <<<"${kind_out}" || {
  echo "kind Tempo compaction measurement must remain outside the acceptance window" >&2
  exit 1
}
grep -Fq 'min_cycle_interval: 1h' <<<"${kind_out}" || {
  echo "kind Tempo compaction cycle must remain outside the acceptance window" >&2
  exit 1
}
grep -Fq 'prune_age: 3h' <<<"${kind_out}" || {
  echo "kind Tempo work prune age must cover at least two compaction measurements" >&2
  exit 1
}
grep -Eq 'checksum/tempo-config: [0-9a-f]{64}' <<<"${kind_out}" || {
  echo "Tempo Deployment must restart when its rendered config changes" >&2
  exit 1
}
grep -Fq 'measure_interval: 1m' <<<"${production_out}" || {
  echo "production Tempo compaction must retain the chart maintenance default" >&2
  exit 1
}
grep -Fq 'prune_age: 1h' <<<"${production_out}" || {
  echo "production Tempo work prune age must retain the chart default" >&2
  exit 1
}

ruby -ryaml -e '
  documents = YAML.load_stream($stdin.read).compact
  expected = {
    "isolate-alloy-logs-ingress" => [[12345], [12345]],
    "isolate-grafana-ingress" => [[3000], [3000]],
    "allow-loki-clients" => [[3100], [3100], [15008]],
    "allow-tempo-clients" => [[4317], [3200], [3200], [3200], [15008]],
    "allow-prometheus-client" => [[9090], [9090], [9090], [15008]],
    "allow-kube-state-metrics-client" => [[8080], [8081], [15008]],
    "allow-object-store-clients" => [[9000], [9000], [15008]],
    "allow-application-otlp" => [[12345], [12345], [4317, 4318], [15008]],
    "allow-alertmanager-client" => [[9093], [9093], [15008]],
    "allow-redis-exporter-client" => [[9121], [15008]],
    "prometheus-application-metrics" => [[8080], [8080], [9090], [15008]],
    "allow-postsync-evidence-gateway" => [[8080]],
  }
  network_policies = documents.select { |document| document["kind"] == "NetworkPolicy" }
  expected.each do |name, expected_rules|
    policy = network_policies.find { |document| document.dig("metadata", "name") == name }
    abort "missing NetworkPolicy #{name}" unless policy
    actual_rules = (policy.dig("spec", "ingress") || []).map do |rule|
      (rule["ports"] || []).map { |port| port["port"] }.sort
    end.sort
    abort "unexpected ports for NetworkPolicy #{name}: #{actual_rules.inspect}" unless actual_rules == expected_rules.map(&:sort).sort
    hbone_rule = (policy.dig("spec", "ingress") || []).find { |rule| (rule["ports"] || []).map { |port| port["port"] } == [15008] }
    abort "HBONE transport rule must not rely on original pod selectors: #{name}" if hbone_rule&.key?("from")
  end
  observability_policies = network_policies.reject { |policy| policy.dig("metadata", "namespace") == "uno-arena" }
  selectors = observability_policies.map { |policy| policy.dig("spec", "podSelector", "matchLabels", "app.kubernetes.io/name") }
  expected_selectors = %w[alertmanager alloy-logs alloy-otlp grafana kube-state-metrics loki minio prometheus redis-exporter tempo]
  abort "each observability workload must have one non-overlapping ingress policy: #{selectors.inspect}" unless selectors.sort == expected_selectors.sort
  authorization_policies = documents.select { |document| document["kind"] == "AuthorizationPolicy" }
  abort "AuthorizationPolicy must authorize original destination ports, not HBONE" if authorization_policies.any? { |policy| policy.to_s.include?("15008") }
  otlp = authorization_policies.find { |policy| policy.dig("metadata", "name") == "allow-application-otlp" }
  principals = otlp&.dig("spec", "rules", 0, "from", 0, "source", "principals") || []
  expected = "cluster.local/ns/uno-arena/sa/room-gameplay-player-stream-compactor"
  abort "player-stream compactor OTLP principal missing" unless principals.include?(expected)
' <<<"${production_out}"

grep -Fq 'replacement = "/var/log/pods/*$1/*.log"' <<<"${kind_out}" || {
  echo "Alloy pod-log path must resolve UID/container/restart-file exactly" >&2
  exit 1
}
grep -Fq 'values = ["filename", "service_name", "stream"]' <<<"${kind_out}" || {
  echo "Alloy must drop automatic high-cardinality and duplicate labels" >&2
  exit 1
}
grep -Fq 'discover_service_name: []' <<<"${kind_out}" || {
  echo "Loki automatic service_name duplication must remain disabled" >&2
  exit 1
}
grep -Fq 'disk_full_threshold: 0.99' <<<"${kind_out}" || {
  echo "kind Loki WAL must retain a safety threshold above the disposable node baseline" >&2
  exit 1
}
grep -Fq 'disk_full_threshold: 0.9' <<<"${production_out}" || {
  echo "production Loki WAL must retain its conservative disk safety threshold" >&2
  exit 1
}
grep -Fq 'name: GOMEMLIMIT' <<<"${kind_out}" || {
  echo "kind Grafana must carry its bounded Go memory target" >&2
  exit 1
}
if grep -Fq 'name: GOMEMLIMIT' <<<"${production_out}"; then
  echo "production Grafana must not inherit the kind-only Go memory target" >&2
  exit 1
fi
for checksum in alloy-logs-config alloy-otlp-config loki-config tempo-config prometheus-config prometheus-rules grafana-config alertmanager-config webhook-sink-config; do
  grep -Fq "checksum/${checksum}:" <<<"${kind_out}" || {
    echo "missing rollout checksum for ${checksum}" >&2
    exit 1
  }
done
ruby -ryaml -e '
  documents = YAML.load_stream($stdin.read).compact
  daemon_set = documents.find { |document| document["kind"] == "DaemonSet" && document.dig("metadata", "name") == "alloy-logs" }
  abort "missing Alloy Logs DaemonSet" unless daemon_set
  pod_security = daemon_set.dig("spec", "template", "spec", "securityContext") || {}
  container = daemon_set.dig("spec", "template", "spec", "containers")&.find { |item| item["name"] == "alloy" }
  security = container&.fetch("securityContext", {}) || {}
  pod_logs = container&.fetch("volumeMounts", [])&.find { |mount| mount["name"] == "pod-logs" }
  abort "Alloy Logs must use RuntimeDefault seccomp" unless pod_security.dig("seccompProfile", "type") == "RuntimeDefault"
  abort "Alloy Logs process must remain non-root" unless pod_security["runAsNonRoot"] == true && security["runAsUser"] == 473 && security["runAsGroup"] == 473
  abort "Alloy Logs needs only supplemental GID 0 to traverse root-group node logs" unless pod_security["supplementalGroups"] == [0]
  abort "Alloy Logs process must remain constrained" unless security["allowPrivilegeEscalation"] == false && security["readOnlyRootFilesystem"] == true && security.dig("capabilities", "drop") == ["ALL"]
  abort "Alloy Logs host mount must remain read-only" unless pod_logs && pod_logs["readOnly"] == true
  abort "kind Alloy Logs must retain 500m burst CPU" unless container.dig("resources", "limits", "cpu") == "500m"
' <<<"${kind_out}"
grep -Fq '{key: service.name, value: service}' <<<"${kind_out}" || {
  echo "Grafana trace-to-logs must map service.name to the Loki service label" >&2
  exit 1
}
grep -Fq '{key: unoarena.component, value: component}' <<<"${kind_out}" || {
  echo "Grafana trace-to-logs must map unoarena.component to the Loki component label" >&2
  exit 1
}
for policy in allow-loki-clients allow-tempo-clients allow-prometheus-client allow-kube-state-metrics-client \
  allow-alloy-logs-metrics allow-alloy-otlp-metrics allow-loki-metrics allow-tempo-metrics allow-grafana-metrics \
  allow-alertmanager-client allow-redis-exporter-client allow-redis-exporter-to-redis allow-alert-webhook-sink-client \
  allow-object-store-clients; do
  grep -Fq "name: ${policy}" <<<"${kind_out}" || {
    echo "missing identity-scoped observability policy: ${policy}" >&2
    exit 1
  }
done
for identity in alloy-logs alloy-otlp loki tempo prometheus alertmanager redis-exporter kube-state-metrics grafana alert-webhook-sink; do
  grep -Fq "serviceAccountName: ${identity}" <<<"${kind_out}" || {
    echo "observability workload does not use its dedicated identity: ${identity}" >&2
    exit 1
  }
done

! grep -Eq 'type: (NodePort|LoadBalancer)' <<<"${kind_out}"
! grep -Eq 'kind: (PodMonitor|ServiceMonitor|PrometheusRule)' <<<"${kind_out}"
! grep -Fq 'kind: Secret' <<<"${kind_out}"

ruby -ryaml -rjson -e '
  documents = YAML.load_stream($stdin.read).compact
  config_names = %w[prometheus-config prometheus-rules alertmanager-config]
  config_names.each do |name|
    config = documents.find { |document| document["kind"] == "ConfigMap" && document.dig("metadata", "name") == name }
    abort "missing #{name}" unless config
    config.fetch("data").each_value { |value| YAML.safe_load(value) }
  end
  rules_config = documents.find { |document| document.dig("metadata", "name") == "prometheus-rules" }
  rule_groups = YAML.safe_load(rules_config.dig("data", "uno-arena-rules.yml")).fetch("groups")
  red_rules = rule_groups.find { |group| group["name"] == "uno-arena-red-recording" }.fetch("rules")
  raw_http_rules = red_rules.select { |rule| rule.fetch("expr").include?("http_server_request_duration_seconds") }
  expected_raw = %w[unoarena:http_errors:rate5m unoarena:http_request_duration_seconds:p95_5m unoarena:http_requests:rate5m]
  abort "HTTP RED raw rule set is incomplete" unless raw_http_rules.map { |rule| rule.fetch("record") }.sort == expected_raw.sort
  abort "HTTP RED raw rules must target only the API scrape job" unless raw_http_rules.all? { |rule| rule.fetch("expr").include?(%q(job="uno-arena-services")) && !rule.fetch("expr").include?("uno-arena-workers") }
  target_alert = rule_groups.flat_map { |group| group.fetch("rules") }.find { |rule| rule["alert"] == "UnoArenaServiceTargetDown" }
  abort "API and worker scrape availability must remain monitored" unless target_alert.fetch("expr").include?(%q(job=~"uno-arena-services|uno-arena-workers"))
  dashboard = documents.find { |document| document.dig("metadata", "name") == "grafana-red-dashboard" }
  red = JSON.parse(dashboard.fetch("data").fetch("red.json"))
  abort "RED dashboard identity mismatch" unless red["uid"] == "uno-arena-red"
  abort "RED dashboard must state its API HTTP scope" unless red["title"] == "Uno Arena - API HTTP RED"
  expressions = red.fetch("panels").flat_map { |panel| panel.fetch("targets", []) }.map { |target| target["expr"] }.compact.join(" ")
  %w[unoarena:http_requests:rate5m unoarena:http_error_ratio:rate5m unoarena:http_request_duration_seconds:p95_5m].each do |metric|
    abort "RED dashboard missing #{metric}" unless expressions.include?(metric)
  end
  checksums = {
    "alloy-logs" => %w[checksum/alloy-logs-config],
    "alloy-otlp" => %w[checksum/alloy-otlp-config],
    "loki" => %w[checksum/loki-config],
    "tempo" => %w[checksum/tempo-config],
    "prometheus" => %w[checksum/prometheus-config checksum/prometheus-rules],
    "grafana" => %w[checksum/grafana-config],
    "alertmanager" => %w[checksum/alertmanager-config],
    "alert-webhook-sink" => %w[checksum/webhook-sink-config]
  }
  checksums.each do |name, keys|
    workload = documents.find { |document| %w[Deployment DaemonSet].include?(document["kind"]) && document.dig("metadata", "name") == name }
    annotations = workload&.dig("spec", "template", "metadata", "annotations") || {}
    missing = keys.reject { |key| annotations[key]&.match?(/\A[0-9a-f]{64}\z/) }
    abort "#{name} config checksum missing: #{missing.inspect}" unless missing.empty?
  end
' <<<"${kind_out}"

grep -Fq 'url: "http://alert-webhook-sink.observability.svc.cluster.local:8080/alerts?receiver=kind"' <<<"${kind_out}"
grep -Fq 'url_file: /etc/alertmanager/receiver/webhook-url' <<<"${production_out}"
grep -Fq 'kind: ExternalSecret' <<<"${production_out}"
grep -Fq 'property: alertmanager-webhook-url' <<<"${production_out}"
! grep -Fq 'alert-webhook-sink' <<<"${production_out}"
! grep -Eq 'https?://[^[:space:]]*(hooks|webhook|slack|pagerduty)' <<<"${production_out}"
grep -Fq 'Alertmanager reads url_file per notification' <<<"${production_out}"
printf '%s' "${production_out}" | ruby -ryaml -e '
  documents=YAML.load_stream(STDIN.read).compact
  deployment=documents.find { |document| document["kind"] == "Deployment" && document.dig("metadata", "name") == "alertmanager" }
  mount=deployment.dig("spec", "template", "spec", "containers", 0, "volumeMounts").find { |entry| entry["name"] == "receiver" }
  volume=deployment.dig("spec", "template", "spec", "volumes").find { |entry| entry["name"] == "receiver" }
  abort "Alertmanager receiver Secret must be directory-mounted for projected rotation" unless mount == {"name"=>"receiver", "mountPath"=>"/etc/alertmanager/receiver", "readOnly"=>true}
  abort "Alertmanager receiver volume must retain the ESO-managed Secret" unless volume == {"name"=>"receiver", "secret"=>{"secretName"=>"uno-arena-observability-alertmanager"}}
'
grep -Fq 'url_file: /etc/alertmanager/receiver/webhook-url' <<<"${local_production_out}"
grep -Fq 'name: alert-webhook-sink' <<<"${local_production_out}"
local_rules="$(ruby -ryaml -e 'document=YAML.load_stream(STDIN.read).compact.find { |item| item.dig("metadata", "name") == "prometheus-rules" }; print document.dig("data", "uno-arena-rules.yml")' <<<"${local_production_out}")"
production_rules="$(ruby -ryaml -e 'document=YAML.load_stream(STDIN.read).compact.find { |item| item.dig("metadata", "name") == "prometheus-rules" }; print document.dig("data", "uno-arena-rules.yml")' <<<"${production_out}")"
[[ "${local_rules}" == "${production_rules}" ]] || {
  echo "local-production must retain production alert semantics" >&2
  exit 1
}

ruby -ryaml -e '
  documents=YAML.load_stream(STDIN.read).compact
  deployment=documents.find { |document| document["kind"] == "Deployment" && document.dig("metadata", "name") == "redis-exporter" }
  env=deployment.dig("spec", "template", "spec", "containers", 0, "env").find { |entry| entry["name"] == "REDIS_ADDR" }
  expected={"name"=>"REDIS_ADDR", "value"=>"redis://redis.uno-arena.svc.cluster.local:6379"}
  abort "kind Redis exporter must retain the in-cluster endpoint" unless env == expected
' <<<"${kind_out}"
ruby -ryaml -e '
  documents=YAML.load_stream(STDIN.read).compact
  deployment=documents.find { |document| document["kind"] == "Deployment" && document.dig("metadata", "name") == "redis-exporter" }
  env=deployment.dig("spec", "template", "spec", "containers", 0, "env").find { |entry| entry["name"] == "REDIS_ADDR" }
  expected={"name"=>"REDIS_ADDR", "valueFrom"=>{"secretKeyRef"=>{"name"=>"platform-endpoints", "key"=>"redis-url"}}}
  abort "production Redis exporter must consume the external endpoint Secret" unless env == expected
' <<<"${production_out}"

if "${HELM}" template invalid-redis "${CHART}" -f "${CHART}/values.kind.yaml" \
  --set redisExporter.existingSecret=platform-endpoints >/dev/null 2>&1; then
  echo "Redis exporter address and Secret must be mutually exclusive" >&2
  exit 1
fi
if "${HELM}" template invalid-redis "${CHART}" -f "${CHART}/values.kind.yaml" \
  --set redisExporter.address= --set redisExporter.existingSecret= >/dev/null 2>&1; then
  echo "Redis exporter endpoint configuration must fail closed" >&2
  exit 1
fi

if "${HELM}" template invalid "${CHART}" -f "${CHART}/values.production.yaml" >/dev/null 2>&1; then
  echo "production values without external S3 bucket/region must fail" >&2
  exit 1
fi

! grep -Fq 'kind: Deployment
metadata:
  name: minio' <<<"${production_out}"

grep -Fq -- '--pull=never --network=none' "${CHART}/validation/check-config.sh" || {
  echo "semantic config validation must remain offline and fail closed" >&2
  exit 1
}
if grep -Eq 'docker[[:space:]]+pull' "${CHART}/validation/check-config.sh"; then
  echo "ordinary semantic config validation must not acquire images" >&2
  exit 1
fi
grep -Eq 'docker[[:space:]]+pull' "${CHART}/validation/fetch-images.sh" || {
  echo "validator image acquisition must remain an explicit operator step" >&2
  exit 1
}
"${CHART}/validation/check-config.sh"

echo "ok observability-helm"
