#!/usr/bin/env bash
# Offline contract checks for kind and external-S3 renders.
set -euo pipefail
CHART="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
HELM="${HELM:-helm}"
command -v "${HELM}" >/dev/null 2>&1 || { echo "helm required" >&2; exit 1; }

"${HELM}" lint "${CHART}" -f "${CHART}/values.kind.yaml" >/dev/null
kind_out="$(${HELM} template uno-observability "${CHART}" -n observability -f "${CHART}/values.kind.yaml")"

for digest in \
  4f6ddc56ffdcf8a6316748fc5162972e20cb301523cac1bb4a31957df733ae9b \
  70b9f699fc9bb868b62f1cfd4f787dfa50242f1fd92e6089787d5d7daea75fe8 \
  cda87c212d8c584dc0b89e337e7ed648a5100feb657e5d528480ee4fa03dbbe3 \
  3c42b892cf723fa54d2f262c37a0e1f80aa8c8ddb1da7b9b0df9455a35a7f893 \
  85108987d044b18a098126732f98602df408888c0f7d456241f5abefb9744bc1 \
  121a7a9ece6dc10b969f1f96eed64b4f07dfac0d0b8abc070f7cb83bbde86f63
do
  grep -Fq "@sha256:${digest}" <<<"${kind_out}"
done

for required in loki-logs tempo-traces 'storage: 5Gi' 'storage: 1Gi' '--storage.tsdb.retention.time=24h' '--storage.tsdb.retention.size=4GB' \
  unoarena_players_created_total unoarena_games_completed_total unoarena_tournaments_completed_total \
  'kind: DaemonSet' 'name: alloy-logs' 'name: alloy-otlp' 'name: kube-state-metrics' 'type: ClusterIP' \
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
[[ "$(grep -c 'timeoutSeconds: 10' <<<"${kind_out}")" -eq 7 ]] || {
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
    "isolate-alloy-logs-ingress" => [[12345]],
    "isolate-grafana-ingress" => [[3000]],
    "allow-loki-clients" => [[3100], [3100], [15008]],
    "allow-tempo-clients" => [[4317], [3200], [3200], [15008]],
    "allow-prometheus-client" => [[9090], [9090], [15008]],
    "allow-kube-state-metrics-client" => [[8080], [8081], [15008]],
    "allow-object-store-clients" => [[9000], [9000], [15008]],
    "allow-application-otlp" => [[12345], [4317, 4318], [15008]],
    "prometheus-application-metrics" => [[8080], [8080], [9090], [15008]],
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
  expected_selectors = %w[alloy-logs alloy-otlp grafana kube-state-metrics loki minio prometheus tempo]
  abort "each observability workload must have one non-overlapping ingress policy: #{selectors.inspect}" unless selectors.sort == expected_selectors.sort
  authorization_policies = documents.select { |document| document["kind"] == "AuthorizationPolicy" }
  abort "AuthorizationPolicy must authorize original destination ports, not HBONE" if authorization_policies.any? { |policy| policy.to_s.include?("15008") }
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
for checksum in alloy-logs-config alloy-otlp-config loki-config tempo-config; do
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
for policy in allow-loki-clients allow-tempo-clients allow-prometheus-client allow-kube-state-metrics-client allow-object-store-clients; do
  grep -Fq "name: ${policy}" <<<"${kind_out}" || {
    echo "missing identity-scoped observability policy: ${policy}" >&2
    exit 1
  }
done
for identity in alloy-logs alloy-otlp loki tempo prometheus kube-state-metrics grafana; do
  grep -Fq "serviceAccountName: ${identity}" <<<"${kind_out}" || {
    echo "observability workload does not use its dedicated identity: ${identity}" >&2
    exit 1
  }
done

! grep -Eq 'type: (NodePort|LoadBalancer)' <<<"${kind_out}"
! grep -Eq 'kind: (PodMonitor|ServiceMonitor|PrometheusRule)' <<<"${kind_out}"

if "${HELM}" template invalid "${CHART}" -f "${CHART}/values.production.yaml" >/dev/null 2>&1; then
  echo "production values without external S3 bucket/region must fail" >&2
  exit 1
fi

! grep -Fq 'kind: Deployment
metadata:
  name: minio' <<<"${production_out}"

echo "ok observability-helm"
