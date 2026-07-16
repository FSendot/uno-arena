#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"

for chart in "${ROOT}"/services/*/helm/*; do
  release="$(basename "${chart}")"
  for environment in production local-production; do
    values="${chart}/values.${environment}.yaml"
    [[ -f "${values}" ]] || { echo "missing ${values}" >&2; exit 1; }
    value_args=(-f "${values}")
    if [[ "${environment}" == "local-production" ]]; then
      value_args=(-f "${chart}/values.production.yaml" -f "${values}")
    fi
    rendered="$(helm template "${release}" "${chart}" -n uno-arena "${value_args[@]}" \
      --set image.digest=sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa \
      --set image.tag=)"
    printf '%s' "${rendered}" | ruby -ryaml -e '
      docs = YAML.load_stream(STDIN.read).compact
      release, environment = ARGV
      deploy = docs.find { |d| d["kind"] == "Deployment" && d.dig("metadata", "name") == release }
      abort "#{release}: primary Deployment missing" unless deploy
      spec = deploy.fetch("spec")
      abort "#{release}: unsafe rollout" unless spec.dig("strategy", "rollingUpdate", "maxUnavailable") == 0 && spec.dig("strategy", "rollingUpdate", "maxSurge") == 1
      abort "#{release}: HPA-managed Deployment must omit replicas" if spec.key?("replicas")
      pod = spec.dig("template", "spec")
      abort "#{release}: termination grace missing" unless pod["terminationGracePeriodSeconds"].to_i >= 30
      abort "#{release}: worker node selector missing" unless pod.dig("nodeSelector", "node-role.kubernetes.io/worker") == ""
      abort "#{release}: topology spread missing" unless pod.fetch("topologySpreadConstraints").any? { |c| c["topologyKey"] == "kubernetes.io/hostname" && c["whenUnsatisfiable"] == "DoNotSchedule" }
      container = pod.fetch("containers").first
      abort "#{release}: startup probe must gate on readiness" unless container.dig("startupProbe", "httpGet", "path") == "/ready"
      abort "#{release}: preStop drain missing" unless container.dig("lifecycle", "preStop", "exec", "command")&.join(" ")&.include?("sleep")
      hpa = docs.find { |d| d["kind"] == "HorizontalPodAutoscaler" }
      pdb = docs.find { |d| d["kind"] == "PodDisruptionBudget" }
      abort "#{release}: HPA missing" unless hpa && hpa.dig("spec", "scaleTargetRef", "name") == release
      abort "#{release}: PDB missing" unless pdb && pdb.dig("spec", "minAvailable") == 1
      abort "#{release}: worker/controller HPA rendered" if docs.any? { |d| d["kind"] == "HorizontalPodAutoscaler" && d.dig("metadata", "name") != release }
      sas = docs.select { |d| d["kind"] == "ServiceAccount" }
      abort "#{release}: workload ServiceAccounts missing imagePullSecrets" unless sas.all? { |sa| sa.fetch("imagePullSecrets").any? { |s| s["name"] == "gitlab-registry" } }
      service_accounts = sas.each_with_object({}) { |sa, out| out[sa.dig("metadata", "name")] = sa }
      docs.select { |d| d["kind"] == "Deployment" }.each do |workload|
        workload_name = workload.dig("metadata", "name")
        workload_pod = workload.dig("spec", "template", "spec")
        abort "#{release}: #{workload_name} worker selector missing" unless workload_pod.dig("nodeSelector", "node-role.kubernetes.io/worker") == ""
        abort "#{release}: #{workload_name} topology spread missing" unless workload_pod.fetch("topologySpreadConstraints").any? { |c| c["topologyKey"] == "kubernetes.io/hostname" }
        account_name = workload_pod["serviceAccountName"]
        abort "#{release}: #{workload_name} uses missing/default ServiceAccount" unless account_name && service_accounts.key?(account_name)
      end
    ' "${release}" "${environment}"
  done

  kind_rendered="$(helm template "${release}" "${chart}" -n uno-arena -f "${chart}/values.kind.yaml")"
  if grep -q '^kind: HorizontalPodAutoscaler$' <<<"${kind_rendered}"; then
    echo "${release}: kind must not enable HPA" >&2
    exit 1
  fi
done

echo "ok local-production-service-chart-contracts"
