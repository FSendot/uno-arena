#!/usr/bin/env bash
# Explicit networked acquisition step for the offline semantic validators.
set -euo pipefail
CHART="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
command -v docker >/dev/null 2>&1 || { echo "docker required" >&2; exit 1; }
command -v ruby >/dev/null 2>&1 || { echo "ruby required" >&2; exit 1; }

for component in prometheus alertmanager; do
  image="$(ruby -ryaml -e 'puts YAML.safe_load(File.read(ARGV.fetch(0))).dig("images", ARGV.fetch(1))' "${CHART}/values.yaml" "${component}")"
  [[ "${image}" == *@sha256:* ]] || { echo "${component} validator image must be digest-pinned" >&2; exit 1; }
  docker pull "${image}"
done

echo "ok observability-validation-images"
