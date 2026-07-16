#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
VALIDATOR="${ROOT}/infrastructure/local-production/bin/validate-argocd-control-plane-application"

degraded_foundation='{
  "metadata":{"name":"uno-arena-local-production-foundations"},
  "status":{
    "sync":{"status":"Synced"},
    "health":{"status":"Degraded"},
    "resources":[{
      "group":"gateway.networking.k8s.io",
      "kind":"HTTPRoute",
      "namespace":"uno-arena",
      "name":"uno-arena-bff",
      "status":"Synced",
      "health":{
        "status":"Degraded",
        "message":"Parent uno-arena-gateway: backend(gateway.uno-arena.svc.cluster.local) not found"
      }
    }]
  }
}'

printf '%s' "${degraded_foundation}" |
  "${VALIDATOR}" foundation foundation-only

if printf '%s' "${degraded_foundation}" |
  "${VALIDATOR}" foundation full >/dev/null 2>&1; then
  echo "full acceptance allowed a missing BFF backend" >&2
  exit 1
fi

unrelated_degradation="${degraded_foundation/uno-arena-bff/other-route}"
if printf '%s' "${unrelated_degradation}" |
  "${VALIDATOR}" foundation foundation-only >/dev/null 2>&1; then
  echo "foundation-only acceptance allowed an unrelated degraded resource" >&2
  exit 1
fi

progressing_foundation="$(printf '%s' "${degraded_foundation}" | ruby -rjson -e '
  document = JSON.parse(STDIN.read)
  document.fetch("status").fetch("health")["status"] = "Progressing"
  print JSON.generate(document)
')"
if printf '%s' "${progressing_foundation}" |
  "${VALIDATOR}" foundation foundation-only >/dev/null 2>&1; then
  echo "foundation-only acceptance allowed a non-Degraded Application" >&2
  exit 1
fi

echo "ok local-production-argocd-control-plane-state-contracts"
