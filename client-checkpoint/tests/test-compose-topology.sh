#!/usr/bin/env bash
# Focused compose topology checks (config only; no stack up / no pull).
# Verifies edge + private networking: only gateway on edge, all services on
# private, concrete BFF host port present, capability project-scoped names.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$REPO_ROOT"

pass=0
fail=0
check() {
  local label="$1"
  shift
  if "$@"; then
    echo "ok - $label"
    pass=$((pass + 1))
  else
    echo "not ok - $label" >&2
    fail=$((fail + 1))
  fi
}

command -v docker >/dev/null 2>&1 || {
  echo "FAIL: docker required for compose topology checks" >&2
  exit 1
}
command -v ruby >/dev/null 2>&1 || {
  echo "FAIL: ruby required for compose topology checks" >&2
  exit 1
}

SELECTED_PORT=61061
PROJECT_NAME="uno-cap-topo-check"

BASE_CFG="$(mktemp "${TMPDIR:-/tmp}/uno-compose-base.XXXXXX.yml")"
CAP_CFG="$(mktemp "${TMPDIR:-/tmp}/uno-compose-cap.XXXXXX.yml")"
trap 'rm -f "$BASE_CFG" "$CAP_CFG"' EXIT

BFF_HOST_PORT="$SELECTED_PORT" docker compose \
  -f docker-compose.local.yml \
  --env-file .env.example \
  config >"$BASE_CFG"

BFF_HOST_PORT="$SELECTED_PORT" COMPOSE_PROJECT_NAME="$PROJECT_NAME" docker compose \
  -p "$PROJECT_NAME" \
  -f docker-compose.local.yml \
  -f docker-compose.capability.yml \
  --env-file .env.example \
  config >"$CAP_CFG"

# Ruby asserts against resolved compose YAML (no stack).
assert_topology() {
  local cfg="$1"
  local mode="$2" # base | capability
  local project="$3"
  local selected="$4"
  ruby -ryaml -e '
cfg_path, mode, project, selected = ARGV
doc = YAML.load_file(cfg_path)
nets = doc.fetch("networks")
svcs = doc.fetch("services")
errors = []

edge = nets["uno_edge"] || nets.find { |k, _| k.to_s.include?("edge") }&.last
priv = nets["uno_private"] || nets.find { |k, _| k.to_s.include?("private") }&.last
errors << "missing uno_edge" unless edge
errors << "missing uno_private" unless priv

if edge
  errors << "uno_edge must not be internal" if edge["internal"] == true
  if mode == "capability"
    want = "#{project}_edge"
    got = edge["name"].to_s
    errors << "capability edge name want=#{want} got=#{got}" unless got == want || got.end_with?("_edge")
  end
end

if priv
  errors << "uno_private must be internal" unless priv["internal"] == true
  if mode == "capability"
    want = "#{project}_private"
    got = priv["name"].to_s
    errors << "capability private name want=#{want} got=#{got}" unless got == want || got.end_with?("_private")
  end
end

def net_keys(svc)
  n = svc["networks"]
  return [] if n.nil?
  return n.map(&:to_s) if n.is_a?(Array)
  n.keys.map(&:to_s)
end

gateway = svcs["gateway"]
errors << "missing gateway" unless gateway
if gateway
  gnets = net_keys(gateway)
  errors << "gateway missing uno_edge (#{gnets.inspect})" unless gnets.any? { |k| k.include?("edge") }
  errors << "gateway missing uno_private (#{gnets.inspect})" unless gnets.any? { |k| k.include?("private") }

  ports = gateway["ports"] || []
  published = ports.map { |p|
    case p
    when Hash then (p["published"] || p["host_ip"]).to_s
    else p.to_s.split(":").first
    end
  }
  unless published.any? { |p| p == selected }
    errors << "gateway published port missing selected=#{selected} got=#{published.inspect}"
  end
  if published.any? { |p| p == "0" || p.empty? }
    errors << "gateway must not publish zero/empty host port (#{published.inspect})"
  end
end

svcs.each do |name, svc|
  next if name == "gateway"
  nkeys = net_keys(svc)
  if nkeys.any? { |k| k.include?("edge") }
    errors << "#{name} must not attach to edge (#{nkeys.inspect})"
  end
  unless nkeys.any? { |k| k.include?("private") }
    errors << "#{name} missing uno_private (#{nkeys.inspect})"
  end
end

if errors.empty?
  exit 0
end
warn errors.join("\n")
exit 1
' "$cfg" "$mode" "$project" "$selected"
}

check "base topology (edge+private, gateway-only edge, port $SELECTED_PORT)" \
  assert_topology "$BASE_CFG" base "" "$SELECTED_PORT"

check "capability topology (project-scoped names, gateway-only edge, port $SELECTED_PORT)" \
  assert_topology "$CAP_CFG" capability "$PROJECT_NAME" "$SELECTED_PORT"

# Capability must omit eventstore from default service set.
if ! grep -qE '^[[:space:]]*eventstore:' "$CAP_CFG"; then
  check "capability config omits eventstore service block" true
else
  # Profiled services may still appear in full config; ensure not in --services.
  CAP_SVCS="$(BFF_HOST_PORT="$SELECTED_PORT" COMPOSE_PROJECT_NAME="$PROJECT_NAME" docker compose \
    -p "$PROJECT_NAME" \
    -f docker-compose.local.yml \
    -f docker-compose.capability.yml \
    --env-file .env.example \
    config --services)"
  if printf '%s\n' "$CAP_SVCS" | grep -qx eventstore; then
    check "capability config --services omits eventstore" false
  else
    check "capability config --services omits eventstore" true
  fi
fi

echo "compose topology: $pass passed, $fail failed"
[ "$fail" -eq 0 ]
