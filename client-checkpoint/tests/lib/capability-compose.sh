# Compose lifecycle helpers for the capability-stack harness.
# Isolates a single project via `docker compose -p`.
# Teardown is caller-controlled: only tear down a project this harness started
# (or an external project when CAP_TEARDOWN_EXTERNAL=1 is explicitly set).
# Capability overlay profiles EventStore out; up uses resolved `config --services`
# so EventStore is never selected (amd64-only image; GI uses memory).

capability_compose_init() {
  local repo_root="$1"
  local project="$2"
  local host_port="$3"
  local env_file="${4:-.env.example}"

  CAP_REPO_ROOT="$repo_root"
  CAP_COMPOSE_PROJECT="$project"
  CAP_BFF_HOST_PORT="$host_port"
  CAP_ENV_FILE="$env_file"
  CAP_COMPOSE_FILES=(
    -f "$repo_root/docker-compose.local.yml"
    -f "$repo_root/docker-compose.capability.yml"
  )
  export BFF_HOST_PORT="$host_port"
  export COMPOSE_PROJECT_NAME="$project"
}

capability_compose_cmd() {
  docker compose -p "$CAP_COMPOSE_PROJECT" \
    "${CAP_COMPOSE_FILES[@]}" \
    --env-file "$CAP_REPO_ROOT/$CAP_ENV_FILE" \
    "$@"
}

# Resolved active services for capability mode (must not include eventstore).
capability_compose_services() {
  capability_compose_cmd config --services
}

capability_compose_up() {
  local services=()
  local svc
  while IFS= read -r svc; do
    [ -n "$svc" ] || continue
    if [ "$svc" = "eventstore" ]; then
      echo "FAIL: capability compose selected eventstore (amd64-only; omit in capability mode)" >&2
      return 1
    fi
    services+=("$svc")
  done < <(capability_compose_services)

  if [ "${#services[@]}" -eq 0 ]; then
    echo "FAIL: capability compose resolved zero services" >&2
    return 1
  fi

  # Exact harness up selection = resolved config --services (EventStore omitted).
  capability_compose_cmd up -d --build --remove-orphans "${services[@]}"
}

# Extract a host port integer from "0.0.0.0:12345", "[::]:12345", or "12345".
capability_compose_parse_host_port() {
  local mapped="$1"
  local port
  mapped="$(printf '%s' "$mapped" | tr -d '[:space:]')"
  [ -n "$mapped" ] || return 1
  port="${mapped##*:}"
  # Strip brackets leftovers from odd formats.
  port="${port##*]}"
  printf '%s' "$port"
}

# True if PORT is a usable non-zero TCP host port.
capability_compose_port_usable() {
  local port="$1"
  case "$port" in
    ''|*[!0-9]*) return 1 ;;
    0) return 1 ;;
  esac
  [ "$port" -gt 0 ] 2>/dev/null
}

# Pick a free TCP port on 127.0.0.1 via Python (bind :0, read, close).
# Compose 5.x does not materialize host bindings for published=0, so the harness
# always selects a concrete nonzero port (retry on bind collision).
capability_compose_free_host_port() {
  local port
  port="$(python3 - <<'PY'
import socket
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind(("127.0.0.1", 0))
print(s.getsockname()[1])
s.close()
PY
)"
  port="$(printf '%s' "$port" | tr -d '[:space:]')"
  if ! capability_compose_port_usable "$port"; then
    echo "FAIL: free host port selector returned unusable '${port:-empty}'" >&2
    return 1
  fi
  printf '%s\n' "$port"
}

# True if discovered PORT matches the selected nonzero CAP_BFF_HOST_PORT.
capability_compose_port_matches_selected() {
  local port="$1"
  local selected="${CAP_BFF_HOST_PORT:-}"
  capability_compose_port_usable "$port" || return 1
  capability_compose_port_usable "$selected" || return 1
  [ "$port" = "$selected" ]
}

# Print compose ps diagnostics when gateway port discovery fails.
capability_compose_gateway_port_diagnostics() {
  echo "--- compose ps (project=${CAP_COMPOSE_PROJECT:-?}) ---" >&2
  capability_compose_cmd ps -a >&2 2>&1 || true
  echo "--- compose ps -q gateway ---" >&2
  capability_compose_cmd ps -q gateway >&2 2>&1 || true
}

# Resolve the host port published for gateway:8080.
# Inspects live container bindings (docker inspect / docker port / compose port)
# but only accepts a usable port that matches the selected nonzero CAP_BFF_HOST_PORT.
# Never accepts zero / empty. Waits until the gateway container exists and
# NetworkSettings reports the selected HostPort, up to CAP_READY_TIMEOUT_S
# (default 60s) while the dependency graph materializes.
capability_compose_gateway_host_port() {
  local cid="" mapped="" port="" last_port=""
  local selected="${CAP_BFF_HOST_PORT:-}"
  local timeout_s="${CAP_READY_TIMEOUT_S:-60}"
  local elapsed=0
  local interval_s=1

  if ! capability_compose_port_usable "$selected"; then
    echo "FAIL: CAP_BFF_HOST_PORT must be a concrete nonzero port before discovery (got='${selected:-empty}')" >&2
    return 1
  fi

  case "$timeout_s" in
    ''|*[!0-9]*) timeout_s=60 ;;
  esac
  if [ "$timeout_s" -lt 1 ]; then
    timeout_s=60
  fi

  while [ "$elapsed" -lt "$timeout_s" ]; do
    cid="$(capability_compose_cmd ps -q gateway 2>/dev/null | head -n1 | tr -d '[:space:]' || true)"
    if [ -n "$cid" ]; then
      port="$(docker inspect -f '{{with (index .NetworkSettings.Ports "8080/tcp")}}{{with (index . 0)}}{{.HostPort}}{{end}}{{end}}' "$cid" 2>/dev/null || true)"
      port="$(printf '%s' "$port" | tr -d '[:space:]')"
      last_port="$port"
      if capability_compose_port_matches_selected "$port"; then
        printf '%s\n' "$port"
        return 0
      fi
      mapped="$(docker port "$cid" 8080/tcp 2>/dev/null | head -n1 || true)"
      port="$(capability_compose_parse_host_port "$mapped" || true)"
      last_port="${port:-$last_port}"
      if capability_compose_port_matches_selected "$port"; then
        printf '%s\n' "$port"
        return 0
      fi
    fi

    mapped="$(capability_compose_cmd port gateway 8080 2>/dev/null | head -n1 || true)"
    port="$(capability_compose_parse_host_port "$mapped" || true)"
    if capability_compose_port_matches_selected "$port"; then
      printf '%s\n' "$port"
      return 0
    fi
    if [ -n "$port" ]; then
      last_port="$port"
    fi

    sleep "$interval_s"
    elapsed=$((elapsed + interval_s))
  done

  echo "FAIL: could not confirm gateway host port ${selected} within ${timeout_s}s (last='${last_port:-empty}')" >&2
  capability_compose_gateway_port_diagnostics
  return 1
}

# Bring the stack up with a concrete nonzero BFF_HOST_PORT. On bind failure,
# retries with a freshly selected free localhost port when allow_retry=1.
# Never retries with published=0 (Compose 5.x leaves HostPort unbound).
# Usage: capability_compose_up_ready [allow_retry=1]
capability_compose_up_ready() {
  local allow_retry="${1:-1}"
  local attempt=0
  local max_attempts=3
  local up_log
  local resolved
  local next_port

  while [ "$attempt" -lt "$max_attempts" ]; do
    attempt=$((attempt + 1))
    up_log="$(mktemp "${TMPDIR:-/tmp}/uno-cap-up.XXXXXX")"
    if capability_compose_up >"$up_log" 2>&1; then
      rm -f "$up_log"
      resolved="$(capability_compose_gateway_host_port)" || return 1
      CAP_BFF_HOST_PORT="$resolved"
      export BFF_HOST_PORT="$CAP_BFF_HOST_PORT"
      return 0
    fi

    echo "WARN: compose up failed (attempt $attempt/$max_attempts)" >&2
    cat "$up_log" >&2 || true

    # Detect host-port bind conflicts; otherwise fail immediately.
    if ! grep -qiE 'address already in use|bind|port is already allocated|failed to bind' "$up_log" 2>/dev/null; then
      rm -f "$up_log"
      return 1
    fi
    rm -f "$up_log"

    capability_compose_down || true

    if [ "$allow_retry" != "1" ]; then
      return 1
    fi

    # Retry with another concrete free localhost port (never published=0).
    next_port="$(capability_compose_free_host_port)" || return 1
    CAP_BFF_HOST_PORT="$next_port"
    export BFF_HOST_PORT="$next_port"
    echo "==> retrying compose up with free host port $next_port" >&2
  done

  echo "FAIL: compose up exhausted bind retries" >&2
  return 1
}

capability_compose_down() {
  # Always scoped to CAP_COMPOSE_PROJECT only.
  if [ -z "${CAP_COMPOSE_PROJECT:-}" ] || [ -z "${CAP_REPO_ROOT:-}" ]; then
    return 0
  fi
  docker compose -p "$CAP_COMPOSE_PROJECT" \
    -f "$CAP_REPO_ROOT/docker-compose.local.yml" \
    -f "$CAP_REPO_ROOT/docker-compose.capability.yml" \
    --env-file "$CAP_REPO_ROOT/${CAP_ENV_FILE:-.env.example}" \
    down -v --remove-orphans >/dev/null 2>&1 || true
}

capability_compose_config() {
  capability_compose_cmd config
}
