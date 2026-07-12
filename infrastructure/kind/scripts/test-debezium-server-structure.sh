#!/usr/bin/env bash
# Offline structure checks for Debezium Server (Room realtime → Redis).
# Asserts config shape only. Does NOT claim live Postgres→Redis delivery.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "${SCRIPT_DIR}/lib.sh"

die() {
  echo "FAIL: $*" >&2
  exit 1
}

MANIFEST="${MANIFESTS_DIR}/80-debezium-server/debezium-server-room-realtime.yaml"
LOAD="${SCRIPT_DIR}/load-debezium-server.sh"
STATUS="${SCRIPT_DIR}/status-debezium-server.sh"
LIVE_REDIS="${SCRIPT_DIR}/test-debezium-postgres-to-redis-live.sh"
APPLY="${SCRIPT_DIR}/apply.sh"
WAIT="${SCRIPT_DIR}/wait.sh"

[[ -f "${MANIFEST}" ]] || die "missing ${MANIFEST}"
[[ -f "${LOAD}" ]] || die "missing ${LOAD}"
[[ -f "${STATUS}" ]] || die "missing ${STATUS}"
[[ -f "${LIVE_REDIS}" ]] || die "missing ${LIVE_REDIS}"
bash -n "${LIVE_REDIS}"
grep -qEi 'SSE E2E not claimed|Gateway SSE E2E not claimed|not claim' "${LIVE_REDIS}" \
  || die "live Redis script must disclaim Gateway SSE E2E"

# --- Image: exact quay digest + Never; node-native crictl pull (no kind load / no local runtime tags) ---
ARM64_DIGEST="sha256:65ba00e8de90c437fa4a3b34c6904b1f0235702350e1794006f774b7df1b826b"
MULTIARCH_DIGEST="sha256:d70982832b59186e364ab616fee3f5aec84d419dea14f18df354b55ac0dd1984"
SOURCE_IMAGE="quay.io/debezium/server:3.6.0.Final@${ARM64_DIGEST}"
STALE_RUNTIME_TAG="docker.io/uno-arena/debezium-server:3.6.0.Final-65ba00e8de90"
SHORT_STALE_TAG="uno-arena/debezium-server:3.6.0.Final-65ba00e8de90"

grep -qF "${SOURCE_IMAGE}" "${MANIFEST}" || die "Deployment must use exact source ${SOURCE_IMAGE}"
grep -qF "${SOURCE_IMAGE}" "${LOAD}" || die "load script must stage exact source ${SOURCE_IMAGE}"
python3 - "${MANIFEST}" "${SOURCE_IMAGE}" <<'PY' || die "Deployment must use digest-qualified quay image"
import pathlib, re, sys
text, want = pathlib.Path(sys.argv[1]).read_text(), sys.argv[2]
imgs = re.findall(r"^\s+image:\s+(\S+)\s*$", text, re.M)
if want not in imgs:
    raise SystemExit(f"image: line missing {want}")
if any("@" not in i for i in imgs):
    raise SystemExit("Deployment must use digest (@sha256) image references")
if any(not i.startswith("quay.io/debezium/server:") for i in imgs):
    raise SystemExit("Deployment image must be quay.io/debezium/server:...")
print("ok Server exact-digest image")
PY
grep -qF 'imagePullPolicy: Never' "${MANIFEST}" || die "Server must use imagePullPolicy Never"
grep -qF 'imagePullPolicy: IfNotPresent' "${MANIFEST}" && die "Server must not use IfNotPresent (registry fallback)"
grep -qF "${STALE_RUNTIME_TAG}" "${MANIFEST}" && die "manifest must not reference stale runtime tag ${STALE_RUNTIME_TAG}"
grep -qF "${SHORT_STALE_TAG}" "${MANIFEST}" && die "manifest must not reference short stale tag ${SHORT_STALE_TAG}"
grep -qF "${STALE_RUNTIME_TAG}" "${LOAD}" || die "load script must remove prior stale runtime tag ${STALE_RUNTIME_TAG}"
grep -qF 'RUNTIME_IMAGE' "${MANIFEST}" && die "Server ConfigMap must not claim RUNTIME_IMAGE local tag"
grep -qF 'SOURCE_IMAGE_REF' "${MANIFEST}" || die "ConfigMap must record SOURCE_IMAGE_REF"
grep -qF "${ARM64_DIGEST}" "${MANIFEST}" || die "manifest must record ARM64 source digest"
grep -qF "${MULTIARCH_DIGEST}" "${MANIFEST}" || die "manifest must record multiarch index digest"
# Loader: docker exec + crictl pull exact digest; never kind load.
grep -qF 'docker exec' "${LOAD}" || die "load script must docker exec into kind node"
grep -qF 'crictl pull' "${LOAD}" || die "load script must crictl pull exact digest on node"
grep -qF 'crictl inspecti' "${LOAD}" || die "load script must verify via crictl inspecti"
grep -qF 'arm64' "${LOAD}" || die "load script must verify architecture arm64"
grep -qF 'kind get nodes' "${LOAD}" || die "load script must identify kind node via kind get nodes"
if grep -E 'kind[[:space:]]+load[[:space:]]+docker-image' "${LOAD}" >/dev/null 2>&1; then
  die "load script must not use kind load for Debezium"
fi
if grep -E '^[[:space:]]*docker[[:space:]]+pull\b' "${LOAD}" >/dev/null 2>&1; then
  die "load script must not docker pull on host (node-native crictl pull only)"
fi
if grep -E '^[[:space:]]*docker[[:space:]]+tag\b' "${LOAD}" >/dev/null 2>&1; then
  die "load script must not retag to local runtime tags"
fi
grep -qF 'crictl rmi' "${LOAD}" || die "load script must crictl rmi prior known refs before pull"
grep -qF '65ba00e8de90c437fa4a3b34c6904b1f0235702350e1794006f774b7df1b826b' "${LOAD}" \
  || die "load script must use ARM64 source digest"
# Fail-closed: reject kind-import aliases in repoDigests; do not encode containerd surgery.
grep -qF 'docker.io/library/import-' "${LOAD}" \
  || die "load script must fail closed on docker.io/library/import- repoDigests"
grep -qF '/import-' "${LOAD}" \
  || die "load script must fail closed on any /import- repoDigest"
grep -qF 'stale kind-import metadata' "${LOAD}" \
  || die "load script must name stale kind-import metadata in the error"
grep -qE 'reset/recreate|reset.*recreate' "${LOAD}" \
  || die "load script import rejection must tell operator to reset/recreate the disposable cluster"
grep -qF 'node-native loader' "${LOAD}" \
  || die "load script import rejection must tell operator to rerun the node-native loader"
if grep -Eiq 'ctr[[:space:]]+content|content[[:space:]]+remove|restart[[:space:]]+containerd|containerd[[:space:]]+restart' "${LOAD}"; then
  die "load script must not encode ctr content removal / containerd restart as remediation"
fi

# --- Separate from Connect ---
if grep -Ei 'debezium/connect|connect-distributed|Kafka Connect worker' "${MANIFEST}" >/dev/null 2>&1; then
  die "Debezium Server manifest must not embed Kafka Connect"
fi
if grep -E 'kind:.*KafkaConnect|strimzi\.io/.*[Cc]onnect' "${MANIFEST}" >/dev/null 2>&1; then
  die "Debezium Server manifest must not define Kafka Connect CRDs"
fi
grep -qF 'app.kubernetes.io/component: debezium-server' "${MANIFEST}" \
  || die "must label component debezium-server"
grep -qF 'Separate from Kafka Connect' "${MANIFEST}" \
  || die "header must state Server is separate from Kafka Connect"

# --- Room realtime source identity ---
grep -qF 'postgres-room-gameplay.uno-arena.svc.cluster.local' "${MANIFEST}" \
  || die "must target Room Postgres service"
grep -qF 'database.dbname=room_gameplay' "${MANIFEST}" || die "must use DB room_gameplay"
grep -qF 'room_cdc_realtime' "${MANIFEST}" || die "must use room_cdc_realtime identity"
grep -qF 'room-cdc-realtime-password' "${MANIFEST}" || die "password must come from uno-arena-local-credentials"
grep -qF 'room_cdc_realtime_outbox' "${MANIFEST}" || die "publication must be room_cdc_realtime_outbox"
grep -qF 'realtime_outbox_events' "${MANIFEST}" || die "must capture realtime_outbox_events only"
grep -qF 'slot.name=debezium_server_room_realtime' "${MANIFEST}" \
  || die "must use unique Debezium Server slot debezium_server_room_realtime"
grep -qF 'publication.autocreate.mode=disabled' "${MANIFEST}" \
  || die "must not auto-create publication (bootstrap owns it)"

# --- Redis sink DB2 extended ---
grep -qF 'debezium.sink.type=redis' "${MANIFEST}" || die "sink must be redis"
grep -qF 'redis.uno-arena.svc.cluster.local:6379' "${MANIFEST}" || die "must target kind Redis service"
grep -qF 'debezium.sink.redis.db.index=2' "${MANIFEST}" || die "sink must use Redis DB 2"
grep -qF 'message.format=extended' "${MANIFEST}" || die "must use Redis extended format"

# --- Unique Redis offset key; no unsupported offset db.index; no schema-history store ---
grep -qF 'metadata:debezium:room-realtime:offsets' "${MANIFEST}" \
  || die "offset key must be unique room-realtime key"
grep -qF 'RedisOffsetBackingStore' "${MANIFEST}" || die "must use Redis offset storage"
grep -qF 'offset.storage.redis.db.index' "${MANIFEST}" \
  && die "offset Redis db.index is unsupported and must not be set"
grep -qiE 'schema\.history|SchemaHistory|schema_history' "${MANIFEST}" \
  && die "PostgreSQL insert-only outbox must not configure Redis schema history"

# --- Official format properties (not source.* converter aliases) ---
grep -qF 'debezium.format.key=json' "${MANIFEST}" || die "must set debezium.format.key=json"
grep -qF 'debezium.format.key.schemas.enable=false' "${MANIFEST}" || die "must disable key schemas"
grep -qF 'debezium.format.value=json' "${MANIFEST}" || die "must set debezium.format.value=json"
grep -qF 'debezium.format.value.schemas.enable=false' "${MANIFEST}" || die "must disable value schemas"
grep -qF 'debezium.format.header=json' "${MANIFEST}" || die "must set debezium.format.header=json"
grep -qF 'debezium.format.header.schemas.enable=false' "${MANIFEST}" || die "must disable header schemas"
grep -qE 'debezium\.source\.(key|value)\.converter' "${MANIFEST}" \
  && die "must not use source key/value converter properties"

# --- Outbox router → target_stream + envelope field aliases (no double payload placement) ---
grep -qF 'io.debezium.transforms.outbox.EventRouter' "${MANIFEST}" || die "must use Outbox Event Router"
grep -qF 'route.by.field=target_stream' "${MANIFEST}" || die "must route by target_stream"
# Property-file SmallRye escape: file has \\${routedByValue} → resolved ${routedByValue}.
# Single-quoted -F pattern: two backslashes + literal ${routedByValue}.
grep -qF 'route.topic.replacement=\\${routedByValue}' "${MANIFEST}" \
  || die "Server route.topic.replacement must be \\\\${routedByValue} (SmallRye property-file escape)"
# Reject bare unescaped assignment (NPE under SmallRye when expression is unresolved).
if grep -E '^[[:space:]]*debezium\.transforms\.outbox\.route\.topic\.replacement=\$\{routedByValue\}[[:space:]]*$' \
  "${MANIFEST}" >/dev/null 2>&1; then
  die "Server must not use bare unescaped route.topic.replacement=\${routedByValue}"
fi
grep -qF 'table.field.event.payload=payload' "${MANIFEST}" || die "default payload field must be payload"
grep -qF 'event_id:envelope:eventId' "${MANIFEST}" || die "must map event_id→eventId"
grep -qF 'event_type:envelope:event' "${MANIFEST}" || die "must map event_type→event"
grep -qF 'schema_version:envelope:schemaVersion' "${MANIFEST}" || die "must map schema_version→schemaVersion"
grep -qF 'sequence_number:envelope:sequence' "${MANIFEST}" || die "must map sequence_number→sequence"
grep -qF 'player_id:envelope:playerId' "${MANIFEST}" || die "must map player_id→playerId"
grep -qF 'session_id:envelope:sessionId' "${MANIFEST}" || die "must map session_id→sessionId"
grep -qE 'payload:envelope:(payload|data)' "${MANIFEST}" \
  && die "must not re-place payload via additional.placement (default payload remains)"
grep -qF 'snapshot.mode=no_data' "${MANIFEST}" || die "Server must use snapshot.mode=no_data (insert-only)"
grep -qF 'snapshot.mode=never' "${MANIFEST}" && die "snapshot.mode=never is retired; use no_data"

# --- Probes + config mount ---
grep -qF '/q/health/ready' "${MANIFEST}" || die "readinessProbe must hit Quarkus ready"
grep -qF '/q/health/live' "${MANIFEST}" || die "livenessProbe must hit Quarkus live"
grep -qF 'mountPath: /debezium/config' "${MANIFEST}" || die "must mount config at /debezium/config"

# --- Wiring: apply → Server restart → wait; no false delivery claim ---
grep -qF '80-debezium-server' "${APPLY}" || die "apply.sh must apply 80-debezium-server"
grep -qF 'debezium-server-room-realtime' "${APPLY}" || die "apply.sh must wait for Server rollout"
grep -qF 'debezium-server-room-realtime' "${WAIT}" || die "wait.sh must include Server deployment"
# ConfigMap apply does not restart pods; kind-apply must restart Server only (namespace-scoped).
grep -qE 'kubectl -n "\$\{KIND_NAMESPACE\}" rollout restart "deployment/debezium-server-room-realtime"' \
  "${APPLY}" \
  || die "apply.sh must namespace-scoped rollout restart deployment/debezium-server-room-realtime after apply"
python3 - "${APPLY}" <<'PY' || die "apply.sh must order Server apply → restart → wait"
import pathlib, sys
lines = pathlib.Path(sys.argv[1]).read_text().splitlines()
apply_i = restart_i = wait_i = None
for i, line in enumerate(lines):
    if 'kubectl apply -f "${MANIFESTS_DIR}/80-debezium-server"' in line:
        apply_i = i
    if 'rollout restart "deployment/debezium-server-room-realtime"' in line:
        restart_i = i
    if (
        apply_i is not None
        and wait_i is None
        and 'rollout status "deployment/debezium-server-room-realtime"' in line
    ):
        wait_i = i
if apply_i is None or restart_i is None or wait_i is None:
    raise SystemExit("missing Server apply, restart, or wait")
if not (apply_i < restart_i < wait_i):
    raise SystemExit(f"order must be apply({apply_i}) < restart({restart_i}) < wait({wait_i})")
# Between apply and wait: only this Server restart (no unrelated rollout restart).
for j in range(apply_i + 1, wait_i):
    line = lines[j]
    if "rollout restart" in line and "debezium-server-room-realtime" not in line:
        raise SystemExit(f"unrelated rollout restart between Server apply and wait: line {j}")
if "KIND_NAMESPACE" not in lines[restart_i]:
    raise SystemExit("Server rollout restart must be namespace-scoped (-n KIND_NAMESPACE)")
print("ok Server apply → restart → wait ordering")
PY
# Reject affirmative live-delivery claims; allow explicit disclaimers.
if grep -Ei 'proves[[:space:]]+live|verified[[:space:]]+live[[:space:]]+(postgres|CDC|delivery)|end-to-end[[:space:]]+CDC[[:space:]]+delivery' \
  "${MANIFEST}" "${LOAD}" "${STATUS}" >/dev/null 2>&1; then
  die "scripts/manifests must not falsely claim live Postgres→Redis delivery"
fi
grep -Ei 'no live delivery|not claim|Does not claim' "${STATUS}" >/dev/null 2>&1 \
  || die "status script must disclaim live delivery"

# --- Live Redis proof: unique stream/seq, extended JSON validation, scoped cleanup ---
grep -qF 'room-kind-live-' "${LIVE_REDIS}" || die "live Redis script must use unique room id per run"
grep -qE 'sequence_number|SEQ=' "${LIVE_REDIS}" || die "live Redis script must use a run-scoped sequence"
grep -qF 'payload' "${LIVE_REDIS}" || die "live Redis script must validate payload JSON"
for field in eventId event schemaVersion sequence playerId sessionId; do
  grep -qF "${field}" "${LIVE_REDIS}" || die "live Redis script must validate envelope field ${field}"
done
grep -qE 'DEL "\$\{STREAM\}"|DEL "\$STREAM"' "${LIVE_REDIS}" \
  || die "live Redis script must delete only the unique test stream"

# Live Redis: Python program and captured data must use separate fds.
# Forbid pipe|here-string into `python3 -` plus stdin heredoc (program steals data).
grep -qE 'python3[[:space:]]+/dev/fd/3' "${LIVE_REDIS}" \
  || die "live Redis script must run Python from /dev/fd/3 so stdin carries captured data"
grep -qE '3<<' "${LIVE_REDIS}" \
  || die "live Redis script must feed Python program via fd 3 heredoc (3<<)"
python3 - "${LIVE_REDIS}" <<'PY' || die "live Redis script must not pipe/here-string into python3 - with stdin heredoc"
import pathlib, re, sys
text = pathlib.Path(sys.argv[1]).read_text()
logical = re.sub(r"\\\n", " ", text)
bad = re.compile(
    r"(?:\|\s*|<<<\s*)python3\s+-[^\n]*<<",
    re.M,
)
if bad.search(logical):
    raise SystemExit("pipe/here-string into python3 - plus heredoc steals stdin from captured data")
print("ok live Redis python stdin/fd separation")
PY

# Live Redis: heredoc-fed psql INSERT requires kubectl exec -i (stdin); no tty.
grep -qE 'kubectl[[:space:]].*[[:space:]]exec[[:space:]]+-i[[:space:]].*postgres-room-gameplay' "${LIVE_REDIS}" \
  || die "live Redis script must use kubectl exec -i for Postgres insert"
python3 - "${LIVE_REDIS}" <<'PY' || die "live Redis script must not feed psql via non-interactive kubectl exec heredoc"
import pathlib, re, sys

text = pathlib.Path(sys.argv[1]).read_text()
logical = re.sub(r"\\\n", " ", text)
pattern = re.compile(r"kubectl\b([^\n]*?)\bexec\b([^\n]*?)\bpsql\b([^\n]*?)<<", re.M)
matches = list(pattern.finditer(logical))
if not matches:
    raise SystemExit("expected kubectl exec ... psql ... <<heredoc for Postgres insert")
for m in matches:
    after_exec = m.group(2)
    tokens = []
    for tok in after_exec.split():
        if tok == "--":
            break
        tokens.append(tok)
    has_i = any(t in ("-i", "--stdin") or t in ("-it", "-ti") for t in tokens)
    has_t = any(t in ("-t", "--tty") or t in ("-it", "-ti") for t in tokens)
    if not has_i:
        raise SystemExit(
            "non-interactive kubectl exec + heredoc does not forward SQL to psql; use exec -i"
        )
    if has_t:
        raise SystemExit("kubectl exec for heredoc-fed psql must not allocate a tty (-t)")
print("ok live Redis kubectl exec -i for heredoc-fed psql")
PY

echo "ok debezium-server-structure (config assertions only; no live delivery claim)"
