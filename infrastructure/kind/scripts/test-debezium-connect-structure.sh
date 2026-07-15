#!/usr/bin/env bash
# Offline structure checks for Debezium Kafka Connect kind manifests + scripts.
# No cluster, no network, no mutation. Does not claim live Postgres→Kafka delivery.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "${SCRIPT_DIR}/lib.sh"

die() {
  echo "FAIL: $*" >&2
  exit 1
}

CONNECT_MANIFEST="${MANIFESTS_DIR}/80-debezium/connect.yaml"
JOB_MANIFEST="${MANIFESTS_DIR}/80-debezium/job-register-connectors.yaml"
LOAD_SCRIPT="${SCRIPT_DIR}/load-debezium-connect.sh"
STATUS_SCRIPT="${SCRIPT_DIR}/test-debezium-connectors.sh"
STRUCT_SCRIPT="${SCRIPT_DIR}/test-debezium-connect-structure.sh"
LIVE_KAFKA_SCRIPT="${SCRIPT_DIR}/test-debezium-postgres-to-kafka-live.sh"
MULTIARCH_DIGEST="sha256:61d29e5a0316de5dd0a564ec40eaa662d837a05217523e1a1745ecde3d790455"
STALE_RUNTIME_TAG="docker.io/uno-arena/debezium-connect:3.6.0.Final-b7ca129320f4"
SHORT_STALE_TAG="uno-arena/debezium-connect:3.6.0.Final-b7ca129320f4"
SOURCE_IMAGE="quay.io/debezium/connect:3.6.0.Final@${MULTIARCH_DIGEST}"

[[ -f "${CONNECT_MANIFEST}" ]] || die "missing ${CONNECT_MANIFEST}"
[[ -f "${JOB_MANIFEST}" ]] || die "missing ${JOB_MANIFEST}"
[[ -f "${LOAD_SCRIPT}" ]] || die "missing ${LOAD_SCRIPT}"
[[ -f "${STATUS_SCRIPT}" ]] || die "missing ${STATUS_SCRIPT}"
[[ -f "${STRUCT_SCRIPT}" ]] || die "missing ${STRUCT_SCRIPT}"
[[ -f "${LIVE_KAFKA_SCRIPT}" ]] || die "missing ${LIVE_KAFKA_SCRIPT}"

bash -n "${LOAD_SCRIPT}"
bash -n "${STATUS_SCRIPT}"
bash -n "${STRUCT_SCRIPT}"
bash -n "${LIVE_KAFKA_SCRIPT}"
grep -qEi 'consumer delivery is outside|no consumer claim' "${LIVE_KAFKA_SCRIPT}" \
  || die "live Kafka script must scope its evidence to the producer path"

# --- Exact quay digest + Never; node-native crictl pull (no kind load / no local runtime tags) ---
grep -qF "${SOURCE_IMAGE}" "${CONNECT_MANIFEST}" || die "connect Deployment must use exact source ${SOURCE_IMAGE}"
grep -qF "${SOURCE_IMAGE}" "${JOB_MANIFEST}" || die "register Job must use same exact source ${SOURCE_IMAGE}"
grep -qF "${SOURCE_IMAGE}" "${LOAD_SCRIPT}" || die "load script must stage exact source ${SOURCE_IMAGE}"
# Same Connect image string in Deployment and Job.
python3 - "${CONNECT_MANIFEST}" "${JOB_MANIFEST}" "${SOURCE_IMAGE}" <<'PY' || die "Deployment and Job must use identical Connect image"
import pathlib, re, sys
dep, job, want = pathlib.Path(sys.argv[1]).read_text(), pathlib.Path(sys.argv[2]).read_text(), sys.argv[3]
dep_imgs = re.findall(r"^\s+image:\s+(\S+)\s*$", dep, re.M)
job_imgs = re.findall(r"^\s+image:\s+(\S+)\s*$", job, re.M)
if want not in dep_imgs:
    raise SystemExit(f"Deployment image: line missing {want}")
if want not in job_imgs:
    raise SystemExit(f"Job image: line missing {want}")
if any("@" not in i for i in dep_imgs + job_imgs):
    raise SystemExit("Deployment/Job must use digest-qualified (@sha256) image references")
if any(not i.startswith("quay.io/debezium/connect:") for i in dep_imgs + job_imgs):
    raise SystemExit("Deployment/Job image must be quay.io/debezium/connect:...")
print("ok identical Connect exact-digest image on Deployment and Job")
PY
grep -qF 'imagePullPolicy: Never' "${CONNECT_MANIFEST}" || die "connect must use imagePullPolicy Never"
grep -qF 'imagePullPolicy: Never' "${JOB_MANIFEST}" || die "register job must use imagePullPolicy Never"
grep -qF 'imagePullPolicy: IfNotPresent' "${CONNECT_MANIFEST}" && die "connect must not use IfNotPresent (registry fallback)"
grep -qF 'imagePullPolicy: IfNotPresent' "${JOB_MANIFEST}" && die "register job must not use IfNotPresent (registry fallback)"
# Workloads/ConfigMaps must not use stale local runtime tags.
for path in "${CONNECT_MANIFEST}" "${JOB_MANIFEST}"; do
  grep -qF "${STALE_RUNTIME_TAG}" "${path}" && die "${path} must not reference stale runtime tag ${STALE_RUNTIME_TAG}"
  grep -qF "${SHORT_STALE_TAG}" "${path}" && die "${path} must not reference short stale tag ${SHORT_STALE_TAG}"
done
grep -qF 'RUNTIME_IMAGE' "${CONNECT_MANIFEST}" && die "connect ConfigMap must not claim RUNTIME_IMAGE local tag"
grep -qF 'SOURCE_IMAGE_REF' "${CONNECT_MANIFEST}" || die "connect ConfigMap must record SOURCE_IMAGE_REF"
grep -qF "${MULTIARCH_DIGEST}" "${CONNECT_MANIFEST}" || die "connect manifest must document multiarch index digest"
grep -qF "${MULTIARCH_DIGEST}" "${JOB_MANIFEST}" || die "register job must use multiarch index digest"
grep -qF "${MULTIARCH_DIGEST}" "${LOAD_SCRIPT}" || die "load script must stage multiarch index digest"
grep -qF '3.6.0.Final' "${CONNECT_MANIFEST}" || die "connect must pin 3.6.0.Final"
# Loader: docker exec + crictl pull exact digest; never kind load.
grep -qF 'docker exec' "${LOAD_SCRIPT}" || die "load script must docker exec into kind node"
grep -qF 'crictl pull' "${LOAD_SCRIPT}" || die "load script must crictl pull exact digest on node"
grep -qF 'crictl inspecti' "${LOAD_SCRIPT}" || die "load script must verify via crictl inspecti"
grep -qF 'expected_arch="amd64"' "${LOAD_SCRIPT}" || die "load script must resolve AMD64"
grep -qF 'expected_arch="arm64"' "${LOAD_SCRIPT}" || die "load script must resolve ARM64"
grep -qF 'kind get nodes' "${LOAD_SCRIPT}" || die "load script must identify kind node via kind get nodes"
if grep -E 'kind[[:space:]]+load[[:space:]]+docker-image' "${LOAD_SCRIPT}" >/dev/null 2>&1; then
  die "load script must not use kind load for Debezium"
fi
if grep -E '^[[:space:]]*docker[[:space:]]+pull\b' "${LOAD_SCRIPT}" >/dev/null 2>&1; then
  die "load script must not docker pull on host (node-native crictl pull only)"
fi
if grep -E '^[[:space:]]*docker[[:space:]]+tag\b' "${LOAD_SCRIPT}" >/dev/null 2>&1; then
  die "load script must not retag to local runtime tags"
fi
grep -qF "${STALE_RUNTIME_TAG}" "${LOAD_SCRIPT}" || die "load script must remove prior stale runtime tag ${STALE_RUNTIME_TAG}"
grep -qF 'crictl rmi' "${LOAD_SCRIPT}" || die "load script must crictl rmi prior known refs before pull"
# Fail-closed: reject kind-import aliases in repoDigests; do not encode containerd surgery.
grep -qF 'docker.io/library/import-' "${LOAD_SCRIPT}" \
  || die "load script must fail closed on docker.io/library/import- repoDigests"
grep -qF '/import-' "${LOAD_SCRIPT}" \
  || die "load script must fail closed on any /import- repoDigest"
grep -qF 'stale kind-import metadata' "${LOAD_SCRIPT}" \
  || die "load script must name stale kind-import metadata in the error"
grep -qE 'reset/recreate|reset.*recreate' "${LOAD_SCRIPT}" \
  || die "load script import rejection must tell operator to reset/recreate the disposable cluster"
grep -qF 'node-native loader' "${LOAD_SCRIPT}" \
  || die "load script import rejection must tell operator to rerun the node-native loader"
if grep -Eiq 'ctr[[:space:]]+content|content[[:space:]]+remove|restart[[:space:]]+containerd|containerd[[:space:]]+restart' "${LOAD_SCRIPT}"; then
  die "load script must not encode ctr content removal / containerd restart as remediation"
fi

# --- Exactly four connector templates ---
for name in \
  connector-identity-outbox.json \
  connector-room-integration-outbox.json \
  connector-tournament-outbox.json \
  connector-ranking-outbox.json
do
  grep -qF "${name}" "${CONNECT_MANIFEST}" || die "missing connector template key ${name}"
done
# Count connector-*.json keys — exactly 4.
count="$(grep -cE '^[[:space:]]*connector-[a-z0-9-]+\.json:' "${CONNECT_MANIFEST}" || true)"
[[ "${count}" == "4" ]] || die "expected exactly 4 connector-*.json templates, got ${count}"

# --- Outbox router field mappings ---
for field in \
  'table.field.event.id": "event_id"' \
  'table.field.event.key": "partition_key"' \
  'table.field.event.payload": "payload"' \
  'route.by.field": "topic"' \
  'event_type:header:type'
do
  matches="$(grep -cF "${field}" "${CONNECT_MANIFEST}" || true)"
  [[ "${matches}" -ge 4 ]] || die "outbox router mapping ${field} must appear in all 4 connectors (got ${matches})"
done
grep -qF 'table.field.event.timestamp' "${CONNECT_MANIFEST}" && die "outbox timestamp override is invalid for Postgres timestamptz; occurredAt stays in payload"
# Identity/tournament/ranking must not invent occurred_at when absent from schema.
identity_block="$(awk '/connector-identity-outbox.json:/,/connector-room-integration-outbox.json:/' "${CONNECT_MANIFEST}")"
echo "${identity_block}" | grep -qF 'occurred_at' && die "identity connector must not map occurred_at (column absent)"

# --- publication.autocreate disabled; pgoutput; snapshot.no_data; single table ---
autocreate="$(grep -cF '"publication.autocreate.mode": "disabled"' "${CONNECT_MANIFEST}" || true)"
[[ "${autocreate}" == "4" ]] || die "all 4 connectors must disable publication.autocreate (got ${autocreate})"
plugin="$(grep -cF '"plugin.name": "pgoutput"' "${CONNECT_MANIFEST}" || true)"
[[ "${plugin}" == "4" ]] || die "all 4 connectors must use pgoutput"
snap="$(grep -cF '"snapshot.mode": "no_data"' "${CONNECT_MANIFEST}" || true)"
[[ "${snap}" == "4" ]] || die "all 4 connectors must use snapshot.mode=no_data"
grep -qF '"snapshot.mode": "never"' "${CONNECT_MANIFEST}" && die "snapshot.mode=never is retired; use no_data"
grep -qF 'public.integration_outbox_events' "${CONNECT_MANIFEST}" || die "room connector must include integration_outbox_events"
grep -qF 'realtime_outbox' "${CONNECT_MANIFEST}" && die "Kafka Connect templates must not include realtime_outbox"
grep -qiE 'Heartbeat|heartbeat\.action\.query|transforms\.Heartbeat' "${CONNECT_MANIFEST}" && die "no Heartbeat SMT / heartbeat.action.query"
# Guard scripts may mention Heartbeat when rejecting it; forbid actual SMT wiring only.
if grep -E 'transforms\.Heartbeat|heartbeat\.action\.query|type=.io\.debezium\.connector\.postgresql\.transforms\.Heartbeat' "${JOB_MANIFEST}" >/dev/null 2>&1; then
  die "register job must not configure Heartbeat SMT"
fi

# --- Worker converters omitted (connectors set their own); no misleading KEY_CONVERTER ---
if grep -E '^\s+- name: (KEY_CONVERTER|VALUE_CONVERTER|CONNECT_KEY_CONVERTER_SCHEMAS_ENABLE|CONNECT_VALUE_CONVERTER_SCHEMAS_ENABLE)\s*$' \
  "${CONNECT_MANIFEST}" >/dev/null 2>&1; then
  die "Connect worker must not set redundant/uncertain converter env; connectors own converters"
fi

# --- Heap must be explicit and below container memory limit (reject image default -Xmx2G) ---
grep -E '^\s+- name: KAFKA_HEAP_OPTS\s*$' "${CONNECT_MANIFEST}" >/dev/null \
  || die "Connect must set KAFKA_HEAP_OPTS (image default -Xmx2G exceeds the local limit)"
python3 - "${CONNECT_MANIFEST}" <<'PY' || die "Connect KAFKA_HEAP_OPTS must keep -Xmx below memory limit"
import pathlib, re, sys

text = pathlib.Path(sys.argv[1]).read_text()
limit_m = re.search(r"limits:\s*\n\s+memory:\s*(\d+)Mi", text)
if not limit_m:
    raise SystemExit("missing memory limit Mi")
limit_mi = int(limit_m.group(1))
heap = re.search(
    r"- name:\s*KAFKA_HEAP_OPTS\s*\n\s+value:\s*\"([^\"]+)\"",
    text,
)
if not heap:
    raise SystemExit("missing KAFKA_HEAP_OPTS value")
opts = heap.group(1)
if "-Xmx2G" in opts or "-Xmx2g" in opts:
    raise SystemExit("must not use image-default -Xmx2G")
xmx = re.search(r"-Xmx(\d+)([MmGg])", opts)
if not xmx:
    raise SystemExit("KAFKA_HEAP_OPTS must include -XmxNNNM")
n, unit = int(xmx.group(1)), xmx.group(2).upper()
xmx_mi = n * 1024 if unit == "G" else n
if xmx_mi >= limit_mi:
    raise SystemExit(f"-Xmx {xmx_mi}Mi must be below memory limit {limit_mi}Mi")
if not re.search(r"-Xms\d+[Mm]", opts):
    raise SystemExit("KAFKA_HEAP_OPTS must include -Xms")
print(f"ok heap {opts} under limit {limit_mi}Mi")
PY

# --- Publications match bootstrap ---
grep -qF 'identity_cdc_outbox' "${CONNECT_MANIFEST}" || die "identity publication name mismatch"
grep -qF 'room_cdc_integration_outbox' "${CONNECT_MANIFEST}" || die "room integration publication name mismatch"
grep -qF 'tournament_cdc_outbox' "${CONNECT_MANIFEST}" || die "tournament publication name mismatch"
grep -qF 'ranking_cdc_outbox' "${CONNECT_MANIFEST}" || die "ranking publication name mismatch"

# --- Unique slots ---
for slot in identity_outbox_slot room_integration_outbox_slot tournament_outbox_slot ranking_outbox_slot; do
  grep -qF "${slot}" "${CONNECT_MANIFEST}" || die "missing unique slot ${slot}"
done

# --- Connect worker uses existing internal topics RF1 ---
grep -qF 'connect-configs' "${CONNECT_MANIFEST}" || die "Connect must use connect-configs"
grep -qF 'connect-offsets' "${CONNECT_MANIFEST}" || die "Connect must use connect-offsets"
grep -qF 'connect-status' "${CONNECT_MANIFEST}" || die "Connect must use connect-status"
grep -qF 'kafka.uno-arena.svc.cluster.local:9092' "${CONNECT_MANIFEST}" || die "Connect must target kind Kafka DNS"
grep -qF 'CONFIG_STORAGE_REPLICATION_FACTOR' "${CONNECT_MANIFEST}" || die "Connect must set config storage RF1"
grep -qF 'CONNECT_PLUGIN_DISCOVERY' "${CONNECT_MANIFEST}" || die "Connect must configure plugin discovery"
grep -qF 'value: service_load' "${CONNECT_MANIFEST}" || die "Connect must use ServiceLoader-only plugin discovery"
grep -qF 'schemas.enable' "${CONNECT_MANIFEST}" || die "JSON converters must disable schemas"
expand="$(grep -cF '"transforms.outbox.table.expand.json.payload": "true"' "${CONNECT_MANIFEST}" || true)"
[[ "${expand}" == "4" ]] || die "all 4 connectors must expand.json.payload=true (canonical objects; got ${expand})"

# --- Probes ---
grep -qF 'readinessProbe:' "${CONNECT_MANIFEST}" || die "connect readinessProbe required"
grep -qF 'livenessProbe:' "${CONNECT_MANIFEST}" || die "connect livenessProbe required"
grep -qF 'curl' "${CONNECT_MANIFEST}" || die "probes should use curl (available in image)"

# --- Labels ---
grep -qF 'app.kubernetes.io/part-of: uno-arena' "${CONNECT_MANIFEST}" || die "part-of label required"
grep -qF 'uno-arena.local/non-production: "true"' "${CONNECT_MANIFEST}" || die "non-production label required"

# --- Register job: PUT idempotent + fail closed + secrets ---
grep -qF 'PUT' "${JOB_MANIFEST}" || die "register script must PUT connector config"
grep -qF '/connectors/' "${JOB_MANIFEST}" || die "register script must hit /connectors/{name}/config"
grep -qF 'uno-arena-local-credentials' "${JOB_MANIFEST}" || die "register job must use local credentials secret"
grep -qF 'identity-cdc-password' "${JOB_MANIFEST}" || die "register job must mount identity CDC password"
grep -qF 'room-cdc-kafka-password' "${JOB_MANIFEST}" || die "register job must mount room CDC kafka password"
grep -qE 'fail closed|FAILED|fail-closed|failed closed' "${JOB_MANIFEST}" || die "register must fail closed on FAILED status"
grep -qE 'assert_exact_connectors|exactly 4' "${JOB_MANIFEST}" || die "register must assert exactly 4 connectors"
# Status checker exit code must be captured in the else branch (set -e otherwise loses rc).
python3 - "${JOB_MANIFEST}" <<'PY' || die "register job must preserve check_connector_status.py exit code in else branch"
import pathlib, re, sys
text = pathlib.Path(sys.argv[1]).read_text()
# Embedded script lives under register-connectors.sh ConfigMap data.
if "check_connector_status.py" not in text:
    raise SystemExit("missing check_connector_status.py")
# Require: if python3 ...; then ... else rc=$? ... [[ rc -eq 1 ]]
pattern = re.compile(
    r"if\s+python3\s+\"\$\{SCRIPTS_DIR\}/check_connector_status\.py\".*?;\s*then.*?else\s+rc=\$\?.*?\[\[\s*\"\$\{rc\}\"\s*-eq\s*1\s*\]\]",
    re.S,
)
if not pattern.search(text):
    raise SystemExit("status fail-closed must use else rc=$? with rc=1 fatal")
print("ok status else-branch rc capture")
PY
grep -qF 'snapshot.mode must be no_data' "${JOB_MANIFEST}" || die "renderer must reject non-no_data snapshot.mode"

# --- Live status script must not be implied as delivery proof by structure alone ---
grep -qE 'assert_kind_context' "${STATUS_SCRIPT}" || die "live status script must assert kind context"
grep -qEi 'does not claim|no delivery claim|not claim.*delivery|structure only' "${STATUS_SCRIPT}" \
  || die "live status script must note it does not alone prove durable delivery"
grep -qF 'identity-outbox' "${STATUS_SCRIPT}" || die "status script must check identity-outbox"

# --- apply/wait ordering references ---
grep -E 'MANIFESTS_DIR\}/80-debezium"' "${SCRIPT_DIR}/apply.sh" >/dev/null \
  || die "apply.sh must apply manifests/80-debezium (Kafka Connect)"
grep -qF 'debezium-connect' "${SCRIPT_DIR}/wait.sh" || die "wait.sh must wait for debezium-connect"
grep -qF 'register-debezium-connectors' "${SCRIPT_DIR}/wait.sh" || die "wait.sh must wait for register job"
grep -qF 'register-debezium-connectors' "${SCRIPT_DIR}/apply.sh" || die "apply.sh must wait for register job"
# Idempotent re-apply: delete only the registration Job (namespace-scoped) before recreate.
grep -qE 'kubectl -n "\$\{KIND_NAMESPACE\}" delete job/register-debezium-connectors --ignore-not-found' \
  "${SCRIPT_DIR}/apply.sh" \
  || die "apply.sh must delete job/register-debezium-connectors --ignore-not-found before apply"
# Ensure delete is immediately before the 80-debezium apply (not a distant unrelated delete).
python3 - "${SCRIPT_DIR}/apply.sh" <<'PY' || die "apply.sh must delete register job immediately before applying 80-debezium"
import pathlib, sys
lines = pathlib.Path(sys.argv[1]).read_text().splitlines()
delete_i = apply_i = None
for i, line in enumerate(lines):
    if "delete job/register-debezium-connectors" in line and "--ignore-not-found" in line:
        delete_i = i
    if 'kubectl apply -f "${MANIFESTS_DIR}/80-debezium"' in line:
        apply_i = i
if delete_i is None or apply_i is None:
    raise SystemExit("missing delete or apply")
if apply_i != delete_i + 1:
    # Allow a blank line between delete and apply.
    if not (apply_i > delete_i and apply_i <= delete_i + 2 and all(not lines[j].strip() or lines[j].strip().startswith("#") for j in range(delete_i + 1, apply_i))):
        raise SystemExit(f"delete at {delete_i} must immediately precede apply at {apply_i}")
print("ok register job delete immediately before apply")
PY

# --- Live Kafka proof must assert key + canonical envelope (not best-effort id-only) ---
grep -qF 'print.key=true' "${LIVE_KAFKA_SCRIPT}" || die "live Kafka script must print keys"
grep -qF 'partition_key' "${LIVE_KAFKA_SCRIPT}" || die "live Kafka script must validate partition_key as Kafka key"
for field in schemaVersion eventId eventType correlationId occurredAt playerId sessionId; do
  grep -qF "${field}" "${LIVE_KAFKA_SCRIPT}" || die "live Kafka script must validate canonical field ${field}"
done
grep -qiE 'best-effort|best effort' "${LIVE_KAFKA_SCRIPT}" && die "live Kafka script must not use best-effort assertions"

# Live Kafka: Python program and captured data must use separate fds.
# Forbid pipe|here-string into `python3 -` plus stdin heredoc (program steals data).
grep -qE 'python3[[:space:]]+/dev/fd/3' "${LIVE_KAFKA_SCRIPT}" \
  || die "live Kafka script must run Python from /dev/fd/3 so stdin carries captured data"
grep -qE '3<<' "${LIVE_KAFKA_SCRIPT}" \
  || die "live Kafka script must feed Python program via fd 3 heredoc (3<<)"
python3 - "${LIVE_KAFKA_SCRIPT}" <<'PY' || die "live Kafka script must not pipe/here-string into python3 - with stdin heredoc"
import pathlib, re, sys
text = pathlib.Path(sys.argv[1]).read_text()
# Join continued lines so the invocation is one logical statement.
logical = re.sub(r"\\\n", " ", text)
bad = re.compile(
    r"(?:\|\s*|<<<\s*)python3\s+-[^\n]*<<",
    re.M,
)
if bad.search(logical):
    raise SystemExit("pipe/here-string into python3 - plus heredoc steals stdin from captured data")
print("ok live Kafka python stdin/fd separation")
PY

# Live Kafka: heredoc-fed psql INSERT requires kubectl exec -i (stdin); no tty.
grep -qE 'kubectl[[:space:]].*[[:space:]]exec[[:space:]]+-i[[:space:]].*postgres-identity' "${LIVE_KAFKA_SCRIPT}" \
  || die "live Kafka script must use kubectl exec -i for Postgres insert"
python3 - "${LIVE_KAFKA_SCRIPT}" <<'PY' || die "live Kafka script must not feed psql via non-interactive kubectl exec heredoc"
import pathlib, re, sys

text = pathlib.Path(sys.argv[1]).read_text()
logical = re.sub(r"\\\n", " ", text)
# kubectl ... exec [flags] ... psql ... <<heredoc
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
print("ok live Kafka kubectl exec -i for heredoc-fed psql")
PY

echo "ok debezium-connect-structure"
