#!/usr/bin/env bash
# Explicit target: apply foundation manifests + generated Kafka topic Job.
# Order: namespace/secrets/datastores → wait datastore readiness → bootstrap/topic Jobs → wait Jobs.
# Job backoff is not consumed by datastore startup.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "${SCRIPT_DIR}/lib.sh"

require_cmd kubectl
require_cmd ruby
require_cmd docker
assert_kind_context

ruby "${SCRIPT_DIR}/render-kafka-topics.rb" >/dev/null

# KurrentDB publishes separate reviewed images rather than one suitable
# AMD64/ARM64 index. Select from the kind node architecture before mutating the
# cluster, then render only disposable temporary manifests.
node="${KIND_CLUSTER_NAME}-control-plane"
node_arch="$(docker exec "${node}" uname -m)"
case "${node_arch}" in
  x86_64|amd64)
    kurrentdb_image="docker.kurrent.io/kurrent-lts/kurrentdb:26.0.3@sha256:b4d0665a78269cd7184971c4d1fad38265277901f3d3730d89dcfba8f3d37fe9"
    ;;
  aarch64|arm64)
    kurrentdb_image="kurrentplatform/kurrentdb:26.0.3-experimental-arm64-10.0-noble@sha256:8498556a8ba7a74f8d4ea31a149b1e5216e167d6884b630a68b3e1eb9e6e870e"
    ;;
  *)
    die "kind node architecture '${node_arch}' has no reviewed KurrentDB 26.0.3 image"
    ;;
esac

render_dir="$(mktemp -d "${TMPDIR:-/tmp}/uno-arena-kind-render.XXXXXX")"
cleanup_render() { rm -rf "${render_dir}"; }
trap cleanup_render EXIT
render_arch_manifest() {
  local source="$1" destination="$2"
  ruby -e '
    image = ARGV.fetch(0)
    input = STDIN.read
    placeholder = "__KURRENTDB_IMAGE_BY_NODE_ARCH__"
    abort "missing KurrentDB image placeholder" unless input.include?(placeholder)
    STDOUT.write(input.gsub(placeholder, image))
  ' "${kurrentdb_image}" <"${source}" >"${destination}"
}
render_arch_manifest "${MANIFESTS_DIR}/01-local-secrets.yaml" "${render_dir}/01-local-secrets.yaml"
render_arch_manifest "${MANIFESTS_DIR}/40-kurrentdb/kurrentdb.yaml" "${render_dir}/40-kurrentdb.yaml"
echo "selected KurrentDB image for node architecture ${node_arch}: ${kurrentdb_image}"

DATASTORE_TIMEOUT="${KIND_DATASTORE_TIMEOUT:-300s}"
JOB_TIMEOUT="${KIND_JOB_TIMEOUT:-300s}"

kubectl apply -f "${MANIFESTS_DIR}/00-namespace.yaml"
kubectl apply -f "${render_dir}/01-local-secrets.yaml"
kubectl apply -f "${MANIFESTS_DIR}/10-postgres"
kubectl apply -f "${MANIFESTS_DIR}/20-redis"
kubectl apply -f "${MANIFESTS_DIR}/30-kafka"
kubectl apply -f "${render_dir}/40-kurrentdb.yaml"
kubectl apply -f "${MANIFESTS_DIR}/50-clickhouse"
# Realm JSON is embedded in the ConfigMap; do not apply the directory (kubectl would treat realm-unoarena.json as a K8s object).
kubectl apply -f "${MANIFESTS_DIR}/60-keycloak/keycloak.yaml"

deployments=(
  postgres-identity
  postgres-room-gameplay
  postgres-tournament
  postgres-ranking
  redis
  kafka
  kurrentdb
  clickhouse
  keycloak
)

for d in "${deployments[@]}"; do
  echo "waiting for datastore deployment/${d} (timeout=${DATASTORE_TIMEOUT})"
  kubectl -n "${KIND_NAMESPACE}" rollout status "deployment/${d}" --timeout="${DATASTORE_TIMEOUT}"
done

kubectl apply -f "${MANIFESTS_DIR}/70-bootstrap"
kubectl apply -f "${GENERATED_DIR}/job-kafka-topics.yaml"

jobs=(
  bootstrap-postgres-identity
  bootstrap-postgres-room-gameplay
  bootstrap-postgres-tournament
  bootstrap-postgres-ranking
  bootstrap-clickhouse-analytics
  bootstrap-kafka-topics
)

for j in "${jobs[@]}"; do
  echo "waiting for bootstrap job/${j} (timeout=${JOB_TIMEOUT})"
  kubectl -n "${KIND_NAMESPACE}" wait --for=condition=complete "job/${j}" --timeout="${JOB_TIMEOUT}"
done

# Debezium Kafka Connect after Kafka + Postgres bootstrap (connect-* topics + CDC pubs).
# Requires image loaded via make kind-load-debezium-connect. Does not claim live CDC delivery.
# Delete completed registration Job so config changes re-run idempotently (Jobs are immutable).
kubectl -n "${KIND_NAMESPACE}" delete job/register-debezium-connectors --ignore-not-found=true
kubectl apply -f "${MANIFESTS_DIR}/80-debezium"
echo "waiting for deployment/debezium-connect (timeout=${DATASTORE_TIMEOUT})"
kubectl -n "${KIND_NAMESPACE}" rollout status "deployment/debezium-connect" --timeout="${DATASTORE_TIMEOUT}"
echo "waiting for job/register-debezium-connectors (timeout=${JOB_TIMEOUT})"
kubectl -n "${KIND_NAMESPACE}" wait --for=condition=complete "job/register-debezium-connectors" --timeout="${JOB_TIMEOUT}"

# Debezium Server (Room realtime → Redis) after Redis + Room Postgres bootstrap.
# Requires image loaded via make kind-load-debezium-server. Does not claim live CDC delivery.
# ConfigMap apply does not bump the Deployment pod template; restart so Server reloads
# mounted startup config (namespace-scoped; does not restart unrelated Deployments).
kubectl apply -f "${MANIFESTS_DIR}/80-debezium-server"
echo "restarting deployment/debezium-server-room-realtime (reload mounted ConfigMap)"
kubectl -n "${KIND_NAMESPACE}" rollout restart "deployment/debezium-server-room-realtime"
echo "waiting for deployment/debezium-server-room-realtime (timeout=${DATASTORE_TIMEOUT})"
kubectl -n "${KIND_NAMESPACE}" rollout status "deployment/debezium-server-room-realtime" --timeout="${DATASTORE_TIMEOUT}"

# Local-only S3-compatible persistence for Loki and Tempo. The bucket Job is
# recreated so changed bootstrap logic runs despite Job immutability.
kubectl apply -f "${MANIFESTS_DIR}/90-observability-storage/minio.yaml"
kubectl -n observability rollout status deployment/minio --timeout="${DATASTORE_TIMEOUT}"
kubectl -n observability delete job/minio-create-observability-buckets --ignore-not-found=true
kubectl apply -f "${MANIFESTS_DIR}/90-observability-storage/job-buckets.yaml"
kubectl -n observability wait --for=condition=complete job/minio-create-observability-buckets --timeout="${JOB_TIMEOUT}"

echo "ok kind-apply namespace=${KIND_NAMESPACE}"
