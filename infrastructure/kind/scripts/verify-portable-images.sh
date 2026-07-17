#!/usr/bin/env bash
# NETWORKED/read-only: prove every generic third-party pin is an OCI index with
# Linux AMD64 and ARM64 children before the disposable cluster is reset.
set -euo pipefail

command -v docker >/dev/null 2>&1 || { echo "docker required" >&2; exit 1; }
command -v ruby >/dev/null 2>&1 || { echo "ruby required" >&2; exit 1; }
docker buildx version >/dev/null

images=(
  "postgres:18.4-alpine3.24@sha256:9a8afca54e7861fd90fab5fdf4c42477a6b1cb7d293595148e674e0a3181de15"
  "apache/kafka:4.3.1@sha256:77e3df9054047a88b520d0cc46e16696d3b22022e1d580aeccd2632df6532837"
  "redis:8.8.0-alpine@sha256:9d317178eceac8454a2284a9e6df2466b93c745529947f0cd42a0fa9609d7005"
  "clickhouse/clickhouse-server:26.6.1.1193@sha256:1d1f6508eba2dccce2cee9913907c5f7766327debc57a6b1991f2c9e3176c163"
  "quay.io/keycloak/keycloak:26.7.0@sha256:2eb3cd316835c990e69e26ade292ffa78f6fb0db7d5fc6377463c162e1979ac0"
  "edoburu/pgbouncer:v1.24.1-p1@sha256:3db3d7223e93af52b4116f642951a1a5fa44702a88c2a59cf7562cac19320c9e"
  "quay.io/debezium/connect:3.6.0.Final@sha256:d574a7c9575ed78e2349a034ebdf57a99c516771b3dddb7bbeeb44f912a36e22"
  "quay.io/debezium/server:3.6.0.Final@sha256:accbc0d52bcd53f1fe745c2c4957eea8c39be9fd000fb9c20b7d33cbd6c2bfc2"
  "quay.io/minio/minio:RELEASE.2025-04-22T22-12-26Z@sha256:a1ea29fa28355559ef137d71fc570e508a214ec84ff8083e39bc5428980b015e"
  "quay.io/minio/mc:RELEASE.2025-04-16T18-13-26Z@sha256:aead63c77f9db9107f1696fb08ecb0faeda23729cde94b0f663edf4fe09728e3"
  "grafana/alloy:v1.17.1@sha256:4f6ddc56ffdcf8a6316748fc5162972e20cb301523cac1bb4a31957df733ae9b"
  "grafana/loki:3.7.3@sha256:70b9f699fc9bb868b62f1cfd4f787dfa50242f1fd92e6089787d5d7daea75fe8"
  "grafana/tempo:3.0.2@sha256:cda87c212d8c584dc0b89e337e7ed648a5100feb657e5d528480ee4fa03dbbe3"
  "grafana/grafana:13.1.0@sha256:121a7a9ece6dc10b969f1f96eed64b4f07dfac0d0b8abc070f7cb83bbde86f63"
  "prom/prometheus:v3.13.1@sha256:3c42b892cf723fa54d2f262c37a0e1f80aa8c8ddb1da7b9b0df9455a35a7f893"
  "registry.k8s.io/kube-state-metrics/kube-state-metrics:v2.19.1@sha256:85108987d044b18a098126732f98602df408888c0f7d456241f5abefb9744bc1"
  "docker.io/istio/pilot:1.30.2@sha256:d158739d5286f7899bc039589d248720c2a9b6622d54eeb7a3fdfbb65200c22c"
  "docker.io/istio/install-cni:1.30.2@sha256:b2eb80818fc345e3e9033f424ec7757cbf1a9d9a6494fea79648dab4887f2f7f"
  "docker.io/istio/ztunnel:1.30.2@sha256:64d7c4ea9621fdad66160744dcf76999995fcc0ac399d04aba76d6d0aae72242"
)

for image in "${images[@]}"; do
  docker buildx imagetools inspect --raw "${image}" | ruby -rjson -e '
    image = ARGV.fetch(0)
    index = JSON.parse(STDIN.read)
    manifests = index.fetch("manifests") { abort "#{image}: reference is not a multi-architecture index" }
    platforms = manifests.map do |manifest|
      platform = manifest["platform"] || {}
      [platform["os"], platform["architecture"]] if platform["os"] == "linux"
    end.compact
    %w[amd64 arm64].each do |architecture|
      abort "#{image}: missing linux/#{architecture}" unless platforms.include?(["linux", architecture])
    end
  ' "${image}"
  echo "ok portable image ${image}"
done

echo "ok portable-image-indexes count=${#images[@]}"
