# Istio Ambient for Production mTLS

## Status
Accepted

## Context
The production topology requires encrypted and mutually authenticated in-cluster traffic between application services, workers, CDC components, and stateful dependencies. Traditional sidecar injection would multiply proxy resources and lifecycle coupling across a workload count designed for very large tournament surges. Private networking and static service credentials alone do not provide uniform workload-level mTLS identity.

## Decision
Use Istio Ambient as a required production platform component. Enroll application, worker, CDC, datastore, and observability namespaces in the ambient data plane. Per-node `ztunnel` provides L4 mTLS, SPIFFE-backed workload identity from dedicated Kubernetes service accounts, L4 authorization, and transport telemetry. Enforce strict mTLS and default-deny authorization policies for enrolled workloads; explicitly allow only documented service-to-service and operator paths. In particular, application metrics on port `9090` allow the Prometheus service-account principal and deny other scrape identities.

Do not deploy L7 waypoint proxies by default. Application services continue to own HTTP authorization, input policy, correlation, and domain boundaries. Add a waypoint only when a concrete L7 routing or policy requirement justifies it. External client TLS terminates at the ingress/BFF boundary and is distinct from east-west ambient mTLS.

The local `kind` environment installs and verifies the same ambient L4 topology for integration confidence, including both the `uno-arena` and `observability` namespaces, while making no claim that a laptop cluster proves target-scale capacity. Its documented plaintext exception applies only to local application-layer database TLS and disposable credentials; east-west mesh transport remains encrypted and mutually authenticated. Acceptance proves that the Prometheus service account can scrape application port `9090` and that an unauthorized workload identity cannot. Envelope encryption for Game Integrity payloads and encrypted persistent volumes/backups remain mandatory complementary controls.

Pin the mesh to Istio `1.30.2`, a supported security-patched release for the kind cluster's Kubernetes 1.36 line. Vendor the official `base`, `istiod`, `cni`, and `ztunnel` Helm chart archives at that exact version and verify their recorded checksums before installation. Workloads reference the verified multi-architecture indexes directly: `docker.io/istio/pilot` `sha256:d158739d5286f7899bc039589d248720c2a9b6622d54eeb7a3fdfbb65200c22c`, `docker.io/istio/install-cni` `sha256:b2eb80818fc345e3e9033f424ec7757cbf1a9d9a6494fea79648dab4887f2f7f`, and `docker.io/istio/ztunnel` `sha256:64d7c4ea9621fdad66160744dcf76999995fcc0ac399d04aba76d6d0aae72242`. Each index contains Linux AMD64 and ARM64 children; the node runtime selects the child. Offline kind verification renders pilot, CNI, and ztunnel and requires the exact digest plus explicit `imagePullPolicy: IfNotPresent` on all three, so clean-cluster pulls work while reruns reuse the node cache. The acceptance lane does not require `istioctl`.

## Consequences
ADR-0009 is superseded. Istio control-plane/CNI/ztunnel availability, certificate issuance and rotation, service-account identity, strict-policy rollout, mTLS verification, and per-node tunnel capacity become production concerns. Avoiding per-pod sidecars reduces workload resource and rollout coupling, but the cluster now depends on ambient networking and must test failure, upgrade, and rollback procedures. L4 mTLS does not replace application authorization or data-at-rest/payload encryption.

## Verification sources

- [Istio supported releases and Kubernetes compatibility](https://istio.io/latest/docs/releases/supported-releases/)
- [Istio Ambient Helm installation](https://istio.io/latest/docs/ambient/install/helm/)
