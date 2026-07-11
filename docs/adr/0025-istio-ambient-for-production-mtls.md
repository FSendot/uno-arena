# Istio Ambient for Production mTLS

## Status
Accepted

## Context
The production topology requires encrypted and mutually authenticated in-cluster traffic between application services, workers, CDC components, and stateful dependencies. Traditional sidecar injection would multiply proxy resources and lifecycle coupling across a workload count designed for very large tournament surges. Private networking and static service credentials alone do not provide uniform workload-level mTLS identity.

## Decision
Use Istio Ambient as a required production platform component. Enroll application, worker, CDC, and datastore namespaces in the ambient data plane. Per-node `ztunnel` provides L4 mTLS, SPIFFE-backed workload identity from dedicated Kubernetes service accounts, L4 authorization, and transport telemetry. Enforce strict mTLS and default-deny authorization policies for enrolled workloads; explicitly allow only documented service-to-service and operator paths.

Do not deploy L7 waypoint proxies by default. Application services continue to own HTTP authorization, input policy, correlation, and domain boundaries. Add a waypoint only when a concrete L7 routing or policy requirement justifies it. External client TLS terminates at the ingress/BFF boundary and is distinct from east-west ambient mTLS.

The local `kind` environment installs and verifies the same ambient L4 topology for integration confidence, while making no claim that a laptop cluster proves target-scale capacity. Envelope encryption for Game Integrity payloads and encrypted persistent volumes/backups remain mandatory complementary controls.

## Consequences
ADR-0009 is superseded. Istio control-plane/CNI/ztunnel availability, certificate issuance and rotation, service-account identity, strict-policy rollout, mTLS verification, and per-node tunnel capacity become production concerns. Avoiding per-pod sidecars reduces workload resource and rollout coupling, but the cluster now depends on ambient networking and must test failure, upgrade, and rollback procedures. L4 mTLS does not replace application authorization or data-at-rest/payload encryption.
