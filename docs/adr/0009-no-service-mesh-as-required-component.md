# No Service Mesh as Required Component

## Status
Superseded by ADR-0025

## Context
The platform can meet its current checkpoint needs without introducing the operational and cognitive overhead of a mandatory service mesh.

## Decision
For the original checkpoint scope, a service mesh was not required. Private networking, service credentials, service-level auth, and correlation headers were sufficient for that checkpoint.

## Consequences
The production architecture and threat model later introduced mandatory in-cluster mTLS at a workload scale where per-pod sidecars are undesirable. ADR-0025 supersedes this checkpoint decision with Istio Ambient.
