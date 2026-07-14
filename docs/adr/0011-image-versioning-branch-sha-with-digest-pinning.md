# Image Versioning: Branch-SHA Tag with Digest Pinning for Deploys

## Status
Accepted

## Context
The checkpoint requires that the same artifact built once in the `deliver` stage is promoted to production without rebuilding. This means staging and production must reference the same image in a reproducible and verifiable way. Image tags are mutable and can be overwritten; digests are immutable and content-addressable.

## Decision
Each service image is tagged as `registry/<service>:<branch>-<commit-sha>` (e.g., `registry/identity:main-abc1234`) for human readability and provenance. Delivery publishes a Linux `amd64` plus `arm64` OCI manifest list and captures the immutable index digest. Runtime Helm values reference that index (`image.digest: sha256:...`), not the mutable tag or one architecture-specific child. The container runtime selects the matching child for its node while staging and later promotion reuse the same multi-architecture artifact. The tag is for humans; the index digest is what actually runs. Local kind builds may build/load only the detected node architecture under a local tag because they are not promoted artifacts.

Apply the same rule to third-party infrastructure: record an exact release and verified multi-architecture index digest, and place the index digest in the workload image reference so normal OCI platform resolution chooses `linux/amd64` or `linux/arm64`. Version tags alone are insufficient because registries may republish them. Do not bake an ARM64 child digest into generic manifests or pre-stage architecture-specific upstream images. If a required publisher has no suitable multi-architecture index, maintain a reviewed architecture-to-digest map and select it from the Kubernetes node architecture before applying resources; ADR-0035 documents the current KurrentDB exception.

## Consequences
Staging runs the exact multi-architecture artifact built by `deliver` regardless of tag mutation, and the same digest can schedule on supported AMD64 or ARM64 nodes. The promotion model is: `deliver` builds and pushes once, captures the index digest, and passes it to environment deploy jobs; production consumes that same digest and packaged chart version rather than rebuilding. Deployment validation rejects a third-party index that lacks either required Linux architecture, a selected child that is not a member of the declared index, or an architecture-specific digest in a generic workload. Semver bumping is deliberately omitted — it adds manual overhead with no benefit for a two-developer team deploying placeholders.
