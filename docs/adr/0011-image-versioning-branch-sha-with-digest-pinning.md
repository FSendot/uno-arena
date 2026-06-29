# Image Versioning: Branch-SHA Tag with Digest Pinning for Deploys

## Status
Accepted

## Context
The checkpoint requires that the same artifact built once in the `deliver` stage is promoted to production without rebuilding. This means staging and production must reference the same image in a reproducible and verifiable way. Image tags are mutable and can be overwritten; digests are immutable and content-addressable.

## Decision
Each service image is tagged as `registry/<service>:<branch>-<commit-sha>` (e.g., `registry/identity:main-abc1234`) for human readability and provenance. The fully wired Identity `deliver` job captures the image digest output by `docker push` and stores it as a downstream variable for `deploy-staging`. Runtime Helm values reference the image by digest (`image.digest: sha256:...`), not by tag. The tag is for humans; the digest is what actually runs. Production CI is intentionally not implemented for this checkpoint, but the documented production promotion path would reuse the same digest that passed staging.

## Consequences
Staging runs the exact image built by `deliver` regardless of tag mutation. The promotion model is: `deliver` builds and pushes once, captures the digest, and passes it to environment deploy jobs; production would consume that same digest and packaged chart version rather than rebuilding. Semver bumping is deliberately omitted — it adds manual overhead with no benefit for a two-developer team deploying placeholders.
