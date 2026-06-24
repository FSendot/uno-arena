# Image Versioning: Branch-SHA Tag with Digest Pinning for Deploys

## Status
Accepted

## Context
The checkpoint requires that the same artifact built once in the `deliver` stage is promoted to production without rebuilding. This means staging and production must reference the same image in a reproducible and verifiable way. Image tags are mutable and can be overwritten; digests are immutable and content-addressable.

## Decision
Each service image is tagged as `registry/<service>:<branch>-<commit-sha>` (e.g., `registry/identity:main-abc1234`) for human readability and provenance. The `deliver` job captures the image digest output by `docker push` and stores it as a CI artifact or downstream variable. Both `deploy-staging` and `deploy-production` Helm values reference the image by digest (`image.digest: sha256:...`), not by tag. The tag is for humans; the digest is what actually runs.

## Consequences
Staging and production are guaranteed to run the same bits regardless of tag mutation. The promotion model is: `deliver` builds and pushes once, captures the digest, and passes it to both environment deploy jobs. No rebuild occurs between environments. Semver bumping is deliberately omitted — it adds manual overhead with no benefit for a two-developer team deploying placeholders.
