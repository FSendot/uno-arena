# Helm from Pipeline as Deploy Model

## Status
Accepted

## Context
The DevOps checkpoint requires a documented and justified deploy model for staging and production environments. Two options were evaluated: Helm releases applied directly from the GitLab CI pipeline, and GitOps via Argo CD or Flux watching a cluster-state path. The system is a single-cluster deployment across two namespaces (staging and production), operated by a two-person team, with placeholder services for this checkpoint.

## Decision
Use Helm releases applied from the pipeline. The fully wired service's `deploy-staging` job runs `helm upgrade --install` against the cluster using a kubeconfig injected as a masked GitLab CI variable or the local runner host's active Kubernetes context. The readiness gate is `kubectl rollout status`, which blocks the next stage until pods are healthy. Rollback is `helm rollback <release> <revision>`. Production deployment is intentionally documented as a promotion model for this checkpoint, not implemented as executable CI.

## Consequences
The pipeline is the single source of truth for what is deployed. There is no reconciliation loop or drift detection, which is acceptable given the team size and placeholder scope. GitOps would add operational overhead (Argo CD installation, sync observability polling, state repo management) that has no payoff for a two-developer team with one cluster and no governance requirements. If the system grows to multiple clusters or requires audit trails of cluster state changes, migrating to GitOps is the natural next step.
