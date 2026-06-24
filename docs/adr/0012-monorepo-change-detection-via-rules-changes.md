# Monorepo Change Detection via rules:changes per Service Fragment

## Status
Accepted

## Context
All services live in a single GitLab repository. A push touching one service must not trigger rebuilds or deploys of unrelated services. GitLab provides two mechanisms: per-job `rules: changes:` path filters, and dynamic child pipelines that compute affected services at runtime. The system has 8 services, all placeholders, operated by two developers.

## Decision
Each service pipeline fragment uses `rules: changes:` scoped to its own source path and to shared pipeline templates:

```yaml
rules:
  - changes:
    - services/<service-name>/**
    - ci/templates/**
```

A change to `ci/templates/**` triggers all services, since a shared template change may affect every pipeline. A change scoped to one service folder triggers only that service's jobs.

## Consequences
Change detection is declarative, readable, and auditable without additional tooling. Dynamic child pipelines are not used — they add a "compute affected" job and pipeline generation complexity that is disproportionate for 8 services. If the repository grows beyond ~20 services or introduces complex inter-service dependency graphs, dynamic child pipelines become the better trade-off.
