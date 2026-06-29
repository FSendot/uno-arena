# Monorepo Change Detection via rules:changes per Service Fragment

## Status
Accepted

## Context
All services live in a single GitLab repository. A push or merge request touching one service must not trigger rebuilds or deploys of unrelated services. GitLab provides two mechanisms: per-job `rules: changes:` path filters, and dynamic child pipelines that compute affected services at runtime. The system has 8 services, all placeholders, operated by two developers.

## Decision
The root pipeline is created only for merge requests targeting `main`, pushes to `main`, and manual web pipelines on `main`:

```yaml
workflow:
  rules:
    - if: '$CI_PIPELINE_SOURCE == "merge_request_event" && $CI_MERGE_REQUEST_TARGET_BRANCH_NAME == "main"'
    - if: '$CI_PIPELINE_SOURCE == "push" && $CI_COMMIT_BRANCH == "main"'
    - if: '$CI_PIPELINE_SOURCE == "web" && $CI_COMMIT_BRANCH == "main"'
    - when: never
```

Each service pipeline fragment uses `rules: changes:` scoped to its own source path and to shared pipeline templates. Validation jobs run for merge requests targeting `main`, pushes to `main`, and manual web pipelines on `main`:

```yaml
rules:
  - if: '$CI_PIPELINE_SOURCE == "merge_request_event" && $CI_MERGE_REQUEST_TARGET_BRANCH_NAME == "main"'
    changes:
      - services/<service-name>/**
      - ci/templates/**
  - if: '$CI_PIPELINE_SOURCE == "push" && $CI_COMMIT_BRANCH == "main"'
    changes:
      - services/<service-name>/**
      - ci/templates/**
  - if: '$CI_PIPELINE_SOURCE == "web" && $CI_COMMIT_BRANCH == "main" && ($RUN_SERVICE == null || $RUN_SERVICE == "" || $RUN_SERVICE == "all" || $RUN_SERVICE == "<service-name>")'
```

Delivery and staging deploy jobs run for pushes to `main` and manual web pipelines on `main`, with the same service selection rules. Manual web pipelines can set `RUN_SERVICE` to one service name (`identity`, `gateway`, `room-gameplay`, `game-integrity`, `tournament-orchestration`, `ranking`, `spectator-view`, or `analytics`) or `all`; leaving it empty runs all services. Production tag pipelines are intentionally not implemented for this checkpoint; the production promotion path is documented but not executable CI. A change to `ci/templates/**` triggers all services, since a shared template change may affect every pipeline. A change scoped to one service folder triggers only that service's jobs.

## Consequences
Change detection is declarative, readable, and auditable without additional tooling. Dynamic child pipelines are not used — they add a "compute affected" job and pipeline generation complexity that is disproportionate for 8 services. If the repository grows beyond ~20 services or introduces complex inter-service dependency graphs, dynamic child pipelines become the better trade-off.
