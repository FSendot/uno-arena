# Monorepo Change Detection via rules:changes per Service Fragment

## Status
Accepted

## Context
All services live in a single GitLab repository. A push or merge request touching one service must not trigger rebuilds or deploys of unrelated services. GitLab provides two mechanisms: per-job `rules: changes:` path filters, and dynamic child pipelines that compute affected services at runtime. The system has 8 services, all placeholders, operated by two developers.

## Decision
The root pipeline is created only for merge requests targeting `main`, pushes to `main`, and existing tag release flows:

```yaml
workflow:
  rules:
    - if: '$CI_PIPELINE_SOURCE == "merge_request_event" && $CI_MERGE_REQUEST_TARGET_BRANCH_NAME == "main"'
    - if: '$CI_PIPELINE_SOURCE == "push" && $CI_COMMIT_BRANCH == "main"'
    - if: '$CI_COMMIT_TAG'
    - when: never
```

Each service pipeline fragment uses `rules: changes:` scoped to its own source path and to shared pipeline templates. Validation jobs run for merge requests targeting `main` and pushes to `main`:

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
```

Delivery and staging deploy jobs run only for pushes to `main`, with the same path filters. The existing Identity production tag path is preserved: tag pipelines can build and deliver the Identity artifact needed by the manual production jobs. A change to `ci/templates/**` triggers all services, since a shared template change may affect every pipeline. A change scoped to one service folder triggers only that service's jobs.

## Consequences
Change detection is declarative, readable, and auditable without additional tooling. Dynamic child pipelines are not used — they add a "compute affected" job and pipeline generation complexity that is disproportionate for 8 services. If the repository grows beyond ~20 services or introduces complex inter-service dependency graphs, dynamic child pipelines become the better trade-off.
