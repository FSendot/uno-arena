# Client Checkpoint CLI

This directory contains the Client Checkpoint command-line artifact used as the canonical smoke-test entry point for the DevOps checkpoint.

`bin/unoarena` supports the placeholder commands needed by the Identity end-to-end pipeline:

- `unoarena register --user <username> --pass <password> --json`
- `unoarena whoami --user <username> --json`

The CLI reads the target backend from `UNOARENA_API_URL`, which the staging integration job injects from `STAGING_IDENTITY_URL`.
