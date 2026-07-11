# Authoritative Session Validation for State-Changing Commands

## Status
Accepted

## Context
The single-active-session invariant requires a new login or revocation to prevent the superseded session from submitting later mutations. A positive Redis cache entry can remain stale after the authoritative Postgres revocation commits, so using that cache alone would create a window in which the old session still acts.

## Decision
Every state-changing client command must pass an authoritative Identity Postgres session validation before bounded-context dispatch. Redis and gateway-local state may accelerate ACLs, negative lookups, stream closure, and early rejection, but a positive cache hit cannot replace the Postgres validation required to authorize mutation. Public and explicitly low-risk anonymous reads remain outside this path.

The successful authoritative validation is the command's authorization linearization point. A command admitted before a concurrent revocation commit may finish; no command whose validation begins after that commit may be admitted. Identity returns `PlayerId`, `SessionId`, roles, eligibility, and a validation/version marker in the signed internal principal forwarded by the BFF. Downstream services verify the BFF service identity and principal freshness rather than repeating an independent cached authorization decision.

## Consequences
Session correctness does not depend on Redis invalidation timing. `identity.session.invalidated` still closes streams and clears caches quickly, but missed events cannot authorize future mutations. Identity's authoritative validation read path and single context-owned Postgres database must be capacity-planned for command volume with appropriate indexes, connection pooling, bounded timeouts, and fail-closed behavior. The BFF must not silently fall back to cached-positive authorization during Identity/Postgres failure.
