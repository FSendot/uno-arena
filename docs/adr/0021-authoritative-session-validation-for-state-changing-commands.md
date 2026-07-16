# Authoritative Session Validation for State-Changing Commands

## Status
Accepted

## Context
The single-active-session invariant requires a new login or revocation to prevent the superseded session from submitting later mutations. A positive Redis cache entry can remain stale after the authoritative Postgres revocation commits, so using that cache alone would create a window in which the old session still acts.

## Decision
Every state-changing client command must pass an authoritative Identity Postgres session validation before bounded-context dispatch. Redis and gateway-local state may accelerate ACLs, negative lookups, stream closure, and early rejection, but a positive cache hit cannot replace the Postgres validation required to authorize mutation. Public and explicitly low-risk anonymous reads remain outside this path.

The successful authoritative validation is the command's authorization linearization point. A command admitted before a concurrent revocation commit may finish; no command whose validation begins after that commit may be admitted. Gateway therefore decodes the bounded envelope and resolves its destination before calling Identity's dedicated, service-authenticated command-authorization route. Ordinary read, `whoami`, and SSE session validation never mint mutation authority.

For this release, command eligibility is exactly the result of successful authoritative validation of an active, non-expired Identity session; there is no independent suspension or account-status policy. A failed, revoked, or expired session cannot mint a proof. For Room mutations, Identity records that result as `eligible=true` and signs it with `PlayerId`, `SessionId`, roles, a validation marker, and the exact authorization context: Room bounded context/audience, `commandId`, command type, resolved route `roomId`, expected-sequence presence and value, and a SHA-256 digest of the canonical JSON payload. Room recomputes and verifies that binding before dispatch. Changing any bound field requires a new authoritative Identity validation. Repeating the exact same command remains governed by Room's durable `commandId` idempotency. Tournament commands still pass authoritative Identity validation through the command route, but never receive or accept the Room-audience proof.

## Principal key rotation

Signed principals carry a non-secret `kid`. Identity signs with one active key; Room verifies a strict keyring containing the current key and, during rotation, one previous key. Key IDs must be unique and all key material remains within the configured size bounds. Unknown key IDs fail closed.

Rotate without an authorization outage in this order:

1. Publish the new key as `internal-principal-hmac-key-current`, retain the old key as `internal-principal-hmac-key-previous`, assign unique current/previous IDs, increment `principalKeyVersion`, and roll every Room role. The runtime controller replaces every dynamic Room runtime whose key-version label differs.
2. Confirm all stable and dynamic Room processes verify the new current plus old previous key, then wait longer than the maximum accepted proof TTL.
3. Roll Identity with the new current key and its current ID as the active signer.
4. After another interval longer than the maximum proof TTL, remove the previous key/ID from Room, increment `principalKeyVersion` again, and roll Room plus all dynamic runtimes.

Never switch Identity's signer before all Room verifiers have the overlap keyring. Kubernetes Secret updates do not update existing process environments; the rollout marker and runtime reconciliation are part of the security operation, not optional metadata.

## Consequences
Session correctness does not depend on Redis invalidation timing. `identity.session.invalidated` still closes streams and clears caches quickly, but missed events cannot authorize future mutations. Identity's authoritative validation read path and single context-owned Postgres database must be capacity-planned for command volume with appropriate indexes, connection pooling, bounded timeouts, and fail-closed behavior. The BFF must not silently fall back to cached-positive authorization during Identity/Postgres failure.
