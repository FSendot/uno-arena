# Provider-Neutral OIDC with Keycloak as the Reference IdP

## Status
Accepted

## Context
Identity must delegate authentication to an external IdP without leaking provider-specific claims or APIs into the UnoArena domain. Local development, `kind` verification, and the faculty CLI also need deterministic test-account provisioning, while production must not expose password-grant or administrative test paths.

## Decision
Implement a provider-neutral OIDC anti-corruption layer. It validates issuer, audience, signature/JWKS, expiry, nonce/state where applicable, and the stable provider subject, then maps `(issuer, subject)` through Identity's unique indexed external-identity mapping into internal `PlayerId`, `SessionId`, roles, and eligibility language. Downstream contexts receive only the signed internal principal and never provider tokens or raw claims.

Use Keycloak as the local and `kind` reference provider. A separately configured test-provisioning adapter may create/ensure accounts and perform the canonical CLI username/password test flow in non-production environments. Production startup/readiness fails if test provisioning, direct password grants, development issuers, or insecure TLS/JWKS settings are enabled. Production configuration remains compatible with standards-compliant managed OIDC providers.

## Consequences
Authentication-provider changes remain behind Identity and do not change bounded-context contracts. OIDC discovery/JWKS caching and rotation, issuer/audience allowlists, callback security, provider availability, claim mapping, and Keycloak realm/bootstrap configuration become production or environment concerns. Test account provisioning is explicit and environment-gated rather than hidden inside the normal authentication path.
