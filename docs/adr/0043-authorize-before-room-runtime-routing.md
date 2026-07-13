# Authorize Before Room Runtime Routing

## Status

Accepted

## Decision

The stable Room process authenticates and authorizes every original inbound request before resolving or forwarding it to a dedicated Room runtime. It classifies the route, validates the exact scoped authority for that route, and rejects failures before pod lookup. Only an authorized request is upgraded with the router credential, pinned `roomId`, and runtime generation for the trusted router-to-pod hop.

For player-private snapshots, the BFF derives `playerId` from the bearer-authenticated Identity principal. Room accepts that identity only behind the Gateway-to-Room credential and still verifies current or reconnect-eligible membership before returning the private hand. Client-supplied query or header identities are never authoritative by themselves.

## Consequences

The router is an authorization boundary inside Room Gameplay, not a transparent reverse proxy. Dedicated runtimes trust the router credential only for their pinned room and generation; they do not reinterpret untrusted caller identity. Timer and other scoped internal routes retain their distinct credentials at the stable boundary rather than being widened to the generic Gateway credential.

## Rejected alternatives

- Routing before authorization allows any network-reachable workload to acquire router-to-runtime authority.
- Giving every runtime all upstream credentials expands secret exposure with the active-room count.
- Trusting `playerId` from a query or header without an authenticated Gateway principal enables private-snapshot impersonation.
