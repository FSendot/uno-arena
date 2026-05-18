# No Service Mesh as Required Component

## Status
Accepted

## Context
The platform can meet its current checkpoint needs without introducing the operational and cognitive overhead of a mandatory service mesh.

## Decision
A service mesh is not a required component. It remains an optional future hardening choice. Private networking, service credentials, service-level auth, and correlation headers are sufficient at the checkpoint level.

## Consequences
The deployment model stays simpler and easier to operate. Mesh-specific platform work is deferred until there is a clear need. Basic identity, routing, and traceability still remain covered.
