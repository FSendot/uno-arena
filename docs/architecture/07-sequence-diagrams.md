# 07 Sequence Diagrams

## 1. Gameplay Command With Durable Append

```mermaid
sequenceDiagram
    participant C as Client
    participant B as BFF
    participant I as Identity
    participant R as Room Gameplay
    participant G as Game Integrity
    participant K as Kafka / Outbox
    participant S as SSE Stream

    C->>B: POST /commands {commandId,type,expectedSequenceNumber,payload}
    B->>I: Authenticate and authorize
    I-->>B: OK
    B->>R: Dispatch command envelope
    R->>R: Validate room rules and sequence number
    R->>G: Append technical log entry
    G-->>R: Append confirmed
    R->>R: Commit snapshot + outbox with logOffset
    R-->>K: Publish sanitized feed/projection event
    K-->>B: Deliver player/spectator-safe update
    R-->>B: Command accepted/rejected result
    B-->>S: SSE room update
    S-->>C: Realtime state
```

### Image Asset Brief

Prompt brief:

> Polished sequence diagram illustration for a multiplayer game platform. Show client, BFF, identity, gameplay, integrity, and SSE stream with a strict append-before-broadcast flow. Make the visual crisp, minimal, and editorial, with strong directional arrows and emphasis on idempotent command envelopes.

## 2. Session Invalidation Closes SSE

```mermaid
sequenceDiagram
    participant IdP as External IdP
    participant I as Identity
    participant B as BFF
    participant C as Client
    participant S as SSE Stream

    IdP->>I: New login or revocation
    I->>I: Invalidate previous session
    I-->>B: SessionInvalidated(control)
    B-->>S: Close stream
    S-->>C: Disconnect / reauth required
```

## 3. Tournament Advancement

```mermaid
sequenceDiagram
    participant R as Room Gameplay
    participant T as Tournament Orchestration
    participant W as Sharded Workers
    participant P as Redis Bracket Projection
    participant K as Kafka

    R-->>K: MatchCompleted / room result
    K-->>T: Async consume result
    T->>T: Calculate PlayersAdvanced
    T->>W: Provision next round rooms
    T->>P: Update bracket projection
    T-->>K: Tournament progression event
```

### Image Asset Brief

Prompt brief:

> Clean enterprise diagram of tournament orchestration for a multiplayer card platform. Show async room results feeding a tournament service, then sharded workers provisioning the next round, with a Redis bracket projection on the side. Use restrained colors and clear separation between business flow and read projection.

## 4. Timer Expiry

```mermaid
sequenceDiagram
    participant R as Room Gameplay
    participant D as Postgres Deadline Record
    participant X as Redis Index
    participant W as Room Timer Worker
    participant B as BFF
    participant S as SSE Stream

    R->>D: Persist deadline
    R->>X: Add scheduling index entry
    W->>X: Poll due timers
    W->>R: Trigger expiry check
    R->>R: Revalidate current room state
    R-->>B: Expiry result
    B-->>S: SSE timer update
```

## 5. Spectator Projection Rebuild

```mermaid
sequenceDiagram
    participant R as Room Gameplay
    participant T as Tournament Orchestration
    participant S as Spectator View
    participant K as Kafka
    participant B as BFF

    R-->>K: Sanitized room event
    T-->>K: Public tournament event
    K-->>S: Consume safe facts
    S->>S: Rebuild Redis materialized projection
    S-->>B: Spectator-safe read model
```

## Diagram Asset Notes

- The mermaid diagrams are the canonical textual source for this package.
- Any final polished image assets should mirror these flows without adding new semantics.
- Visual assets should emphasize the BFF boundary, the separation between business streams and projection streams, and the privacy split between player and spectator views.
