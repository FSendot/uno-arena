# 07 Sequence Diagrams

## 1. Gameplay Command With Durable Append

```mermaid
sequenceDiagram
    participant C as CLI Client
    participant B as BFF
    participant I as Identity
    participant IP as Identity Postgres
    participant R as Room Gameplay
    participant P as Room Gameplay Postgres
    participant G as Game Integrity
    participant D as Debezium CDC
    participant K as Kafka
    participant DR as Debezium Redis Sink
    participant V as Spectator View
    participant X as Redis Streams
    participant S as SSE Stream
    participant A as Ops/Security Audit

    C->>B: POST /commands {commandId,type,expectedSequenceNumber,payload}
    B->>I: Authoritative Postgres session validation
    I->>IP: Validate token hash and active session
    IP-->>I: Player/session/roles/eligibility/version
    I-->>B: Signed internal principal + validation marker
    B->>R: Dispatch command envelope + principal
    R->>P: BEGIN READ COMMITTED; SELECT Room FOR UPDATE
    P-->>R: Snapshot, sequence, deadlines, dedupe state
    R->>R: Validate room rules and sequence number
    alt command rejected
        R-->>A: Structured rejection audit record
        R-->>B: Rejected result (no domain event, no Game Integrity append)
        B-->>C: Rejection outcome
    else command accepted
        Note over R,P: Room row lock held with bounded deadline
        R->>G: Append technical log entry
        G->>G: Encrypt private payload + attach commitments/key version
        G-->>R: Append confirmed
        R->>P: Commit snapshot + integration/realtime outboxes with logOffset
        P-->>D: Committed outbox insert appears in WAL
        D-->>K: Route cross-context facts by aggregate key
        P-->>DR: Committed realtime outbox appears in WAL
        DR-->>X: Route ordered player feed
        K-->>V: Deliver spectator-safe fact
        V-->>X: Publish ordered spectator projection feed
        X-->>B: Tail authorized player/spectator feed
        R-->>B: Command accepted result
        B-->>S: SSE room update
        S-->>C: Realtime state
    end
```

## 2. Session Invalidation Closes SSE

```mermaid
sequenceDiagram
    participant IdP as External IdP
    participant I as Identity
    participant D as Debezium CDC
    participant K as Kafka
    participant B as BFF
    participant C as CLI Client
    participant S as SSE Stream

    IdP->>I: New login or revocation
    I->>I: Commit invalidation + outbox
    I-->>D: Committed outbox insert appears in WAL
    D-->>K: Route identity.session.invalidated
    K-->>B: Consume SessionInvalidated(control)
    B-->>S: Close stream
    S-->>C: Disconnect / reauth required
```

Identity is the OIDC anti-corruption layer. It validates provider tokens and maps accepted issuer/subject claims to internal identity before creating or replacing the authoritative session; the BFF and downstream contexts never consume raw provider claims.

## 3. Tournament Advancement

```mermaid
sequenceDiagram
    participant R as Room Gameplay
    participant T as Tournament Orchestration
    participant W as Sharded Workers
    participant P as Redis Bracket Projection
    participant D as Debezium CDC
    participant K as Kafka

    R-->>K: MatchCompleted / room result
    K-->>T: Async consume result
    T->>T: Validate slot and record result
    T->>T: Calculate PlayersAdvanced
    T->>T: Wait until every round slot is terminal
    T->>T: Commit advancement + outbox
    T-->>D: Committed outbox insert appears in WAL
    D-->>K: Route TournamentRoundCompleted
    T->>W: Provision next round rooms
    T->>P: Update bracket projection
    T->>T: Commit progression + outbox
    T-->>D: Committed outbox insert appears in WAL
    D-->>K: Route tournament progression event
```

## 4. Timer Expiry

```mermaid
sequenceDiagram
    participant R as Room Gameplay
    participant P as Postgres Deadline Record
    participant I as Redis Index
    participant W as Room Timer Worker
    participant G as Game Integrity
    participant D as Debezium CDC
    participant K as Kafka
    participant DR as Debezium Redis Sink
    participant X as Redis Streams
    participant B as BFF
    participant S as SSE Stream

    R->>P: Persist absolute UTC expiresAt + opening room sequence
    R->>I: ZADD stable timer key scored by expiresAt
    W->>I: Lua claim due batch into visibility-leased set
    I-->>W: Claimed stable timer identities
    W->>R: Trigger expiry check
    R->>R: Revalidate current state, expiresAt, opening sequence, and timer key
    R->>G: Append expiry / forfeit log entry
    G-->>R: Append confirmed
    R->>R: Commit room state + integration/realtime outboxes
    R-->>D: Committed outbox insert appears in WAL
    D-->>K: Route cross-context timer outcome
    R-->>DR: Committed realtime outbox appears in WAL
    DR-->>X: Route ordered player feed update
    X-->>B: Tail authorized player feed
    B-->>S: SSE timer update (corrects advisory CLI countdown)
    W->>I: Acknowledge applied or definitively stale lease
    Note over W,I: Expired leases are reaped and retried
```

CLI countdown or local display is advisory only. The server exclusively decides timeliness from persisted absolute UTC `expiresAt` and the opening room sequence; SSE and command results correct clients.

## 5. Spectator Projection Rebuild

```mermaid
sequenceDiagram
    participant C as CLI Client
    participant R as Room Gameplay
    participant T as Tournament Orchestration
    participant S as Spectator View
    participant K as Kafka
    participant X as Redis Streams
    participant B as BFF

    C->>B: GET spectator snapshot/events
    alt room waiting/locked/in_progress
        B->>S: Admit spectator (public/private auth)
        S-->>B: Snapshot / admission result
        B->>X: Tail room spectator feed
        B-->>C: Spectator-safe stream
    else room completed/cancelled
        B-->>C: Admission denied
    end
    R-->>K: Sanitized room event
    T-->>K: Public tournament event
    K-->>S: Consume safe facts
    S->>S: Rebuild Redis materialized projection
    S-->>X: Publish ordered spectator projection feed
    X-->>B: Deliver spectator-safe update
    R-->>K: RoomCompleted or RoomCancelled
    K-->>S: Terminal room/match state
    S-->>X: Publish terminal spectator feed entry
    X-->>B: Deliver terminal entry
    B-->>C: Stream closed
```

Spectator admission is allowed while the room is `waiting`, `locked`, or `in_progress` subject to public/private authorization. After `RoomCompleted` or `RoomCancelled`, new admission is denied and existing spectator streams close. That terminal boundary is the complete match/room, not an individual game in a best-of-three.

## 6. Consumer Retry, DLQ, and Aggregate Quarantine

```mermaid
sequenceDiagram
    participant K as Kafka Source Topic
    participant C as Context Consumer
    participant Q as Consumer Quarantine Store
    participant D as Consumer-Owned DLQ
    participant O as Operator or Replay Worker

    K-->>C: Event with aggregate key and source offset
    C->>C: Process with bounded retry
    alt processing succeeds
        C->>C: Atomically commit state + contract dedupe key + aggregate checkpoint
        C-->>K: Commit source offset
    else sequence gap or conflicting duplicate
        C->>D: Publish original envelope + conflict metadata
        D-->>C: Broker acknowledgment
        C->>Q: Persist aggregate quarantine + conflict evidence
        C-->>K: Commit source offset
        Note over C,Q: Later records for this aggregate are held or routed; unrelated keys continue
    else terminal failure
        C->>D: Publish original envelope + failure metadata
        D-->>C: Broker acknowledgment
        C->>Q: Quarantine affected aggregate key
        C-->>K: Commit source offset
        Note over C,Q: Unrelated aggregate keys continue
        O->>D: Inspect or replay failed record
        O->>C: Reprocess from contiguous sequence
        C->>Q: Release aggregate quarantine
    end
```

## 7. Ordered Kafka Topic Capacity Cutover

```mermaid
sequenceDiagram
    participant A as Platform Operator
    participant P as Context Producer Pipeline
    participant O as Ordered Topic Old Epoch
    participant N as Versioned Replacement Topic
    participant C as Context Consumer
    participant S as Consumer Owned Store

    A->>N: Provision topic, ACLs, DLQ, routes, and consumer group
    C->>N: Subscribe and buffer without applying
    A->>P: Activate topology epoch cutover
    P->>P: Seal old destination and persist source watermark
    P-->>O: Finish publication through sealed watermark
    P-->>N: Publish subsequent records under new epoch
    C->>O: Apply and checkpoint old records
    C->>C: Verify producer watermark passed and old group lag is zero
    C->>N: Release buffered records by aggregate sequence/version
    C->>S: Commit state, dedupe key, and new-epoch checkpoint
    Note over C,S: Gap or conflicting overlap quarantines only that aggregate
```

Payload schema versions remain independent from physical topic topology epochs. The old topic remains read-only through the rollback and verification window; it is never expanded in place.

## 8. Ad-hoc Host Leaves Before Lock/Start

```mermaid
sequenceDiagram
    participant C as CLI Client
    participant B as BFF
    participant R as Room Gameplay
    participant G as Game Integrity
    participant X as Redis Streams
    participant S as SSE Stream

    C->>B: LeaveRoom (current host, waiting)
    B->>R: Dispatch LeaveRoom
    R->>R: Remove host seat
    alt remaining players exist
        R->>R: Assign host to lowest occupied seat
        R->>G: Append PlayerLeftRoom + HostReassigned
        G-->>R: Append confirmed
        R-->>X: Publish ordered player feed updates
        R-->>B: Accepted
        B-->>S: SSE HostReassigned
    else nobody remains
        R->>G: Append PlayerLeftRoom + RoomCancelled
        G-->>R: Append confirmed
        R-->>X: Publish ordered terminal feed update
        R-->>B: Accepted
        B-->>S: Close spectator streams
    end
```

After lock/start, a host leave still removes the player under normal leave/disconnect policy, but host reassignment has no gameplay authority.
