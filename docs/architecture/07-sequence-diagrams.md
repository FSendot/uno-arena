# 07 Sequence Diagrams

## 1. Gameplay Command With Durable Append

```mermaid
sequenceDiagram
    participant C as CLI Client
    participant B as BFF
    participant I as Identity
    participant R as Room Gameplay
    participant G as Game Integrity
    participant K as Kafka / Outbox
    participant S as SSE Stream
    participant A as Ops/Security Audit

    C->>B: POST /commands {commandId,type,expectedSequenceNumber,payload}
    B->>I: Authenticate and authorize
    I-->>B: OK
    B->>R: Dispatch command envelope
    R->>R: Validate room rules and sequence number
    alt command rejected
        R-->>A: Structured rejection audit record
        R-->>B: Rejected result (no domain event, no Game Integrity append)
        B-->>C: Rejection outcome
    else command accepted
        R->>G: Append technical log entry
        G-->>R: Append confirmed
        R->>R: Commit snapshot + outbox with logOffset
        R-->>K: Publish sanitized feed/projection event
        K-->>B: Deliver player/spectator-safe update
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
    participant B as BFF
    participant C as CLI Client
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
    T->>T: Validate slot and record result
    T->>T: Calculate PlayersAdvanced
    T->>T: Wait until every round slot is terminal
    T-->>K: TournamentRoundCompleted
    T->>W: Provision next round rooms
    T->>P: Update bracket projection
    T-->>K: Tournament progression event
```

## 4. Timer Expiry

```mermaid
sequenceDiagram
    participant R as Room Gameplay
    participant D as Postgres Deadline Record
    participant X as Redis Index
    participant W as Room Timer Worker
    participant G as Game Integrity
    participant K as Kafka / Outbox
    participant B as BFF
    participant S as SSE Stream

    R->>D: Persist absolute UTC expiresAt + opening room sequence
    R->>X: Add scheduling index entry
    W->>X: Poll due timers
    W->>R: Trigger expiry check
    R->>R: Revalidate current state, expiresAt, opening sequence, and timer key
    R->>G: Append expiry / forfeit log entry
    G-->>R: Append confirmed
    R->>R: Commit room state + outbox
    R-->>K: Publish timer outcome
    K-->>B: Deliver player/safe update
    B-->>S: SSE timer update (corrects advisory CLI countdown)
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
    participant B as BFF

    C->>B: GET spectator snapshot/events
    alt room waiting/locked/in_progress
        B->>S: Admit spectator (public/private auth)
        S-->>B: Snapshot / SSE attach
        B-->>C: Spectator-safe stream
    else room completed/cancelled
        B-->>C: Admission denied
    end
    R-->>K: Sanitized room event
    T-->>K: Public tournament event
    K-->>S: Consume safe facts
    S->>S: Rebuild Redis materialized projection
    S-->>B: Spectator-safe read model
    R-->>K: RoomCompleted or RoomCancelled
    K-->>S: Terminal room/match state
    S-->>B: Close existing spectator streams
    B-->>C: Stream closed
```

Spectator admission is allowed while the room is `waiting`, `locked`, or `in_progress` subject to public/private authorization. After `RoomCompleted` or `RoomCancelled`, new admission is denied and existing spectator streams close. That terminal boundary is the complete match/room, not an individual game in a best-of-three.

## 6. Ad-hoc Host Leaves Before Lock/Start

```mermaid
sequenceDiagram
    participant C as CLI Client
    participant B as BFF
    participant R as Room Gameplay
    participant G as Game Integrity
    participant K as Kafka / Outbox
    participant S as SSE Stream

    C->>B: LeaveRoom (current host, waiting)
    B->>R: Dispatch LeaveRoom
    R->>R: Remove host seat
    alt remaining players exist
        R->>R: Assign host to lowest occupied seat
        R->>G: Append PlayerLeftRoom + HostReassigned
        G-->>R: Append confirmed
        R-->>K: Publish safe updates
        R-->>B: Accepted
        B-->>S: SSE HostReassigned
    else nobody remains
        R->>G: Append PlayerLeftRoom + RoomCancelled
        G-->>R: Append confirmed
        R-->>K: Publish RoomCancelled
        R-->>B: Accepted
        B-->>S: Close spectator streams
    end
```

After lock/start, a host leave still removes the player under normal leave/disconnect policy, but host reassignment has no gameplay authority.
