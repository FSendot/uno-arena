# 01 Context and Container View

## Context View

```mermaid
flowchart LR
    Players["Players"]
    Spectators["Spectators"]
    Admins["Tournament Operators"]
    Client["UnoArena Client"]
    BFF["Realtime Gateway / BFF"]
    IdP["External Identity Provider"]

    subgraph Platform["UnoArena Platform"]
        Identity["Identity"]
        Room["Room Gameplay"]
        Integrity["Game Integrity"]
        Tourney["Tournament Orchestration"]
        Rank["Ranking"]
        Spectator["Spectator View"]
        Analytics["Analytics and Public Read Models"]
    end

    Players --> Client
    Spectators --> Client
    Admins --> Client
    Client --> BFF
    BFF --> Identity
    BFF --> Room
    BFF --> Tourney
    BFF --> Spectator
    BFF --> Analytics
    Identity --> IdP
    Room --> Integrity
    Room --> Tourney
    Room --> Rank
    Room --> Spectator
    Room --> Analytics
    Tourney --> Rank
    Tourney --> Analytics
    Rank --> Analytics
```

The BFF is the only external boundary. Clients do not talk to microservices directly. They submit commands to the BFF and subscribe to SSE streams from the same logical realtime gateway.

The platform is split by bounded context, not by transport technology. SSE, Kafka, Postgres, Redis, and EventStoreDB are containers or infrastructure choices, not domain boundaries.

## Container View

```mermaid
flowchart TB
    subgraph Public["Untrusted Client Zone"]
        Client["Client App"]
    end

    subgraph Edge["Trust Boundary / External Edge (DMZ)"]
        BFF["Realtime Gateway / BFF"]
    end

    subgraph Core["Private Network / Core Services"]
        IdentitySvc["Identity Service"]
        RoomSvc["Room Gameplay Service"]
        IntegritySvc["Game Integrity Service"]
        TourneySvc["Tournament Orchestration Service"]
        RankSvc["Ranking Service"]
        SpectatorSvc["Spectator Projection Service"]
        AnalyticsSvc["Analytics / Public Read Models Service"]
    end

    subgraph Workers["Workers"]
        TimerWorker["Room Timer Worker"]
        TournamentWorkers["Sharded Tournament Provisioning Workers"]
        ProjectionWorkers["Projection Rebuilders"]
    end

    subgraph Data["Data Stores"]
        IdentityDB["Postgres"]
        RoomDB["Postgres"]
        EventStore["EventStoreDB"]
        TourneyDB["Postgres"]
        RankDB["Postgres"]
        Redis["Redis"]
        ClickHouse["ClickHouse"]
        Kafka["Kafka"]
    end

    Client --> BFF
    BFF --> IdentitySvc
    BFF --> RoomSvc
    BFF --> TourneySvc
    BFF --> SpectatorSvc
    BFF --> AnalyticsSvc

    IdentitySvc --> IdentityDB
    RoomSvc --> RoomDB
    RoomSvc --> IntegritySvc
    RoomSvc --> Kafka
    RoomSvc --> Redis
    IntegritySvc --> EventStore
    IntegritySvc --> Kafka
    TourneySvc --> TourneyDB
    TourneySvc --> Kafka
    TourneySvc --> Redis
    RankSvc --> RankDB
    RankSvc --> Redis
    SpectatorSvc --> Redis
    SpectatorSvc --> Kafka
    AnalyticsSvc --> ClickHouse
    AnalyticsSvc --> Kafka

    BFF --> Kafka
    TimerWorker --> Redis
    TimerWorker --> RoomDB
    TournamentWorkers --> TourneyDB
    TournamentWorkers --> RoomSvc
    ProjectionWorkers --> Kafka
```

## Container Notes

- `Realtime Gateway / BFF`
  - the only public HTTP and SSE entrypoint
  - terminates the public trust boundary before any core service or data store is reachable
  - maps compact command envelopes to existing command names
  - emits SSE control events for stream close, session invalidation, and reconnect

- `Identity Service`
  - external IdP integration plus internal session and ACL state
  - authoritative on session validity

- `Room Gameplay Service`
  - owns Uno rules, turns, room lifecycle, and operational snapshots
  - asks Game Integrity to append before broadcast

- `Game Integrity Service`
  - authoritative append-only technical log
  - internal audit and replay only

- `Tournament Orchestration Service`
  - owns tournament lifecycle, room provisioning, bracket progression, and advancement

- `Ranking Service`
  - updates persistent ratings asynchronously from authoritative results

- `Spectator Projection Service`
  - serves privacy-filtered room spectator projections

- `Analytics / Public Read Models Service`
  - consumes sanitized/public events into ClickHouse and other derived read models

## Local and Test Topology

- Local development can run the BFF plus a reduced set of services and backing stores.
- Integration tests should cover command validation, SSE fan-out, replay, and cross-context event consumption.
- EventStoreDB, Postgres, Redis, Kafka, and ClickHouse should all be replaceable with test containers or lightweight local equivalents in non-production runs.
- The logical topology stays the same across environments even when containers are collapsed for local speed.
