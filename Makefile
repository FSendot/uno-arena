# UnoArena local foundation targets.
# Targets must not fetch modules or pull images automatically.
# Shared module is stdlib-only; keep GOPROXY=off for shared tests.

GO ?= go
GOFMT ?= gofmt
RUBY ?= ruby
GOCACHE ?= /private/tmp/uno-arena-go-cache
export GOCACHE

GO_TEST_ENV = GOWORK=off GOPROXY=off GOSUMDB=off

.PHONY: help fmt-shared test-shared vet-shared \
	test-telemetry \
	test-analytics test-game-integrity test-game-integrity-integration deps-game-integrity \
	test-gateway test-identity \
	test-ranking test-room-gameplay test-spectator-view test-tournament-orchestration \
	test-modules validate-yaml validate-compose test-compose-topology \
	test-capability-stack test-client-checkpoint test-client-dockerfile \
	check build-shared \
	kind-render kind-validate kind-create-cluster kind-build-load-bootstrap \
	kind-verify-portable-images \
	kind-install-istio kind-deploy-observability kind-wait-observability \
	kind-test-observability-structure kind-test-observability-live \
	kind-test-observability-business-live kind-test-observability-trace-live \
	kind-test-observability-security-live kind-test-observability-outage-live \
	kind-test-observability-persistence-live kind-test-observability-acceptance-live \
	kind-port-forward-grafana \
	kind-build-load-services kind-deploy-game-integrity kind-deploy-services \
	kind-run-live-probes kind-clean-deploy kind-test-clean-deployment-structure \
	kind-load-debezium-server kind-status-debezium-server kind-test-debezium-server-structure \
	kind-load-debezium-connect kind-test-debezium-connectors kind-test-debezium-connect-structure \
	kind-test-debezium-postgres-to-kafka-live kind-test-debezium-postgres-to-redis-live \
	kind-apply kind-wait kind-reset kind-test-game-integrity-adapter \
	kind-deploy-identity kind-test-identity-adapter kind-test-identity-integration \
	test-identity-helm test-identity-integration \
	kind-deploy-room-gameplay kind-test-room-adapter-structure kind-test-room-integration \
	test-room-helm test-room-integration \
	kind-deploy-tournament-orchestration kind-test-tournament-adapter \
	kind-test-tournament-adapter-structure kind-test-tournament-integration \
	kind-test-tournament-redis-integration \
	test-tournament-helm test-tournament-integration \
	kind-deploy-ranking kind-test-ranking-adapter kind-test-ranking-adapter-structure \
	kind-test-ranking-integration kind-test-ranking-redis-integration \
	test-ranking-helm test-ranking-integration test-ranking-redis-integration \
	kind-deploy-analytics kind-test-analytics-adapter kind-test-analytics-adapter-structure \
	kind-test-analytics-integration test-analytics-helm test-analytics-integration \
	kind-test-analytics-kafka-consumer-structure kind-test-analytics-kafka-to-clickhouse-live \
	kind-test-analytics-projection-rebuilder-structure kind-test-analytics-projection-rebuilder-live \
	kind-deploy-spectator-view kind-test-spectator-adapter kind-test-spectator-adapter-structure \
	kind-test-spectator-integration kind-test-spectator-projection-rebuilder-structure \
	kind-test-spectator-projection-rebuilder-live \
	test-spectator-helm test-spectator-integration \
	kind-deploy-gateway kind-test-gateway-adapter kind-test-gateway-adapter-structure \
	kind-test-gateway-si-redis-admission-live kind-test-gateway-session-invalidation-live \
	kind-test-gateway-integration test-gateway-helm test-gateway-integration \
	kind-test-redis-aof kind-test-redis-aof-structure

help:
	@echo "Targets (no network fetch unless noted):"
	@echo "  fmt-shared             gofmt shared module"
	@echo "  test-shared            go test ./shared/... with GOWORK=off GOPROXY=off"
	@echo "  test-modules           test shared + platform telemetry + all eight service modules (GOWORK=off)"
	@echo "  test-telemetry         offline platform telemetry tests (requires cached pinned modules)"
	@echo "  vet-shared             go vet ./shared/... with GOPROXY=off"
	@echo "  deps-game-integrity    NETWORKED: go mod download for GI (Kurrent client cache)"
	@echo "  test-game-integrity    offline GI unit tests (requires module cache; run deps-game-integrity once)"
	@echo "  test-game-integrity-integration  live Kurrent integration (needs KURRENTDB_INTEGRATION_URL + keyring)"
	@echo "  kind-test-game-integrity-adapter EXPLICIT/NETWORKED: kind context + port-forward + GI integration"
	@echo "  kind-deploy-identity         EXPLICIT: helm upgrade --install Identity (kind values)"
	@echo "  kind-test-identity-adapter   EXPLICIT/NETWORKED: Identity durable login/validate/outbox"
	@echo "  kind-test-identity-integration EXPLICIT/NETWORKED: ephemeral test DB + store integration"
	@echo "  test-identity-helm           offline Identity helm lint/template checks"
	@echo "  test-identity-integration    live Postgres integration (safe unoarena_identity_test_* DSN only)"
	@echo "  kind-deploy-room-gameplay    EXPLICIT: helm upgrade --install Room Gameplay (kind values)"
	@echo "  kind-test-room-adapter-structure  offline Room deploy/adapter/PF structure checks"
	@echo "  kind-test-room-integration   EXPLICIT/NETWORKED: ephemeral test DB + Room store integration"
	@echo "  test-room-helm               offline Room helm lint/template checks"
	@echo "  test-room-integration        live Postgres integration (safe unoarena_room_gameplay_test_* DSN only)"
	@echo "  kind-deploy-tournament-orchestration EXPLICIT: helm upgrade --install Tournament (kind values)"
	@echo "  kind-test-tournament-adapter EXPLICIT/NETWORKED: Tournament durable create/idempotency (internal create has no outbox)"
	@echo "  kind-test-tournament-adapter-structure offline Tournament deploy/adapter/PF structure checks"
	@echo "  kind-test-tournament-integration EXPLICIT/NETWORKED: ephemeral test DB + Tournament store + service integration"
	@echo "  kind-test-tournament-redis-integration EXPLICIT/NETWORKED: Tournament Redis bracket projection (DB 14)"
	@echo "  test-tournament-helm         offline Tournament helm lint/template checks"
	@echo "  test-tournament-integration  live Postgres integration (safe unoarena_tournament_test_* DSN only)"
	@echo "  kind-deploy-ranking          EXPLICIT: helm upgrade --install Ranking (kind values)"
	@echo "  kind-test-ranking-adapter    EXPLICIT/NETWORKED: Ranking durable games-results/outbox (no CDC claim)"
	@echo "  kind-test-ranking-adapter-structure offline Ranking deploy/adapter/PF structure checks"
	@echo "  kind-test-ranking-integration EXPLICIT/NETWORKED: ephemeral test DB + Ranking store integration"
	@echo "  test-ranking-helm            offline Ranking helm lint/template checks"
	@echo "  test-ranking-integration     live Postgres integration (safe unoarena_ranking_test_* DSN only)"
	@echo "  kind-deploy-analytics        EXPLICIT: helm upgrade --install Analytics (kind values)"
	@echo "  kind-test-analytics-adapter  EXPLICIT/NETWORKED: Analytics durable ClickHouse ingest (no Kafka claim)"
	@echo "  kind-test-analytics-adapter-structure offline Analytics deploy/adapter/PF structure checks"
	@echo "  kind-test-analytics-kafka-consumer-structure offline Analytics Kafka consumer structure"
	@echo "  kind-test-analytics-kafka-to-clickhouse-live EXPLICIT: Kafka→ClickHouse Analytics ingest proof"
	@echo "  kind-test-analytics-projection-rebuilder-live EXPLICIT: Kafka→backfill HTTP→ClickHouse recovery proof"
	@echo "  kind-test-analytics-integration EXPLICIT/NETWORKED: ephemeral test DB + Analytics store integration"
	@echo "  test-analytics-helm          offline Analytics helm lint/template checks"
	@echo "  test-analytics-integration   live ClickHouse integration (safe unoarena_analytics_test_* DB only)"
	@echo "  kind-deploy-spectator-view   EXPLICIT: helm upgrade --install Spectator View (kind values)"
	@echo "  kind-test-spectator-adapter  EXPLICIT/NETWORKED: Spectator durable Redis ingest (Kafka consumer wired; live probe may still use HTTP bridge)"
	@echo "  kind-test-spectator-adapter-structure offline Spectator deploy/adapter/PF structure checks"
	@echo "  kind-test-spectator-projection-rebuilder-structure offline ADR-0039 rebuilder/worker/topic structure checks"
	@echo "  kind-test-spectator-projection-rebuilder-live EXPLICIT: Kafka→Room snapshot→Redis recovery proof"
	@echo "  kind-test-spectator-integration EXPLICIT/NETWORKED: Redis DB 14 + tagged store suite"
	@echo "  kind-deploy-gateway          EXPLICIT: helm upgrade --install Gateway (kind values)"
	@echo "  kind-test-gateway-adapter    EXPLICIT/NETWORKED: Gateway durable Redis ready (SessionInvalidated Kafka wired)"
	@echo "  kind-test-gateway-adapter-structure offline Gateway deploy/adapter/PF structure checks"
	@echo "  kind-test-gateway-si-redis-admission-live EXPLICIT/NETWORKED: Redis DB6 SI cross-Hub admission only (not Kafka→SSE E2E)"
	@echo "  kind-test-gateway-session-invalidation-live alias → kind-test-gateway-si-redis-admission-live"
	@echo "  kind-test-gateway-integration EXPLICIT/NETWORKED: Redis DBs 11/12/13 + tagged gateway suite"
	@echo "  kind-test-redis-aof-structure offline Redis AOF manifest + live-script structure checks"
	@echo "  kind-test-redis-aof          EXPLICIT/NETWORKED: DB15 prefix survives Redis PID 1 restart"
	@echo "  test-spectator-helm          offline Spectator helm lint/template checks"
	@echo "  test-spectator-integration   live Redis DB 14 integration (SPECTATOR_REDIS_URL required)"
	@echo "  test-gateway-helm            offline Gateway helm lint/template checks"
	@echo "  test-gateway-integration     live Redis DBs 11/12/13 (GATEWAY_*_REDIS_URL required)"
	@echo "  validate-yaml          parse OpenAPI/AsyncAPI/compose YAML via Ruby stdlib"
	@echo "  validate-compose       docker compose config (requires local docker; no pull)"
	@echo "  test-compose-topology  edge+private network assertions on resolved config (no stack)"
	@echo "  test-capability-stack  Docker capability overlay + CLI/BFF integration harness"
	@echo "  test-client-checkpoint offline Client Checkpoint CLI tests (fake BFF, no Docker)"
	@echo "  test-client-dockerfile Client Checkpoint Dockerfile structure (no build/pull)"
	@echo "  check                  fmt + vet + test-modules + validate-yaml + git diff --check"
	@echo "  build-shared           go build ./shared/... with GOPROXY=off"
	@echo "  kind-render            offline AsyncAPI → kind Kafka topic artifacts"
	@echo "  kind-validate          offline kind foundation checks incl. CDC/retention (no pull / no cluster)"
	@echo "  kind-create-cluster    EXPLICIT: create disposable kind cluster uno-arena"
	@echo "  kind-verify-portable-images NETWORKED/read-only: verify AMD64+ARM64 children for every generic OCI index"
	@echo "  kind-build-load-bootstrap  EXPLICIT: docker build + kind load bootstrap image"
	@echo "  kind-build-load-services   EXPLICIT/NETWORKED: build + load all 8 services for the kind node architecture"
	@echo "  kind-install-istio         EXPLICIT: install vendored Istio Ambient 1.30.2"
	@echo "  kind-deploy-observability  EXPLICIT: install Alloy/Loki/Tempo/Prometheus/Grafana after MinIO"
	@echo "  kind-wait-observability    read-only readiness wait for observability workloads"
	@echo "  kind-test-observability-structure offline chart/storage/Istio deployment contracts"
	@echo "  kind-test-observability-live EXPLICIT: data sources, dashboards, targets, and scoped RBAC"
	@echo "  kind-test-observability-business-live EXPLICIT: real CLI flows + exact business counter contracts/deltas"
	@echo "  kind-test-observability-trace-live EXPLICIT: GameCompleted Tempo trace + Loki trace/correlation linkage"
	@echo "  kind-test-observability-security-live EXPLICIT: Prometheus-only 9090 and application-only OTLP ingress"
	@echo "  kind-test-observability-outage-live EXPLICIT: gameplay continuity and trace recovery across Alloy outage"
	@echo "  kind-test-observability-persistence-live EXPLICIT: Loki/Tempo evidence after backend + MinIO pod replacement"
	@echo "  kind-test-observability-acceptance-live EXPLICIT: ordered application observability acceptance lane"
	@echo "  kind-port-forward-grafana  EXPLICIT: loopback-only Grafana port-forward"
	@echo "  kind-deploy-game-integrity EXPLICIT: helm upgrade --install Game Integrity (kind values)"
	@echo "  kind-deploy-services       EXPLICIT: deploy all 8 services in dependency order"
	@echo "  kind-run-live-probes       EXPLICIT/NETWORKED: run the complete kind acceptance probe lane"
	@echo "  kind-clean-deploy          DESTRUCTIVE/NETWORKED: reset uno-arena, rebuild, deploy, and probe"
	@echo "  kind-test-clean-deployment-structure offline clean-deployment orchestration checks"
	@echo "  kind-load-debezium-connect EXPLICIT/NETWORKED: node-native crictl pull Debezium Connect 3.6 (Kafka outbox)"
	@echo "  kind-test-debezium-connect-structure  offline Connect config/wiring structure checks"
	@echo "  kind-test-debezium-connectors  EXPLICIT/NETWORKED: Connect REST status for 4 connectors (no delivery claim)"
	@echo "  kind-test-debezium-postgres-to-kafka-live  EXPLICIT: insert Identity outbox + observe Kafka (no consumer claim)"
	@echo "  kind-load-debezium-server  EXPLICIT/NETWORKED: node-native crictl pull Debezium Server 3.6 (Room realtime)"
	@echo "  kind-status-debezium-server  read-only Deployment/health status (no CDC delivery claim)"
	@echo "  kind-test-debezium-server-structure  offline Server config/wiring structure checks"
	@echo "  kind-test-debezium-postgres-to-redis-live  EXPLICIT: insert Room realtime outbox + observe Redis DB2 (no SSE E2E claim)"
	@echo "  kind-apply             EXPLICIT: apply foundation manifests (context kind-uno-arena)"
	@echo "  kind-wait              wait for foundation Deployments + bootstrap Jobs + Debezium Connect/Server"
	@echo "  kind-reset             EXPLICIT: delete only kind cluster uno-arena"

fmt-shared:
	$(GOFMT) -w shared

test-shared:
	cd shared && $(GO_TEST_ENV) $(GO) test ./...

test-analytics:
	cd services/analytics/src && $(GO_TEST_ENV) $(GO) test -race -count=1 ./...
	cd services/analytics/src && $(GO_TEST_ENV) $(GO) vet ./...

test-game-integrity:
	cd services/game-integrity/src && $(GO_TEST_ENV) $(GO) test ./...

# NETWORKED: approved dependency bootstrap for the Kurrent client. Normal GI targets stay offline.
deps-game-integrity:
	cd services/game-integrity/src && GOWORK=off $(GO) mod download

# Requires a live KurrentDB and versioned keyring env. Does not run under test-modules.
# Ordinary CI unit job (test-game-integrity / go test ./...) must NOT require Kurrent;
# a dedicated CI integration lane is deferred until the deployment pipeline slice.
test-game-integrity-integration:
	@test -n "$$KURRENTDB_INTEGRATION_URL" || (echo "KURRENTDB_INTEGRATION_URL required" >&2; exit 1)
	cd services/game-integrity/src && GOWORK=off GOPROXY=off GOSUMDB=off \
		$(GO) test -count=1 -tags=integration -timeout 120s ./...

# EXPLICIT + NETWORKED: verifies kind-uno-arena, port-forwards Kurrent, runs integration.
# Never applies/resets/deploys.
kind-test-game-integrity-adapter:
	./infrastructure/kind/scripts/test-game-integrity-adapter.sh

kind-deploy-identity:
	./infrastructure/kind/scripts/deploy-identity.sh

kind-test-identity-adapter:
	./infrastructure/kind/scripts/test-identity-adapter-structure.sh
	./infrastructure/kind/scripts/test-identity-adapter.sh

test-identity-helm:
	./services/identity/helm/identity/helm-test.sh

# Requires IDENTITY_POSTGRES_URL whose database name is explicitly prefixed
# unoarena_identity_test_ (never identity/postgres/template0/template1). Prefer
# kind-test-identity-integration, which creates an ephemeral DB for the run.
test-identity-integration:
	@test -n "$$IDENTITY_POSTGRES_URL" || (echo "IDENTITY_POSTGRES_URL required" >&2; exit 1)
	cd services/identity/src && GOWORK=off GOPROXY=off GOSUMDB=off \
		$(GO) test -count=1 -tags=integration -timeout 120s ./store/...

# EXPLICIT + NETWORKED: verifies kind-uno-arena, creates unoarena_identity_test_*,
# port-forwards postgres-identity, runs store integration as identity_runtime, drops DB.
# Never applies/resets/deploys. Never accepts caller-supplied database names.
kind-test-identity-integration:
	./infrastructure/kind/scripts/test-identity-integration-structure.sh
	./infrastructure/kind/scripts/test-identity-integration.sh

kind-deploy-room-gameplay:
	./infrastructure/kind/scripts/deploy-room-gameplay.sh

kind-test-room-adapter-structure:
	./infrastructure/kind/scripts/test-room-adapter-structure.sh

test-room-helm:
	./services/room-gameplay/helm/room-gameplay/helm-test.sh

# Requires ROOM_POSTGRES_URL whose database name is explicitly prefixed
# unoarena_room_gameplay_test_ (never room_gameplay/postgres/template0/template1). Prefer
# kind-test-room-integration, which creates an ephemeral DB for the run.
test-room-integration:
	@test -n "$$ROOM_POSTGRES_URL" || (echo "ROOM_POSTGRES_URL required" >&2; exit 1)
	cd services/room-gameplay/src && GOWORK=off GOPROXY=off GOSUMDB=off \
		$(GO) test -count=1 -tags=integration -timeout 180s ./store/...

# EXPLICIT + NETWORKED: verifies kind-uno-arena, creates unoarena_room_gameplay_test_*,
# port-forwards postgres-room-gameplay, runs store integration as room_runtime, drops DB.
# Never applies/resets/deploys. Never accepts caller-supplied database names.
# Does not reset the authoritative room_gameplay database.
kind-test-room-integration:
	./infrastructure/kind/scripts/test-room-integration-structure.sh
	./infrastructure/kind/scripts/test-room-integration.sh

kind-deploy-tournament-orchestration:
	./infrastructure/kind/scripts/deploy-tournament-orchestration.sh

kind-test-tournament-adapter-structure:
	./infrastructure/kind/scripts/test-tournament-adapter-structure.sh

kind-test-tournament-adapter:
	./infrastructure/kind/scripts/test-tournament-adapter-structure.sh
	./infrastructure/kind/scripts/test-tournament-adapter.sh

test-tournament-helm:
	./services/tournament-orchestration/helm/tournament-orchestration/helm-test.sh

# Requires TOURNAMENT_POSTGRES_URL whose database name is explicitly prefixed
# unoarena_tournament_test_ (never tournament/postgres/template0/template1). Prefer
# kind-test-tournament-integration, which creates an ephemeral DB for the run.
# Runs store integration first, then main-package service durable integration,
# sequentially against the same safe DSN (main tests reset schema per test).
test-tournament-integration:
	@test -n "$$TOURNAMENT_POSTGRES_URL" || (echo "TOURNAMENT_POSTGRES_URL required" >&2; exit 1)
	cd services/tournament-orchestration/src && GOWORK=off GOPROXY=off GOSUMDB=off \
		$(GO) test -count=1 -tags=integration -parallel 2 -timeout 300s ./store/...
	cd services/tournament-orchestration/src && GOWORK=off GOPROXY=off GOSUMDB=off \
		$(GO) test -count=1 -tags=integration -parallel 2 -timeout 300s .
# EXPLICIT + NETWORKED: verifies kind-uno-arena, creates unoarena_tournament_test_*,
# port-forwards postgres-tournament, runs store + main-package service integration
# as tournament_runtime, drops DB.
# Never applies/resets/deploys. Never accepts caller-supplied database names.
kind-test-tournament-integration:
	./infrastructure/kind/scripts/test-tournament-integration-structure.sh
	./infrastructure/kind/scripts/test-tournament-integration.sh

kind-test-tournament-redis-integration:
	./infrastructure/kind/scripts/test-tournament-redis-integration.sh

kind-deploy-ranking:
	./infrastructure/kind/scripts/deploy-ranking.sh

kind-test-ranking-adapter-structure:
	./infrastructure/kind/scripts/test-ranking-adapter-structure.sh

kind-test-ranking-adapter:
	./infrastructure/kind/scripts/test-ranking-adapter-structure.sh
	./infrastructure/kind/scripts/test-ranking-adapter.sh

test-ranking-helm:
	./services/ranking/helm/ranking/helm-test.sh

# Requires RANKING_POSTGRES_URL whose database name is explicitly prefixed
# unoarena_ranking_test_ (never ranking/postgres/template0/template1). Prefer
# kind-test-ranking-integration, which creates an ephemeral DB for the run.
test-ranking-integration:
	@test -n "$$RANKING_POSTGRES_URL" || (echo "RANKING_POSTGRES_URL required" >&2; exit 1)
	cd services/ranking/src && GOWORK=off GOPROXY=off GOSUMDB=off \
		$(GO) test -count=1 -tags=integration -timeout 180s ./store/...

test-ranking-redis-integration:
	@test -n "$$RANKING_REDIS_URL" || (echo "RANKING_REDIS_URL required" >&2; exit 1)
	cd services/ranking/src && GOWORK=off GOPROXY=off GOSUMDB=off \
		$(GO) test -count=1 -tags=redis_integration -timeout 120s ./store/...

# EXPLICIT + NETWORKED: verifies kind-uno-arena, creates unoarena_ranking_test_*,
# port-forwards postgres-ranking, runs store integration as ranking_runtime, drops DB.
# Never applies/resets/deploys. Never accepts caller-supplied database names.
kind-test-ranking-integration:
	./infrastructure/kind/scripts/test-ranking-integration-structure.sh
	./infrastructure/kind/scripts/test-ranking-integration.sh

kind-test-ranking-redis-integration:
	./infrastructure/kind/scripts/test-ranking-redis-integration.sh

kind-deploy-analytics:
	./infrastructure/kind/scripts/deploy-analytics.sh

kind-test-analytics-adapter-structure:
	./infrastructure/kind/scripts/test-analytics-adapter-structure.sh

kind-test-analytics-kafka-consumer-structure:
	./infrastructure/kind/scripts/test-analytics-kafka-consumer-structure.sh

kind-test-analytics-projection-rebuilder-structure:
	./infrastructure/kind/scripts/test-analytics-projection-rebuilder-structure.sh

kind-test-analytics-projection-rebuilder-live:
	./infrastructure/kind/scripts/test-analytics-projection-rebuilder-live.sh

kind-test-analytics-kafka-to-clickhouse-live:
	./infrastructure/kind/scripts/test-analytics-kafka-consumer-structure.sh
	./infrastructure/kind/scripts/test-analytics-kafka-to-clickhouse-live.sh

kind-test-analytics-adapter:
	./infrastructure/kind/scripts/test-analytics-adapter-structure.sh
	./infrastructure/kind/scripts/test-analytics-adapter.sh

test-analytics-helm:
	./services/analytics/helm/analytics/helm-test.sh

# Requires ANALYTICS_CLICKHOUSE_* whose database name is explicitly prefixed
# unoarena_analytics_test_ (never analytics/default/system). Prefer
# kind-test-analytics-integration, which creates an ephemeral DB for the run.
test-analytics-integration:
	@test -n "$$ANALYTICS_CLICKHOUSE_URL" || (echo "ANALYTICS_CLICKHOUSE_URL required" >&2; exit 1)
	@test -n "$$ANALYTICS_CLICKHOUSE_DATABASE" || (echo "ANALYTICS_CLICKHOUSE_DATABASE required" >&2; exit 1)
	cd services/analytics/src && GOWORK=off GOPROXY=off GOSUMDB=off \
		$(GO) test -count=1 -race -tags=integration -timeout 180s ./store/...

# EXPLICIT + NETWORKED: verifies kind-uno-arena, creates unoarena_analytics_test_*,
# port-forwards clickhouse, runs store integration as analytics_runtime, drops DB.
# Never applies/resets/deploys. Never accepts caller-supplied database names.
kind-test-analytics-integration:
	./infrastructure/kind/scripts/test-analytics-integration-structure.sh
	./infrastructure/kind/scripts/test-analytics-integration.sh

test-gateway:
	cd services/gateway/src && $(GO_TEST_ENV) $(GO) test ./...
	cd services/gateway/src && $(GO_TEST_ENV) $(GO) vet ./...

# Live Redis DBs 11/12/13 integration (tagged). Parent kind harness sets GATEWAY_*_REDIS_URL.
test-gateway-integration:
	@test -n "$${GATEWAY_REDIS_URL}" || (echo "GATEWAY_REDIS_URL required (must use Redis DB 11)" >&2; exit 1)
	@test -n "$${GATEWAY_PLAYER_FEED_REDIS_URL}" || (echo "GATEWAY_PLAYER_FEED_REDIS_URL required (must use Redis DB 12)" >&2; exit 1)
	@test -n "$${GATEWAY_SPECTATOR_REDIS_URL}" || (echo "GATEWAY_SPECTATOR_REDIS_URL required (must use Redis DB 13)" >&2; exit 1)
	cd services/gateway/src && GOWORK=off GOPROXY=off GOSUMDB=off \
		$(GO) test -race -count=1 -tags=redis_integration ./bff/ -timeout 120s

test-gateway-helm:
	./services/gateway/helm/gateway/helm-test.sh

kind-deploy-gateway:
	./infrastructure/kind/scripts/deploy-gateway.sh

kind-test-gateway-adapter-structure:
	./infrastructure/kind/scripts/test-gateway-adapter-structure.sh

kind-test-gateway-adapter:
	./infrastructure/kind/scripts/test-gateway-adapter-structure.sh
	./infrastructure/kind/scripts/test-gateway-adapter.sh

kind-test-gateway-si-redis-admission-live:
	./infrastructure/kind/scripts/test-gateway-si-redis-admission-live.sh

# Compatibility alias (exact proof is Redis admission only; see si-redis-admission-live).
kind-test-gateway-session-invalidation-live: kind-test-gateway-si-redis-admission-live

kind-test-gateway-integration:
	./infrastructure/kind/scripts/test-gateway-integration-structure.sh
	./infrastructure/kind/scripts/test-gateway-integration.sh

test-identity:
	cd services/identity/src && $(GO_TEST_ENV) $(GO) test ./...

test-ranking:
	cd services/ranking/src && $(GO_TEST_ENV) $(GO) test ./...

test-room-gameplay:
	cd services/room-gameplay/src && $(GO_TEST_ENV) $(GO) test ./...

test-spectator-view:
	cd services/spectator-view/src && $(GO_TEST_ENV) $(GO) test -race -count=1 ./...
	cd services/spectator-view/src && $(GO_TEST_ENV) $(GO) vet ./...

# Live Redis DB 14 integration (tagged). Parent kind harness sets SPECTATOR_REDIS_URL.
# Requires /14 in the URL. Suite uses a per-run random key prefix and cleans only that prefix.
test-spectator-integration:
	@test -n "$${SPECTATOR_REDIS_URL}" || (echo "SPECTATOR_REDIS_URL required (must use Redis DB 14)" >&2; exit 1)
	cd services/spectator-view/src && GOWORK=off GOPROXY=off GOSUMDB=off \
		$(GO) test -race -count=1 -tags=redis_integration ./store/ -timeout 120s

test-spectator-helm:
	./services/spectator-view/helm/spectator-view/helm-test.sh

kind-deploy-spectator-view:
	./infrastructure/kind/scripts/deploy-spectator-view.sh

kind-test-spectator-adapter-structure:
	./infrastructure/kind/scripts/test-spectator-adapter-structure.sh

kind-test-spectator-projection-rebuilder-structure:
	./infrastructure/kind/scripts/test-spectator-projection-rebuilder-structure.sh

kind-test-spectator-projection-rebuilder-live:
	./infrastructure/kind/scripts/test-spectator-projection-rebuilder-live.sh

kind-test-spectator-adapter:
	./infrastructure/kind/scripts/test-spectator-adapter-structure.sh
	./infrastructure/kind/scripts/test-spectator-adapter.sh

kind-test-spectator-integration:
	./infrastructure/kind/scripts/test-spectator-integration-structure.sh
	./infrastructure/kind/scripts/test-spectator-integration.sh

kind-test-redis-aof-structure:
	./infrastructure/kind/scripts/test-redis-aof-structure.sh

kind-test-redis-aof:
	./infrastructure/kind/scripts/test-redis-aof-structure.sh
	./infrastructure/kind/scripts/test-redis-aof.sh

test-tournament-orchestration:
	cd services/tournament-orchestration/src && $(GO_TEST_ENV) $(GO) test ./...

test-telemetry:
	cd platform/telemetry && $(GO_TEST_ENV) $(GO) test ./...

test-modules: test-shared test-telemetry \
	test-analytics test-game-integrity test-gateway test-identity \
	test-ranking test-room-gameplay test-spectator-view test-tournament-orchestration

vet-shared:
	cd shared && $(GO_TEST_ENV) $(GO) vet ./...

build-shared:
	cd shared && $(GO_TEST_ENV) $(GO) build ./...

validate-yaml:
	@command -v $(RUBY) >/dev/null 2>&1 || { echo "ruby unavailable; cannot validate YAML" >&2; exit 1; }
	@$(RUBY) -ryaml -e 'paths=%w[contracts/openapi/bff-v1.yaml contracts/asyncapi/kafka-v1.yaml docker-compose.local.yml docker-compose.capability.yml]; paths.each{|p| YAML.load_file(p); puts "ok yaml: #{p}"}'

validate-compose:
	@docker compose -f docker-compose.local.yml --env-file .env.example config >/dev/null
	@echo "ok compose config"
	@docker compose -f docker-compose.local.yml -f docker-compose.capability.yml --env-file .env.example config >/dev/null
	@echo "ok compose capability overlay"

# Resolved config only (no up / no pull): gateway on edge+private, backends
# private-only, concrete BFF_HOST_PORT, capability project-scoped network names.
test-compose-topology:
	./client-checkpoint/tests/test-compose-topology.sh

# Offline Client Checkpoint CLI against ephemeral stdlib fake BFF (no Docker).
test-client-checkpoint:
	./client-checkpoint/tests/run-tests.sh

# Dockerfile / packaging structure only (no docker build, no pull).
test-client-dockerfile:
	./client-checkpoint/tests/test-dockerfile-structure.sh

# Boots an isolated compose project (-p), polls BFF /health then /ready, exercises
# real-service capability paths via client-checkpoint CLI + curl, then
# `down -v --remove-orphans` for a harness-started project only.
# KEEP_STACK=1 leaves the stack up. CAP_SKIP_UP=1 reuses caller UNOARENA_API_URL
# without tearing down external volumes (CAP_TEARDOWN_EXTERNAL=1 to opt in).
test-capability-stack:
	./client-checkpoint/tests/run-capability-stack.sh

check: fmt-shared vet-shared test-modules validate-yaml
	@git diff --check
	@echo "check passed"

# --- kind local foundation (Slice 0). create/apply/reset are explicit only. ---
kind-render:
	./infrastructure/kind/scripts/render.sh

kind-validate:
	./infrastructure/kind/scripts/validate.sh

kind-create-cluster:
	./infrastructure/kind/scripts/create-cluster.sh

kind-verify-portable-images:
	./infrastructure/kind/scripts/verify-portable-images.sh

kind-build-load-bootstrap:
	./infrastructure/kind/scripts/build-load-bootstrap.sh

kind-build-load-services:
	./infrastructure/kind/scripts/build-load-services.sh

kind-install-istio:
	./infrastructure/kind/scripts/install-istio.sh

kind-deploy-observability:
	./infrastructure/kind/scripts/deploy-observability.sh

kind-wait-observability:
	./infrastructure/kind/scripts/wait-observability.sh

kind-test-observability-structure:
	./infrastructure/kind/scripts/test-observability-structure.sh

kind-test-observability-live:
	./infrastructure/kind/scripts/test-observability-live.sh

kind-test-observability-business-live:
	./infrastructure/kind/scripts/test-observability-business-live.sh

kind-test-observability-trace-live:
	./infrastructure/kind/scripts/test-observability-trace-live.sh

kind-test-observability-security-live:
	./infrastructure/kind/scripts/test-observability-security-live.sh

kind-test-observability-outage-live:
	./infrastructure/kind/scripts/test-observability-outage-live.sh

kind-test-observability-persistence-live:
	./infrastructure/kind/scripts/test-observability-persistence-live.sh

kind-test-observability-acceptance-live:
	./infrastructure/kind/scripts/test-observability-live.sh
	./infrastructure/kind/scripts/test-observability-business-live.sh
	./infrastructure/kind/scripts/test-observability-trace-live.sh
	./infrastructure/kind/scripts/test-observability-security-live.sh
	./infrastructure/kind/scripts/test-observability-outage-live.sh
	./infrastructure/kind/scripts/test-observability-persistence-live.sh

kind-port-forward-grafana:
	./infrastructure/kind/scripts/port-forward-grafana.sh

kind-deploy-game-integrity:
	./infrastructure/kind/scripts/deploy-game-integrity.sh

kind-deploy-services:
	./infrastructure/kind/scripts/deploy-services.sh

kind-run-live-probes:
	./infrastructure/kind/scripts/run-live-probes.sh

kind-clean-deploy:
	./infrastructure/kind/scripts/clean-deploy.sh --confirm-reset uno-arena

kind-test-clean-deployment-structure:
	./infrastructure/kind/scripts/test-clean-deployment-structure.sh

kind-load-debezium-connect:
	./infrastructure/kind/scripts/load-debezium-connect.sh

kind-test-debezium-connect-structure:
	./infrastructure/kind/scripts/test-debezium-connect-structure.sh

kind-test-debezium-connectors:
	./infrastructure/kind/scripts/test-debezium-connect-structure.sh
	./infrastructure/kind/scripts/test-debezium-connectors.sh

kind-test-debezium-postgres-to-kafka-live:
	./infrastructure/kind/scripts/test-debezium-postgres-to-kafka-live.sh

kind-load-debezium-server:
	./infrastructure/kind/scripts/load-debezium-server.sh

kind-status-debezium-server:
	./infrastructure/kind/scripts/status-debezium-server.sh

kind-test-debezium-server-structure:
	./infrastructure/kind/scripts/test-debezium-server-structure.sh

kind-test-debezium-postgres-to-redis-live:
	./infrastructure/kind/scripts/test-debezium-postgres-to-redis-live.sh

kind-apply:
	./infrastructure/kind/scripts/apply.sh

kind-wait:
	./infrastructure/kind/scripts/wait.sh

kind-reset:
	./infrastructure/kind/scripts/reset.sh
