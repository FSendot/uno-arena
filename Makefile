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
	test-analytics test-game-integrity test-game-integrity-integration deps-game-integrity \
	test-gateway test-identity \
	test-ranking test-room-gameplay test-spectator-view test-tournament-orchestration \
	test-modules validate-yaml validate-compose test-compose-topology \
	test-capability-stack check build-shared \
	kind-render kind-validate kind-create-cluster kind-build-load-bootstrap \
	kind-apply kind-wait kind-reset kind-test-game-integrity-adapter \
	kind-deploy-identity kind-test-identity-adapter kind-test-identity-integration \
	test-identity-helm test-identity-integration \
	kind-deploy-room-gameplay kind-test-room-adapter-structure kind-test-room-integration \
	test-room-helm test-room-integration \
	kind-deploy-tournament-orchestration kind-test-tournament-adapter \
	kind-test-tournament-adapter-structure kind-test-tournament-integration \
	test-tournament-helm test-tournament-integration \
	kind-deploy-ranking kind-test-ranking-adapter kind-test-ranking-adapter-structure \
	kind-test-ranking-integration test-ranking-helm test-ranking-integration \
	kind-deploy-analytics kind-test-analytics-adapter kind-test-analytics-adapter-structure \
	kind-test-analytics-integration test-analytics-helm test-analytics-integration \
	kind-deploy-spectator-view kind-test-spectator-adapter kind-test-spectator-adapter-structure \
	kind-test-spectator-integration test-spectator-helm test-spectator-integration \
	kind-deploy-gateway kind-test-gateway-adapter kind-test-gateway-adapter-structure \
	kind-test-gateway-integration test-gateway-helm test-gateway-integration \
	kind-test-redis-aof kind-test-redis-aof-structure

help:
	@echo "Targets (no network fetch unless noted):"
	@echo "  fmt-shared             gofmt shared module"
	@echo "  test-shared            go test ./shared/... with GOWORK=off GOPROXY=off"
	@echo "  test-modules           test shared + all eight service modules (GOWORK=off)"
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
	@echo "  kind-test-tournament-adapter EXPLICIT/NETWORKED: Tournament durable create/outbox (no CDC claim)"
	@echo "  kind-test-tournament-adapter-structure offline Tournament deploy/adapter/PF structure checks"
	@echo "  kind-test-tournament-integration EXPLICIT/NETWORKED: ephemeral test DB + Tournament store integration"
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
	@echo "  kind-test-analytics-integration EXPLICIT/NETWORKED: ephemeral test DB + Analytics store integration"
	@echo "  test-analytics-helm          offline Analytics helm lint/template checks"
	@echo "  test-analytics-integration   live ClickHouse integration (safe unoarena_analytics_test_* DB only)"
	@echo "  kind-deploy-spectator-view   EXPLICIT: helm upgrade --install Spectator View (kind values)"
	@echo "  kind-test-spectator-adapter  EXPLICIT/NETWORKED: Spectator durable Redis ingest (Kafka PENDING)"
	@echo "  kind-test-spectator-adapter-structure offline Spectator deploy/adapter/PF structure checks"
	@echo "  kind-test-spectator-integration EXPLICIT/NETWORKED: Redis DB 14 + tagged store suite"
	@echo "  kind-deploy-gateway          EXPLICIT: helm upgrade --install Gateway (kind values)"
	@echo "  kind-test-gateway-adapter    EXPLICIT/NETWORKED: Gateway durable Redis ready (Kafka/Debezium PENDING)"
	@echo "  kind-test-gateway-adapter-structure offline Gateway deploy/adapter/PF structure checks"
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
	@echo "  check                  fmt + vet + test-modules + validate-yaml + git diff --check"
	@echo "  build-shared           go build ./shared/... with GOPROXY=off"
	@echo "  kind-render            offline AsyncAPI → kind Kafka topic artifacts"
	@echo "  kind-validate          offline kind foundation checks incl. CDC/retention (no pull / no cluster)"
	@echo "  kind-create-cluster    EXPLICIT: create disposable kind cluster uno-arena"
	@echo "  kind-build-load-bootstrap  EXPLICIT: docker build + kind load bootstrap image"
	@echo "  kind-apply             EXPLICIT: apply foundation manifests (context kind-uno-arena)"
	@echo "  kind-wait              wait for foundation Deployments + bootstrap Jobs"
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
test-tournament-integration:
	@test -n "$$TOURNAMENT_POSTGRES_URL" || (echo "TOURNAMENT_POSTGRES_URL required" >&2; exit 1)
	cd services/tournament-orchestration/src && GOWORK=off GOPROXY=off GOSUMDB=off \
		$(GO) test -count=1 -tags=integration -timeout 180s ./store/...

# EXPLICIT + NETWORKED: verifies kind-uno-arena, creates unoarena_tournament_test_*,
# port-forwards postgres-tournament, runs store integration as tournament_runtime, drops DB.
# Never applies/resets/deploys. Never accepts caller-supplied database names.
kind-test-tournament-integration:
	./infrastructure/kind/scripts/test-tournament-integration-structure.sh
	./infrastructure/kind/scripts/test-tournament-integration.sh

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

# EXPLICIT + NETWORKED: verifies kind-uno-arena, creates unoarena_ranking_test_*,
# port-forwards postgres-ranking, runs store integration as ranking_runtime, drops DB.
# Never applies/resets/deploys. Never accepts caller-supplied database names.
kind-test-ranking-integration:
	./infrastructure/kind/scripts/test-ranking-integration-structure.sh
	./infrastructure/kind/scripts/test-ranking-integration.sh

kind-deploy-analytics:
	./infrastructure/kind/scripts/deploy-analytics.sh

kind-test-analytics-adapter-structure:
	./infrastructure/kind/scripts/test-analytics-adapter-structure.sh

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

test-modules: test-shared \
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

kind-build-load-bootstrap:
	./infrastructure/kind/scripts/build-load-bootstrap.sh

kind-apply:
	./infrastructure/kind/scripts/apply.sh

kind-wait:
	./infrastructure/kind/scripts/wait.sh

kind-reset:
	./infrastructure/kind/scripts/reset.sh
