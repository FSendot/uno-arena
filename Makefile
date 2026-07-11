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
	test-analytics test-game-integrity test-gateway test-identity \
	test-ranking test-room-gameplay test-spectator-view test-tournament-orchestration \
	test-modules validate-yaml validate-compose test-compose-topology \
	test-capability-stack check build-shared

help:
	@echo "Targets (no network fetch unless noted):"
	@echo "  fmt-shared             gofmt shared module"
	@echo "  test-shared            go test ./shared/... with GOWORK=off GOPROXY=off"
	@echo "  test-modules           test shared + all eight service modules (GOWORK=off)"
	@echo "  vet-shared             go vet ./shared/... with GOPROXY=off"
	@echo "  validate-yaml          parse OpenAPI/AsyncAPI/compose YAML via Ruby stdlib"
	@echo "  validate-compose       docker compose config (requires local docker; no pull)"
	@echo "  test-compose-topology  edge+private network assertions on resolved config (no stack)"
	@echo "  test-capability-stack  Docker capability overlay + CLI/BFF integration harness"
	@echo "  check                  fmt + vet + test-modules + validate-yaml + git diff --check"
	@echo "  build-shared           go build ./shared/... with GOPROXY=off"

fmt-shared:
	$(GOFMT) -w shared

test-shared:
	cd shared && $(GO_TEST_ENV) $(GO) test ./...

test-analytics:
	cd services/analytics/src && $(GO_TEST_ENV) $(GO) test ./...

test-game-integrity:
	cd services/game-integrity/src && $(GO_TEST_ENV) $(GO) test ./...

test-gateway:
	cd services/gateway/src && $(GO_TEST_ENV) $(GO) test ./...

test-identity:
	cd services/identity/src && $(GO_TEST_ENV) $(GO) test ./...

test-ranking:
	cd services/ranking/src && $(GO_TEST_ENV) $(GO) test ./...

test-room-gameplay:
	cd services/room-gameplay/src && $(GO_TEST_ENV) $(GO) test ./...

test-spectator-view:
	cd services/spectator-view/src && $(GO_TEST_ENV) $(GO) test ./...

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
