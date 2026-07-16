# frozen_string_literal: true

require_relative "test_helper"

class ImpactPlannerTest < Minitest::Test
  include ImpactTestHelpers

  def test_service_source_separates_image_from_chart
    impact = plan(["services/gateway/src/main.go"])
    assert actions(impact, "gateway")["test"]
    assert actions(impact, "gateway")["image"]
    assert actions(impact, "gateway")["publishImage"]
    refute actions(impact, "gateway")["chart"]
    refute actions(impact, "identity")["test"]
  end

  def test_real_production_main_push_is_blocked_without_executable_external_preflight
    error = assert_raises(UnoArenaCI::ImpactError) do
      UnoArenaCI::ImpactPlanner.new(
        impact_map, source: "push", branch: "main", changed_paths: ["services/gateway/src/main.go"],
        deploy_environments: "production"
      ).plan
    end
    assert_match(/external platform contracts have executable CI preflight evidence/, error.message)
    assert_raises(UnoArenaCI::ImpactError) do
      UnoArenaCI::ImpactPlanner.new(
        impact_map, source: "web", branch: "main", run_component: "gateway",
        deploy_environments: "production"
      ).plan
    end
  end

  def test_chart_only_change_does_not_rebuild_image
    impact = plan(["services/identity/helm/identity/templates/deployment.yaml"])
    assert actions(impact, "identity")["chart"]
    assert actions(impact, "identity")["publishChart"]
    refute actions(impact, "identity")["image"]
    refute actions(impact, "identity")["test"]
  end

  def test_shared_go_impacts_only_importers
    impact = plan(["shared/envelope/envelope.go"])
    %w[gateway room-gameplay tournament-orchestration].each do |component|
      assert actions(impact, component)["test"], component
      assert actions(impact, component)["image"], component
    end
    refute actions(impact, "identity")["test"]
  end

  def test_telemetry_impacts_all_service_images
    impact = plan(["platform/telemetry/runtime.go"])
    impact_map.component_names.each do |component|
      assert actions(impact, component)["test"], component
      assert actions(impact, component)["image"], component
    end
  end

  def test_room_contract_tests_producer_and_consumer_without_rebuilding
    impact = plan(["services/room-gameplay/contracts/room.spectator-safe.events.schema.json"])
    assert actions(impact, "room-gameplay")["test"]
    assert actions(impact, "spectator-view")["test"]
    refute actions(impact, "spectator-view")["image"]
  end

  def test_spectator_source_is_an_actual_room_image_input
    impact = plan(["services/spectator-view/src/main.go"])
    assert actions(impact, "spectator-view")["image"]
    assert actions(impact, "room-gameplay")["image"]
    refute actions(impact, "room-gameplay")["test"]
  end

  def test_docs_only_emits_no_component_work
    impact = plan(["docs/README.md"])
    assert impact["repositoryValidation"]
    impact.fetch("components").each_value do |action|
      refute action.values_at("test", "image", "chart", "publishImage", "publishChart").any?
    end
  end

  def test_inventory_only_requests_validation_and_reconciliation
    impact = plan(
      ["environments/production/services/gateway.yaml"],
      deploy_environments: "production,local-production"
    )
    assert impact["inventoryValidation"]
    assert actions(impact, "gateway")["reconcile"]
    refute actions(impact, "gateway")["image"]
  end


  def test_platform_change_reconciles_transitive_dependents_without_rebuilding
    impact = plan(["environments/local-production/platform/postgres-contexts.yaml"])
    impact_map.component_names.each do |component|
      assert actions(impact, component)["reconcile"], component
      refute actions(impact, component)["image"], component
    end
  end

  def test_platform_chart_change_publishes_only_that_platform_chart
    impact = plan(["infrastructure/local-production/charts/kafka/templates/resources.yaml"])
    assert platform_actions(impact, "kafka")["publishChart"]
    assert impact["inventoryValidation"]
    refute platform_actions(impact, "kafka")["publishImage"]
    refute platform_actions(impact, "redis")["chart"]
    impact.fetch("components").each_value { |action| refute action["image"] }
  end

  def test_bootstrap_inputs_build_only_the_context_bootstrap_image
    impact = plan(["services/identity/migrations/001_init.sql"])
    assert platform_actions(impact, "context-bootstrap")["publishImage"]
    refute platform_actions(impact, "context-bootstrap")["publishChart"]
    refute platform_actions(impact, "kafka")["image"]
  end

  def test_mixed_migration_and_source_change_cannot_promote_to_production_without_a_gate
    error = assert_raises(UnoArenaCI::ImpactError) do
      plan(
        ["services/identity/migrations/002_expand.sql", "services/identity/src/main.go"],
        deploy_environments: "production,local-production"
      )
    end
    assert_match(/production migration ordering gate/, error.message)

    local = plan(
      ["services/identity/migrations/002_expand.sql", "services/identity/src/main.go"],
      deploy_environments: "local-production"
    )
    assert actions(local, "identity")["publishImage"]
    assert platform_actions(local, "context-bootstrap")["publishImage"]
  end

  def test_platform_inventory_change_only_reconciles_without_publication
    impact = plan(["environments/local-production/platform/kafka.yaml"])
    assert impact["inventoryValidation"]
    assert platform_actions(impact, "kafka")["reconcile"]
    refute platform_actions(impact, "kafka")["promote"]
  end

  def test_local_argocd_change_tracks_exact_root_and_foundation_applications
    impact = plan(["environments/local-production/argocd/templates/root-application.yaml"])
    assert_equal %w[uno-arena-local-production-foundations uno-arena-local-production-root],
                 impact.dig("controlPlaneApplications", "local-production")
    assert_empty impact.dig("controlPlaneApplications", "production")
  end

  def test_inventory_validation_does_not_short_circuit_later_changed_paths
    impact = plan(["environments/local-production/platform/kafka.yaml", "services/gateway/src/main.go"])
    assert platform_actions(impact, "kafka")["reconcile"]
    assert actions(impact, "gateway")["publishImage"]
  end

  def test_dependency_reconciliation_is_transitive
    impact = plan(["services/identity/src/main.go"])
    assert actions(impact, "identity")["promote"]
    assert actions(impact, "room-gameplay")["reconcile"]
    assert actions(impact, "tournament-orchestration")["reconcile"]
    assert actions(impact, "gateway")["reconcile"]
  end

  def test_merge_request_never_publishes
    impact = plan(["services/gateway/src/main.go"], source: "merge_request_event", branch: nil)
    assert actions(impact, "gateway")["image"]
    refute actions(impact, "gateway")["publishImage"]
  end

  def test_manual_component_and_legacy_alias
    direct = plan([], source: "web", run_component: "gateway")
    legacy = plan([], source: "web", run_service: "gateway")
    assert actions(direct, "gateway")["chart"]
    assert actions(legacy, "gateway")["image"]
    refute actions(direct, "identity")["test"]
  end

  def test_manual_all_and_invalid_selection
    impact = plan([], source: "web", run_component: "all")
    impact_map.component_names.each { |component| assert actions(impact, component)["promote"] }
    impact_map.platform_component_names.each { |component| assert platform_actions(impact, component)["promote"] }
    assert_raises(UnoArenaCI::ImpactError) { plan([], source: "web") }
    assert_raises(UnoArenaCI::ImpactError) { plan([], source: "web", run_component: "wat") }
    assert_raises(UnoArenaCI::ImpactError) { plan([], source: "web", run_component: "gateway", run_service: "identity") }
  end

  def test_deployment_environment_selection_is_fail_closed
    local = plan(["services/gateway/src/main.go"], deploy_environments: "local-production")
    assert_equal ["local-production"], local["deployEnvironments"]
    assert_equal ["local-production"], actions(local, "gateway")["reconcileEnvironments"]

    production = plan(["services/gateway/src/main.go"], deploy_environments: "production")
    assert_equal ["production"], actions(production, "gateway")["reconcileEnvironments"]

    assert_raises(UnoArenaCI::ImpactError) { plan(["docs/README.md"], deploy_environments: "") }
    assert_raises(UnoArenaCI::ImpactError) { plan(["docs/README.md"], deploy_environments: "staging") }
    assert_raises(UnoArenaCI::ImpactError) do
      plan(["docs/README.md"], deploy_environments: "local-production,local-production")
    end
  end

  def test_production_selection_does_not_publish_local_platform_artifacts
    impact = plan(
      ["infrastructure/local-production/charts/kafka/templates/resources.yaml"],
      deploy_environments: "production"
    )
    assert platform_actions(impact, "kafka")["chart"]
    refute platform_actions(impact, "kafka")["publishChart"]
    refute platform_actions(impact, "kafka")["reconcile"]
  end

  def test_production_selection_publishes_environment_supported_platform_artifacts
    impact = plan(
      ["infrastructure/observability/helm/uno-arena-observability/Chart.yaml"],
      deploy_environments: "production"
    )
    assert platform_actions(impact, "observability")["publishChart"]
    assert_equal ["production"], platform_actions(impact, "observability")["reconcileEnvironments"]
  end

  def test_production_managed_platform_inventory_reconciles_only_production
    impact = plan(
      ["environments/production/platform-releases/platform-secrets.yaml"],
      deploy_environments: "production,local-production"
    )
    assert platform_actions(impact, "platform-secrets")["reconcile"]
    assert_equal ["production"], platform_actions(impact, "platform-secrets")["reconcileEnvironments"]
  end

  def test_inventory_reconciliation_remains_environment_scoped
    production = plan(
      ["environments/production/services/gateway.yaml"],
      deploy_environments: "production,local-production"
    )
    assert_equal ["production"], actions(production, "gateway")["reconcileEnvironments"]

    local = plan(
      ["environments/local-production/services/gateway.yaml"],
      deploy_environments: "production,local-production"
    )
    assert_equal ["local-production"], actions(local, "gateway")["reconcileEnvironments"]
  end

  def test_shared_inventory_schema_reconciles_every_selected_environment
    impact = plan(
      ["environments/schema/component-release.schema.yaml"],
      deploy_environments: "production,local-production"
    )
    assert impact["inventoryValidation"]
    impact.fetch("components").each_value do |action|
      assert_equal %w[production local-production], action["reconcileEnvironments"]
    end
  end
end
