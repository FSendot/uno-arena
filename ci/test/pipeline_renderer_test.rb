# frozen_string_literal: true

require_relative "test_helper"

class PipelineRendererTest < Minitest::Test
  include ImpactTestHelpers

  def render(paths, source: "push", branch: "main", deploy_environments: "local-production")
    impact = plan(paths, source: source, branch: branch, deploy_environments: deploy_environments)
    YAML.safe_load(UnoArenaCI::PipelineRenderer.new(impact_map, impact).render, [], [], true)
  end

  def test_source_change_renders_test_build_and_publish_without_chart
    pipeline = render(["services/gateway/src/main.go"])
    assert pipeline.key?("test:gateway")
    refute pipeline.key?("build:image:gateway")
    assert pipeline.key?("publish:image:gateway")
    assert pipeline.key?("promote:desired-state")
    assert pipeline.key?("reconcile:wait")
    assert pipeline.key?("verify:post-deploy")
    assert pipeline.key?("rollback:stateless")
    refute pipeline.key?("lint:chart:gateway")
    refute pipeline.key?("publish:chart:gateway")
  end

  def test_merge_request_renders_build_but_not_publish
    pipeline = render(["services/gateway/src/main.go"], source: "merge_request_event", branch: nil)
    assert pipeline.key?("build:image:gateway")
    refute pipeline.key?("publish:image:gateway")
  end

  def test_docs_only_child_is_not_empty
    pipeline = render(["docs/adr/0048-incremental-production-cluster-reconciliation.md"])
    assert_equal ".validate-repository", pipeline.fetch("validate:repository").fetch("extends")
    assert_equal %w[validate test build publish promote reconcile verify rollback], pipeline.fetch("stages")
  end

  def test_inventory_change_adds_inventory_validation
    pipeline = render(["environments/local-production/services/identity.yaml"])
    assert pipeline.key?("validate:inventories")
  end

  def test_argocd_change_waits_for_control_plane_and_all_released_applications
    pipeline = render(["environments/local-production/argocd/templates/root-application.yaml"])
    applications = pipeline.dig("reconcile:wait", "variables", "ARGOCD_APPLICATIONS_LOCAL_PRODUCTION").split(",")
    expected = released_local_production_applications + %w[
      uno-arena-local-production-foundations
      uno-arena-local-production-root
    ]
    assert_equal expected.uniq.sort, applications
  end


  def test_trigger_delivery_uses_portable_single_job_image_publication
    pipeline = render(["services/gateway/src/main.go"])
    refute pipeline.key?("build:image:gateway")
    publish = pipeline.fetch("publish:image:gateway")
    assert_equal ["test:gateway"], publish.fetch("needs")
  end


  def test_platform_chart_pipeline_publishes_promotes_waits_and_collects_evidence_without_rollback
    pipeline = render(["infrastructure/local-production/charts/kafka/templates/resources.yaml"])
    assert pipeline.key?("lint:platform-chart:kafka")
    assert pipeline.key?("validate:inventories")
    assert pipeline.key?("publish:platform-chart:kafka")
    refute pipeline.key?("publish:platform-chart:redis")
    promote = pipeline.fetch("promote:desired-state")
    assert_equal "kafka", promote.dig("variables", "PROMOTE_PLATFORM_COMPONENTS")
    assert_includes promote.fetch("needs"), { "job" => "publish:platform-chart:kafka", "artifacts" => true }
    assert_equal [{ "job" => "promote:desired-state", "artifacts" => true }], pipeline.fetch("reconcile:wait").fetch("needs")
    assert_includes pipeline.dig("reconcile:wait", "variables", "ARGOCD_APPLICATIONS_LOCAL_PRODUCTION"),
                    "uno-arena-local-production-kafka"
    assert_equal "local-production", pipeline.dig("reconcile:wait", "variables", "DEPLOY_ENVIRONMENTS")
    assert pipeline.key?("verify:post-deploy")
    assert_includes pipeline.dig("verify:post-deploy", "variables", "ARGOCD_APPLICATIONS_LOCAL_PRODUCTION"),
                    "uno-arena-local-production-kafka"
    refute pipeline.key?("rollback:stateless")
  end

  def test_service_delivery_is_constrained_to_configured_environment_and_rollback_matches
    pipeline = render(["services/gateway/src/main.go"])
    assert_equal "local-production", pipeline.dig("promote:desired-state", "variables", "DEPLOY_ENVIRONMENTS")
    assert_equal "", pipeline.dig("reconcile:wait", "variables", "ARGOCD_APPLICATIONS_PRODUCTION")
    assert_equal "local-production", pipeline.dig("rollback:stateless", "variables", "DEPLOY_ENVIRONMENTS")
    assert_equal "uno-arena-local-production-gateway",
                 pipeline.dig("rollback:stateless", "variables", "ARGOCD_APPLICATIONS_LOCAL_PRODUCTION")
  end

  def test_context_bootstrap_image_is_self_contained_publish_job
    pipeline = render(["infrastructure/bootstrap/Dockerfile"])
    publish = pipeline.fetch("publish:platform-image:context-bootstrap")
    assert_equal "platform", publish.dig("variables", "COMPONENT_KIND")
    assert_equal "infrastructure/bootstrap/Dockerfile", publish.dig("variables", "DOCKERFILE")
    assert_equal "bootstrap", publish.dig("variables", "IMAGE_NAME")
    refute pipeline.key?("build:platform-image:context-bootstrap")
  end

  def test_observability_uses_its_canonical_chart_identity_and_local_values
    pipeline = render(["infrastructure/observability/helm/uno-arena-observability/Chart.yaml"])
    variables = pipeline.dig("lint:platform-chart:observability", "variables")
    assert_equal "infrastructure/observability/helm/uno-arena-observability", variables["CHART_PATH"]
    assert_equal "uno-arena-observability", variables["CHART_NAME"]
    assert_equal "values.local-production.yaml", variables["CHART_VALUES"]
  end

  def test_production_observability_delivery_targets_the_production_application
    pipeline = render(
      ["infrastructure/observability/helm/uno-arena-observability/Chart.yaml"],
      deploy_environments: "production"
    )
    assert_equal "production", pipeline.dig("reconcile:wait", "variables", "DEPLOY_ENVIRONMENTS")
    assert_equal "uno-arena-production-observability",
                 pipeline.dig("reconcile:wait", "variables", "ARGOCD_APPLICATIONS_PRODUCTION")
    assert_equal "", pipeline.dig("reconcile:wait", "variables", "ARGOCD_APPLICATIONS_LOCAL_PRODUCTION")
    refute pipeline.key?("rollback:stateless")
  end

  private

  def released_local_production_applications
    %w[services platform].flat_map do |inventory|
      Dir[File.join("environments/local-production", inventory, "enabled", "*.yaml")]
    end.map do |path|
      YAML.safe_load(File.read(path), [], [], true).fetch("application")
    end
  end
end
