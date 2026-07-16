# frozen_string_literal: true

require_relative "test_helper"

class GeneratorIntegrationTest < Minitest::Test
  def root_pipeline_documents
    content = File.read(".gitlab-ci.yml")
    header, pipeline = content.split(/^---\s*$\n?/, 2)
    return [{}, YAML.safe_load(header, [], [], true)] unless pipeline

    [YAML.safe_load(header, [], [], true), YAML.safe_load(pipeline, [], [], true)]
  end

  def test_trigger_job_does_not_inherit_runner_tags
    _header, pipeline = root_pipeline_documents
    assert_equal false, pipeline.dig("dispatch:impact", "inherit", "default")
    refute pipeline.fetch("dispatch:impact").key?("tags")
  end

  def test_manual_pipeline_exposes_release_selector_input
    header, pipeline = root_pipeline_documents
    input = header.dig("spec", "inputs", "run_component")

    refute_nil input
    assert_equal "all", input["default"]
    assert_equal ["all"] + UnoArenaCI::ImpactMap.load("ci/impact-map.yaml").release_names,
                 input["options"]
    assert_equal "$[[ inputs.run_component ]]", pipeline.dig("variables", "RUN_COMPONENT")
  end

  def test_reconciliation_uses_read_only_argocd_api_without_cluster_credentials
    wait = File.read("ci/bin/wait-for-argocd")
    assert_includes wait, "Authorization: Bearer"
    refute_match(/kubectl|KUBECONFIG/, wait)
    rollback = File.read("ci/templates/impact-child.gitlab-ci.yml")
    assert_includes rollback, "Restores only stateless service inventory pins"
    template = YAML.safe_load(rollback, [], [], true)
    assert_equal "uno-arena-production-desired-state", template.dig(".promote-releases", "resource_group")
    assert_equal "uno-arena-production-desired-state", template.dig(".rollback-releases", "resource_group")
  end

  def test_generator_writes_parseable_child_and_impact_artifacts
    Dir.mktmpdir do |dir|
      changes = File.join(dir, "changes.txt")
      child = File.join(dir, "child.yml")
      impact = File.join(dir, "impact.json")
      File.write(changes, "services/gateway/src/main.go\n")
      stdout, stderr, status = Open3.capture3(
        "ruby", "ci/bin/generate-impact-pipeline.rb",
        "--changed-file", changes,
        "--source", "push",
        "--branch", "main",
        "--deploy-environments", "local-production",
        "--output", child,
        "--impact-output", impact
      )
      assert status.success?, "#{stdout}\n#{stderr}"
      pipeline = YAML.safe_load(File.read(child), [], [], true)
      result = JSON.parse(File.read(impact))
      assert pipeline.key?("publish:image:gateway")
      assert result.dig("components", "gateway", "publishImage")
    end
  end


  def test_generator_emits_only_affected_platform_release_jobs
    Dir.mktmpdir do |dir|
      changes = File.join(dir, "changes.txt")
      child = File.join(dir, "child.yml")
      impact = File.join(dir, "impact.json")
      File.write(changes, "infrastructure/local-production/charts/redis/Chart.yaml\n")
      stdout, stderr, status = Open3.capture3(
        "ruby", "ci/bin/generate-impact-pipeline.rb", "--changed-file", changes,
        "--source", "push", "--branch", "main", "--deploy-environments", "local-production",
        "--output", child, "--impact-output", impact
      )
      assert status.success?, "#{stdout}\n#{stderr}"
      pipeline = YAML.safe_load(File.read(child), [], [], true)
      assert pipeline.key?("publish:platform-chart:redis")
      refute pipeline.key?("publish:platform-chart:kafka")
      assert JSON.parse(File.read(impact)).dig("platformComponents", "redis", "publishChart")
    end
  end
end
