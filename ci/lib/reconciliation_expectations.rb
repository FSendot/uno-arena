# frozen_string_literal: true

require "json"
require "yaml"
require_relative "impact_map"

module UnoArenaCI
  class ReconciliationExpectations
    ENVIRONMENTS = %w[production local-production].freeze

    def initialize(root = Dir.pwd, desired_revision: ENV["DESIRED_STATE_REVISION"] || ENV["CI_COMMIT_SHA"])
      @root = root
      @desired_revision = desired_revision
    end

    def build(applications_by_environment)
      unknown = applications_by_environment.keys - ENVIRONMENTS
      raise ConfigurationError, "unknown reconciliation environments: #{unknown.join(', ')}" unless unknown.empty?
      applications = ENVIRONMENTS.to_h do |environment|
        names = Array(applications_by_environment.fetch(environment, [])).map(&:to_s).reject(&:empty?).uniq
        [environment, names.to_h { |application| [application, expectation(environment, application)] }]
      end
      { "schemaVersion" => 1, "applications" => applications }
    end

    private

    def expectation(environment, application)
      control = control_plane_expectation(environment, application)
      return control if control

      service_path = File.join(@root, "environments", environment, "services", "#{component_name(environment, application)}.yaml")
      return service_expectation(service_path, environment, application) if File.file?(service_path)

      platform_directory = environment == "production" ? "platform-releases" : "platform"
      platform_path = File.join(@root, "environments", environment, platform_directory,
                                "#{component_name(environment, application)}.yaml")
      return platform_expectation(platform_path, application, environment) if File.file?(platform_path)
      raise ConfigurationError, "cannot resolve reconciliation inventory for #{application}"
    end

    def control_plane_expectation(environment, application)
      paths = {
        "uno-arena-local-production-root" => "environments/local-production/argocd",
        "uno-arena-local-production-foundations" => "infrastructure/local-production/gitops/foundation"
      }
      path = paths[application]
      return nil unless path
      raise ConfigurationError, "desired-state revision is required for #{application}" unless @desired_revision.to_s.match?(/\A[a-f0-9]{40}\z/)
      raise ConfigurationError, "control-plane application #{application} is outside #{environment}" unless environment == "local-production"
      {"kind" => "git-control", "environment" => environment, "path" => path, "revision" => @desired_revision}
    end

    def component_name(environment, application)
      prefix = "uno-arena-#{environment}-"
      raise ConfigurationError, "application #{application} is outside #{environment}" unless application.start_with?(prefix)
      application.delete_prefix(prefix)
    end

    def load_released(path, application)
      document = YAML.safe_load(File.read(path), [], [], true)
      unless document.is_a?(Hash) && document["application"] == application && document["enabled"] == true && document["status"] == "released"
        raise ConfigurationError, "reconciliation inventory is not released for #{application}"
      end
      document
    rescue Psych::Exception => e
      raise ConfigurationError, "cannot parse reconciliation inventory #{path}: #{e.message}"
    end

    def service_expectation(path, environment, application)
      document = load_released(path, application)
      {
        "kind" => "service",
        "component" => document.fetch("component"),
        "environment" => environment,
        "chart" => document.fetch("chart").slice("repository", "name", "version", "packageSha256"),
        "image" => document.fetch("image").slice("repository", "digest"),
        "values" => document.fetch("values").slice("repository", "revision")
      }
    end

    def platform_expectation(path, application, environment)
      document = load_released(path, application)
      {
        "kind" => "platform",
        "component" => document.fetch("component"),
        "environment" => environment,
        "chart" => document.fetch("chart").slice("repository", "name", "version", "packageSha256"),
        "values" => document.fetch("values")
      }
    end
  end

  class ArgoApplicationVerifier
    def verify(document, expectation, application)
      return failure("Argo response name does not match #{application}") unless document.dig("metadata", "name") == application
      return failure("#{application} is not Synced") unless document.dig("status", "sync", "status") == "Synced"
      return failure("#{application} is not Healthy") unless document.dig("status", "health", "status") == "Healthy"

      case expectation.fetch("kind")
      when "service" then verify_service(document, expectation, application)
      when "platform" then verify_platform(document, expectation, application)
      when "git-control" then verify_git_control(document, expectation, application)
      else failure("#{application} has an unknown expectation kind")
      end
    rescue KeyError, Psych::Exception => e
      failure("#{application} response is incomplete: #{e.message}")
    end

    private

    def verify_service(document, expectation, application)
      sources = Array(document.dig("spec", "sources"))
      chart_source = sources.find { |source| source["chart"] }
      values_source = sources.find { |source| source["ref"] == "values" }
      return failure("#{application} chart source does not match promoted inventory") unless source_matches?(chart_source, expectation.fetch("chart"))
      values = expectation.fetch("values")
      unless values_source && values_source["repoURL"] == values.fetch("repository") && values_source["targetRevision"] == values.fetch("revision")
        return failure("#{application} values revision does not match promoted inventory")
      end
      rendered_values = YAML.safe_load(chart_source.dig("helm", "values").to_s, [], [], true) || {}
      return failure("#{application} image does not match promoted inventory") unless rendered_values["image"] == expectation.fetch("image")
      revisions = Array(document.dig("status", "sync", "revisions"))
      expected_revisions = [expectation.dig("chart", "version"), values.fetch("revision")]
      return failure("#{application} has not synced the promoted source revisions") unless revisions == expected_revisions
      success
    end

    def verify_platform(document, expectation, application)
      source = document.dig("spec", "source")
      return failure("#{application} chart source does not match promoted inventory") unless source_matches?(source, expectation.fetch("chart"))
      rendered_values = YAML.safe_load(source.dig("helm", "values").to_s, [], [], true) || {}
      return failure("#{application} values do not match promoted inventory") unless rendered_values == expectation.fetch("values")
      return failure("#{application} has not synced the promoted chart revision") unless document.dig("status", "sync", "revision") == expectation.dig("chart", "version")
      success
    end

    def verify_git_control(document, expectation, application)
      source = document.dig("spec", "source")
      return failure("#{application} Git path does not match repository control-plane inventory") unless source && source["path"] == expectation.fetch("path")
      return failure("#{application} has not synced the exact desired-state revision") unless document.dig("status", "sync", "revision") == expectation.fetch("revision")
      success
    end

    def source_matches?(source, chart)
      source && source["repoURL"] == chart.fetch("repository") && source["chart"] == chart.fetch("name") &&
        source["targetRevision"] == chart.fetch("version")
    end

    def success
      [true, nil]
    end

    def failure(message)
      [false, message]
    end
  end
end
