#!/usr/bin/env ruby
# frozen_string_literal: true

require "fileutils"
require "digest"
require "json"
require "yaml"
require_relative "../lib/impact_map"

components = ENV.fetch("PROMOTE_COMPONENTS", "").split(",").map(&:strip).reject(&:empty?).uniq
platform_components = ENV.fetch("PROMOTE_PLATFORM_COMPONENTS", "").split(",").map(&:strip).reject(&:empty?).uniq
environments = ENV.fetch("DEPLOY_ENVIRONMENTS").split(",").map(&:strip).reject(&:empty?)
sha = ENV.fetch("CI_COMMIT_SHA")
project_url = ENV.fetch("CI_PROJECT_URL")
abort "CI_COMMIT_SHA must be a full SHA" unless sha.match?(/\A[a-f0-9]{40}\z/)
abort "promotion environments must not be empty" if environments.empty?
abort "promotion environments must be unique" unless environments.uniq == environments
abort "invalid promotion environments" unless (environments - %w[production local-production]).empty?
abort "no promotion components" if components.empty? && platform_components.empty?

verify_chart_package = lambda do |component, chart|
  package_sha = chart.fetch("packageSha256")
  abort "invalid promoted chart package SHA-256 for #{component}" unless package_sha.match?(/\A[a-f0-9]{64}\z/)
  package = "artifacts/#{chart.fetch('name')}-#{chart.fetch('version')}.tgz"
  abort "missing promoted chart package #{package}" unless File.file?(package)
  actual = Digest::SHA256.file(package).hexdigest
  abort "promoted chart package checksum mismatch for #{component}" unless actual == package_sha
end

components.each do |component|
  image_path = "artifacts/#{component}-image.json"
  chart_path = "artifacts/#{component}-chart.json"
  image_artifact = File.file?(image_path) ? JSON.parse(File.read(image_path)) : nil
  chart_artifact = File.file?(chart_path) ? JSON.parse(File.read(chart_path)) : nil
  [image_artifact, chart_artifact].compact.each do |artifact|
    abort "artifact component mismatch for #{component}" unless artifact["component"] == component
    abort "artifact source SHA mismatch for #{component}" unless artifact["sourceSha"] == sha
  end
  image = image_artifact && image_artifact.fetch("image")
  chart = chart_artifact && chart_artifact.fetch("chart")
  if image
    abort "invalid promoted image digest" unless image.fetch("digest").match?(/\Asha256:[a-f0-9]{64}\z/)
    abort "invalid promoted image repository" if image.fetch("repository").include?("example.invalid")
  end
  if chart
    abort "invalid promoted chart version" unless chart.fetch("version").match?(/\A[0-9]+\.[0-9]+\.[0-9]+(?:[-+][0-9A-Za-z.-]+)?\z/)
    verify_chart_package.call(component, chart)
  end
  environments.each do |environment|
    canonical = "environments/#{environment}/services/#{component}.yaml"
    abort "missing inventory #{canonical}" unless File.file?(canonical)
    document = YAML.safe_load(File.read(canonical), [], [], true)
    abort "invalid promoted chart name" if chart && chart.fetch("name") != document.dig("chart", "name")
    if document["status"] == "bootstrap-placeholder" && (!image || !chart)
      abort "initial promotion of #{component} requires both immutable image and chart artifacts"
    end
    rollback = "artifacts/rollback/#{environment}/#{component}.yaml"
    FileUtils.mkdir_p(File.dirname(rollback))
    FileUtils.cp(canonical, rollback)
    document["enabled"] = true
    document["status"] = "released"
    document["image"] = image if image
    document["chart"] = chart.slice("repository", "name", "version", "packageSha256") if chart
    document.fetch("values")["repository"] = "#{project_url}.git"
    document.fetch("values")["revision"] = sha
    File.write(canonical, YAML.dump(document))
    enabled = "environments/#{environment}/services/enabled/#{component}.yaml"
    FileUtils.mkdir_p(File.dirname(enabled))
    FileUtils.cp(canonical, enabled)
  end
end

impact_map = UnoArenaCI::ImpactMap.load(File.expand_path("../impact-map.yaml", __dir__))
platform_components.each do |component|
  supported_environments = impact_map.platform_component(component).fetch("environments") & environments
  abort "platform component #{component} is not supported in selected environments" if supported_environments.empty?

  chart_path = "artifacts/#{component}-chart.json"
  chart_artifact = File.file?(chart_path) ? JSON.parse(File.read(chart_path)) : nil
  chart = nil
  if chart_artifact
    abort "artifact component mismatch for #{component}" unless chart_artifact["component"] == component
    abort "artifact source SHA mismatch for #{component}" unless chart_artifact["sourceSha"] == sha
    chart = chart_artifact.fetch("chart")
    expected_chart_name = component == "observability" ? "uno-arena-observability" : component
    abort "invalid promoted chart name" unless chart.fetch("name") == expected_chart_name
    abort "invalid promoted chart version" unless chart.fetch("version").match?(/\A[0-9]+\.[0-9]+\.[0-9]+(?:[-+][0-9A-Za-z.-]+)?\z/)
    verify_chart_package.call(component, chart)
  end

  image_path = "artifacts/#{component}-image.json"
  image_artifact = File.file?(image_path) ? JSON.parse(File.read(image_path)) : nil
  if image_artifact
    abort "image artifact component mismatch for #{component}" unless image_artifact["component"] == component
    abort "image artifact source SHA mismatch for #{component}" unless image_artifact["sourceSha"] == sha
  end
  if component != "context-bootstrap" && image_artifact
    abort "only context-bootstrap may publish a platform image"
  end

  supported_environments.each do |environment|
    directory = environment == "production" ? "platform-releases" : "platform"
    canonical = "environments/#{environment}/#{directory}/#{component}.yaml"
    abort "missing platform inventory #{canonical}" unless File.file?(canonical)
    document = YAML.safe_load(File.read(canonical), [], [], true)
    abort "platform inventory component mismatch for #{component}" unless document["component"] == component
    abort "platform inventory environment mismatch for #{component}" unless document["environment"] == environment
    if document["status"] != "released" && chart_artifact.nil?
      abort "initial platform promotion requires an immutable chart artifact for #{component} in #{environment}"
    end

    if component == "context-bootstrap"
      current_digest = document.dig("values", "image", "digest")
      if document["status"] != "released" && image_artifact.nil?
        abort "initial context-bootstrap promotion requires its immutable image artifact"
      end
      if image_artifact
        image = image_artifact.fetch("image")
        abort "invalid promoted bootstrap image digest" unless image.fetch("digest").match?(/\Asha256:[a-f0-9]{64}\z/)
        abort "invalid promoted bootstrap image repository" if image.fetch("repository").include?("example.invalid")
        document.fetch("values").fetch("image").replace(image.slice("repository", "digest"))
      elsif current_digest == "sha256:#{'0' * 64}"
        abort "context-bootstrap cannot be enabled with the zero image digest"
      end
    end

    document["enabled"] = true
    document["status"] = "released"
    document["chart"] = chart.slice("repository", "name", "version", "packageSha256") if chart_artifact
    if environment == "production" && YAML.dump(document).match?(/example\.invalid|PROJECT_ID|GROUP\/PROJECT|replace-before-release/)
      abort "production platform release #{component} still contains placeholder configuration"
    end

    rollback = "artifacts/rollback/platform/#{environment}/#{component}.yaml"
    FileUtils.mkdir_p(File.dirname(rollback))
    FileUtils.cp(canonical, rollback)
    File.write(canonical, YAML.dump(document))
    enabled = "environments/#{environment}/#{directory}/enabled/#{component}.yaml"
    FileUtils.mkdir_p(File.dirname(enabled))
    FileUtils.cp(canonical, enabled)
  end
end
