# frozen_string_literal: true

require "json"
require "uri"
require "yaml"
require_relative "impact_map"

module UnoArenaCI
  class ReleaseInventory
    COMPONENTS = %w[
      analytics game-integrity gateway identity ranking room-gameplay
      spectator-view tournament-orchestration
    ].freeze
    ENVIRONMENTS = %w[production local-production].freeze
    PRODUCTION_PLATFORM_COMPONENTS = %w[
      cdc clickhouse kafka kurrentdb object-storage oidc postgres-contexts redis
    ].freeze
    PRODUCTION_MANAGED_PLATFORM_COMPONENTS = %w[observability platform-secrets].freeze
    PLATFORM_PREFLIGHT_TYPES = %w[dns tcp tls http sql kafka-metadata s3 oidc-discovery].freeze
    DIGEST = /\Asha256:[a-f0-9]{64}\z/
    PACKAGE_SHA256 = /\A[a-f0-9]{64}\z/
    SEMVER = /\A[0-9]+\.[0-9]+\.[0-9]+(?:[-+][0-9A-Za-z.-]+)?\z/
    PLACEHOLDER = /(example\.invalid|PROJECT_ID|GROUP\/PROJECT)/
    COMMIT_SHA = /\A[a-f0-9]{40}\z/

    attr_reader :root

    def initialize(root = Dir.pwd)
      @root = root
    end

    def validate_all!
      documents = {}
      ENVIRONMENTS.each do |environment|
        COMPONENTS.each do |component|
          path = File.join(root, "environments", environment, "services", "#{component}.yaml")
          raise ConfigurationError, "missing inventory #{relative(path)}" unless File.file?(path)
          documents[[environment, component]] = validate_file!(path, environment: environment, component: component)
        end
        validate_argocd!(environment)
        validate_enabled_copies!(environment, documents)
      end
      validate_cross_environment!(documents)
      validate_platform_inventories!
      validate_production_managed_platform_inventories!
      validate_production_platform_contracts!
      true
    end

    def platform_component_names
      Dir[File.join(root, "environments/local-production/platform/*.yaml")].map { |path| File.basename(path, ".yaml") }.sort
    end

    def production_platform_component_names
      Dir[File.join(root, "environments/production/platform/*.yaml")].map { |path| File.basename(path, ".yaml") }.sort
    end

    def production_managed_platform_component_names
      Dir[File.join(root, "environments/production/platform-releases/*.yaml")].map { |path| File.basename(path, ".yaml") }.sort
    end

    def validate_production_platform_file!(path, component: nil)
      document = YAML.safe_load(File.read(path), [], [], true)
      raise ConfigurationError, "#{relative(path)} must contain a map" unless document.is_a?(Hash)
      required = %w[schemaVersion component environment mode managedInCluster configuration preflight]
      allowed = required + ["semantics"]
      unknown = document.keys - allowed
      missing = required - document.keys
      raise ConfigurationError, "#{relative(path)} missing fields: #{missing.join(', ')}" unless missing.empty?
      raise ConfigurationError, "#{relative(path)} unknown fields: #{unknown.join(', ')}" unless unknown.empty?
      assert(document["schemaVersion"] == 1, path, "schemaVersion must be 1")
      assert(document["component"] == component, path, "component does not match filename") if component
      assert(document["environment"] == "production", path, "environment must be production")
      assert(document["mode"] == "external", path, "mode must be external")
      assert(document["managedInCluster"] == false, path, "managedInCluster must be false")

      configuration = document["configuration"]
      assert(configuration.is_a?(Hash) && !configuration.empty?, path, "configuration must be a non-empty map")
      configuration.each do |name, reference|
        exact_map(reference, path, "configuration.#{name}", %w[kind name key])
        assert(%w[Secret ConfigMap].include?(reference["kind"]), path, "configuration.#{name}.kind is invalid")
        assert_string(reference["name"], path, "configuration.#{name}.name")
        assert_string(reference["key"], path, "configuration.#{name}.key")
      end

      preflight = document["preflight"]
      assert(preflight.is_a?(Array) && !preflight.empty?, path, "preflight must be a non-empty array")
      preflight.each_with_index do |check, index|
        exact_map(check, path, "preflight[#{index}]", %w[type expect])
        assert(PLATFORM_PREFLIGHT_TYPES.include?(check["type"]), path, "preflight[#{index}].type is invalid")
        assert_string(check["expect"], path, "preflight[#{index}].expect")
      end
      semantics = document.fetch("semantics", [])
      assert(semantics.is_a?(Array) && semantics.all? { |entry| entry.is_a?(String) && !entry.empty? }, path,
             "semantics must be an array of non-empty strings")
      document
    rescue Psych::Exception => e
      raise ConfigurationError, "cannot parse #{relative(path)}: #{e.message}"
    end

    def validate_platform_file!(path, component: nil, environment: "local-production")
      document = YAML.safe_load(File.read(path), [], [], true)
      raise ConfigurationError, "#{relative(path)} must contain a map" unless document.is_a?(Hash)
      required = %w[schemaVersion component environment enabled status application stage destinationNamespace chart values]
      unknown = document.keys - required
      missing = required - document.keys
      raise ConfigurationError, "#{relative(path)} missing fields: #{missing.join(', ')}" unless missing.empty?
      raise ConfigurationError, "#{relative(path)} unknown fields: #{unknown.join(', ')}" unless unknown.empty?
      assert(document["schemaVersion"] == 1, path, "schemaVersion must be 1")
      assert(document["component"] == component, path, "component does not match filename") if component
      assert(ENVIRONMENTS.include?(environment), path, "unknown platform release environment")
      assert(document["environment"] == environment, path, "platform release environment does not match directory")
      assert([true, false].include?(document["enabled"]), path, "enabled must be boolean")
      assert(%w[awaiting-immutable-package-publication released].include?(document["status"]), path, "invalid status")
      expected_application = "uno-arena-#{environment}-#{document['component']}"
      assert(document["application"] == expected_application, path, "application must be #{expected_application}")
      assert(document["stage"].is_a?(String) && document["stage"].match?(/\A[0-9]{2}-[a-z0-9-]+\z/), path, "invalid platform stage")
      assert(%w[uno-arena observability].include?(document["destinationNamespace"]), path, "unexpected platform namespace")

      chart_keys = %w[repository name version]
      chart_keys << "packageSha256" if document.fetch("chart", {}).key?("packageSha256")
      chart = exact_map(document["chart"], path, "chart", chart_keys)
      assert_https(chart["repository"], path, "chart.repository")
      expected_chart_name = document["component"] == "observability" ? "uno-arena-observability" : document["component"]
      assert(chart["name"] == expected_chart_name, path, "chart.name must be #{expected_chart_name}")
      assert(chart["version"].is_a?(String) && chart["version"].match?(SEMVER), path, "invalid chart.version")
      assert(document["values"].is_a?(Hash), path, "values must be a map")

      if document["component"] == "context-bootstrap"
        image = exact_map(document.dig("values", "image"), path, "values.image", %w[repository digest])
        assert_string(image["repository"], path, "values.image.repository")
        assert(image["digest"].is_a?(String) && image["digest"].match?(DIGEST), path, "invalid values.image.digest")
      end

      coordinates = YAML.dump(document)
      zero_digest = document.dig("values", "image", "digest") == "sha256:#{'0' * 64}"
      if document["status"] == "awaiting-immutable-package-publication"
        assert(document["enabled"] == false, path, "awaiting platform releases must be disabled")
        assert(chart["repository"].match?(PLACEHOLDER), path, "awaiting platform chart must use explicit placeholder coordinates")
      else
        assert(document["enabled"] == true, path, "released platform inventory must be enabled")
        assert(!coordinates.match?(PLACEHOLDER) && !coordinates.include?("replace-before-release"), path,
               "released platform inventory cannot use placeholder coordinates or configuration")
        assert(!zero_digest, path, "released platform inventory cannot use the zero digest")
        assert(chart["packageSha256"].to_s.match?(PACKAGE_SHA256), path,
               "released platform inventory requires chart.packageSha256")
      end
      document
    rescue Psych::Exception => e
      raise ConfigurationError, "cannot parse #{relative(path)}: #{e.message}"
    end

    def validate_file!(path, environment: nil, component: nil)
      document = YAML.safe_load(File.read(path), [], [], true)
      raise ConfigurationError, "#{relative(path)} must contain a map" unless document.is_a?(Hash)

      required = %w[schemaVersion component environment enabled status application project destination chart image values rollback]
      unknown = document.keys - required
      missing = required - document.keys
      raise ConfigurationError, "#{relative(path)} missing fields: #{missing.join(', ')}" unless missing.empty?
      raise ConfigurationError, "#{relative(path)} unknown fields: #{unknown.join(', ')}" unless unknown.empty?
      assert(document["schemaVersion"] == 1, path, "schemaVersion must be 1")
      assert(COMPONENTS.include?(document["component"]), path, "unknown component")
      assert(ENVIRONMENTS.include?(document["environment"]), path, "unknown environment")
      assert(document["component"] == component, path, "component does not match filename") if component
      assert(document["environment"] == environment, path, "environment does not match directory") if environment
      assert([true, false].include?(document["enabled"]), path, "enabled must be boolean")
      assert(%w[bootstrap-placeholder released].include?(document["status"]), path, "invalid status")
      assert_string(document["application"], path, "application")
      expected_application = "uno-arena-#{document['environment']}-#{document['component']}"
      assert(document["application"] == expected_application, path, "application must be #{expected_application}")
      assert(document["project"] == "uno-arena-workloads", path, "project must be uno-arena-workloads")

      destination = exact_map(document["destination"], path, "destination", %w[server namespace])
      assert(destination["server"] == "https://kubernetes.default.svc", path, "unexpected destination server")
      assert(destination["namespace"] == "uno-arena", path, "unexpected namespace")

      chart_keys = %w[repository name version]
      chart_keys << "packageSha256" if document.fetch("chart", {}).key?("packageSha256")
      chart = exact_map(document["chart"], path, "chart", chart_keys)
      assert_https(chart["repository"], path, "chart.repository")
      assert(chart["name"] == document["component"], path, "chart.name must equal component")
      assert(chart["version"].is_a?(String) && chart["version"].match?(SEMVER), path, "invalid chart.version")

      image = exact_map(document["image"], path, "image", %w[repository digest])
      assert_string(image["repository"], path, "image.repository")
      assert(image["digest"].is_a?(String) && image["digest"].match?(DIGEST), path, "invalid image.digest")

      values = exact_map(document["values"], path, "values", %w[repository revision files])
      assert_https(values["repository"], path, "values.repository")
      assert_string(values["revision"], path, "values.revision")
      expected_files = ["services/#{document['component']}/helm/#{document['component']}/values.production.yaml"]
      if document["environment"] == "local-production"
        expected_files << "services/#{document['component']}/helm/#{document['component']}/values.local-production.yaml"
      end
      assert(values["files"] == expected_files, path, "values.files must be exact ordered environment overlays")
      values["files"].each { |file| assert(File.file?(File.join(root, file)), path, "missing values file #{file}") }

      rollback = exact_map(document["rollback"], path, "rollback", %w[automatic])
      assert([true, false].include?(rollback["automatic"]), path, "rollback.automatic must be boolean")

      validate_lifecycle!(document, path)
      document
    rescue Psych::Exception => e
      raise ConfigurationError, "cannot parse #{relative(path)}: #{e.message}"
    end

    private

    def validate_lifecycle!(document, path)
      coordinates = [document.dig("chart", "repository"), document.dig("image", "repository"), document.dig("values", "repository")].join(" ")
      zero_digest = document.dig("image", "digest") == "sha256:#{'0' * 64}"
      if document["status"] == "bootstrap-placeholder"
        assert(document["enabled"] == false, path, "bootstrap placeholders must be disabled")
        assert(coordinates.match?(PLACEHOLDER), path, "bootstrap placeholder must use explicit placeholder coordinates")
      else
        assert(document["enabled"] == true, path, "released inventory must be enabled")
        assert(!coordinates.match?(PLACEHOLDER), path, "released inventory cannot use placeholder coordinates")
        assert(!zero_digest, path, "released inventory cannot use the zero digest")
        assert(document.dig("values", "revision").match?(COMMIT_SHA), path, "released values.revision must be an immutable commit SHA")
        assert(document.dig("chart", "packageSha256").to_s.match?(PACKAGE_SHA256), path,
               "released inventory requires chart.packageSha256")
      end
    end

    def validate_enabled_copies!(environment, documents)
      directory = File.join(root, "environments", environment, "services", "enabled")
      return unless Dir.exist?(directory)
      Dir[File.join(directory, "*.yaml")].each do |path|
        component = File.basename(path, ".yaml")
        assert(COMPONENTS.include?(component), path, "unknown enabled component")
        active = validate_file!(path, environment: environment, component: component)
        assert(active["enabled"] && active["status"] == "released", path, "enabled copies must be released")
        canonical = documents.fetch([environment, component])
        assert(active == canonical, path, "enabled copy must exactly match canonical inventory")
      end
    end

    def validate_argocd!(environment)
      names = %w[
        app-project.yaml bootstrap-project.yaml platform-app-project.yaml
        platform-applicationset.yaml services-applicationset.yaml
      ]
      names.each do |name|
        path = File.join(root, "environments", environment, "argocd", name)
        raise ConfigurationError, "missing Argo definition #{relative(path)}" unless File.file?(path)
        document = YAML.safe_load(File.read(path), [], [], true)
        assert(document.is_a?(Hash), path, "Argo definition must be a map")
        assert(document["apiVersion"] == "argoproj.io/v1alpha1", path, "unexpected Argo apiVersion")
        serialized = File.read(path)
        assert(!serialized.include?("group: '*'"), path, "wildcard namespace resource authority is forbidden")
        if name == "app-project.yaml"
          assert(!serialized.match?(/kind: (StatefulSet|PersistentVolumeClaim|Namespace|CustomResourceDefinition)/), path,
                 "workload project state-bearing or cluster-scoped authority is forbidden")
        end
        if name == "services-applicationset.yaml"
          expected = "environments/#{environment}/services/enabled/*.yaml"
          assert(serialized.include?(expected), path, "ApplicationSet must watch released-only inventories")
          expected_files = ["values.production.yaml"]
          expected_files << "values.local-production.yaml" if environment == "local-production"
          expected_files.each { |file| assert(serialized.include?(file), path, "ApplicationSet missing #{file}") }
        elsif name == "platform-applicationset.yaml"
          directory = environment == "production" ? "platform-releases" : "platform"
          assert(serialized.include?("environments/#{environment}/#{directory}/enabled/*.yaml"), path,
                 "platform ApplicationSet must watch released-only inventories")
          assert(serialized.include?("preserveResourcesOnDeletion: true"), path,
                 "platform ApplicationSet must preserve resources on deletion")
          if environment == "local-production"
            assert(!serialized.include?("automated:"), path, "stateful simulator platform must require deliberate sync")
          else
            assert(serialized.include?("automated:") && serialized.include?("prune: false") && serialized.include?("selfHeal: true"), path,
                   "production managed platform must self-heal without pruning")
          end
        end
      end
      chart = File.join(root, "environments", environment, "argocd", "Chart.yaml")
      root_template = File.join(root, "environments", environment, "argocd", "templates", "root-application.yaml")
      raise ConfigurationError, "missing Argo root chart #{relative(chart)}" unless File.file?(chart)
      raise ConfigurationError, "missing Argo root Application #{relative(root_template)}" unless File.file?(root_template)
    rescue Psych::Exception => e
      raise ConfigurationError, "cannot parse Argo definition: #{e.message}"
    end

    def validate_cross_environment!(documents)
      COMPONENTS.each do |component|
        production = documents.fetch(["production", component])
        local = documents.fetch(["local-production", component])
        assert(production.dig("chart", "name") == local.dig("chart", "name"), "inventories", "chart identity drift for #{component}")
        assert(production.dig("image", "repository").split("/").last == local.dig("image", "repository").split("/").last,
               "inventories", "image identity drift for #{component}")
      end
    end

    def validate_platform_inventories!
      inventory_names = platform_component_names
      chart_names = Dir[File.join(root, "infrastructure/local-production/charts/*/Chart.yaml")].map do |path|
        File.basename(File.dirname(path))
      end
      observability_chart = File.join(root, "infrastructure/observability/helm/uno-arena-observability/Chart.yaml")
      chart_names << "observability" if File.file?(observability_chart)
      chart_names.sort!
      assert(inventory_names == chart_names, "platform inventories", "platform chart/inventory catalogs differ")
      documents = inventory_names.to_h do |component|
        path = File.join(root, "environments/local-production/platform/#{component}.yaml")
        [component, validate_platform_file!(path, component: component)]
      end
      enabled_dir = File.join(root, "environments/local-production/platform/enabled")
      return unless Dir.exist?(enabled_dir)
      Dir[File.join(enabled_dir, "*.yaml")].each do |path|
        component = File.basename(path, ".yaml")
        assert(inventory_names.include?(component), path, "unknown enabled platform component")
        active = validate_platform_file!(path, component: component)
        assert(active["enabled"] && active["status"] == "released", path, "enabled platform copies must be released")
        assert(active == documents.fetch(component), path, "enabled platform copy must exactly match canonical inventory")
      end
    end

    def validate_production_managed_platform_inventories!
      schema_path = File.join(root, "environments/production/platform-releases/release.schema.json")
      raise ConfigurationError, "missing production managed platform schema" unless File.file?(schema_path)
      schema = JSON.parse(File.read(schema_path))
      assert(schema.dig("properties", "environment", "const") == "production", schema_path,
             "production managed platform environment schema guard missing")
      names = production_managed_platform_component_names
      assert(names == PRODUCTION_MANAGED_PLATFORM_COMPONENTS, "production managed platform inventories",
             "production managed platform catalog differs")
      documents = names.to_h do |component|
        path = File.join(root, "environments/production/platform-releases/#{component}.yaml")
        [component, validate_platform_file!(path, component: component, environment: "production")]
      end
      enabled_dir = File.join(root, "environments/production/platform-releases/enabled")
      return unless Dir.exist?(enabled_dir)
      Dir[File.join(enabled_dir, "*.yaml")].each do |path|
        component = File.basename(path, ".yaml")
        assert(names.include?(component), path, "unknown enabled production platform component")
        active = validate_platform_file!(path, component: component, environment: "production")
        assert(active["enabled"] && active["status"] == "released", path,
               "enabled production platform copies must be released")
        assert(active == documents.fetch(component), path,
               "enabled production platform copy must exactly match canonical inventory")
      end
    rescue JSON::ParserError => e
      raise ConfigurationError, "cannot parse production managed platform schema: #{e.message}"
    end

    def validate_production_platform_contracts!
      schema_path = File.join(root, "environments/production/platform/contract.schema.json")
      raise ConfigurationError, "missing production platform schema" unless File.file?(schema_path)
      schema = JSON.parse(File.read(schema_path))
      assert(schema.dig("properties", "environment", "const") == "production", schema_path, "environment schema guard missing")
      assert(schema.dig("properties", "mode", "const") == "external", schema_path, "external mode schema guard missing")
      assert(schema.dig("properties", "managedInCluster", "const") == false, schema_path,
             "managedInCluster schema guard missing")
      names = production_platform_component_names
      assert(names == PRODUCTION_PLATFORM_COMPONENTS, "production platform contracts", "production platform contract catalog differs")
      names.each do |component|
        path = File.join(root, "environments/production/platform/#{component}.yaml")
        validate_production_platform_file!(path, component: component)
      end
    rescue JSON::ParserError => e
      raise ConfigurationError, "cannot parse production platform schema: #{e.message}"
    end

    def exact_map(value, path, name, keys)
      assert(value.is_a?(Hash), path, "#{name} must be a map")
      assert((value.keys - keys).empty? && (keys - value.keys).empty?, path, "#{name} fields must be exactly #{keys.join(', ')}")
      value
    end

    def assert_https(value, path, name)
      uri = URI.parse(value.to_s)
      assert(uri.scheme == "https" && uri.host, path, "#{name} must be an HTTPS URL")
    rescue URI::InvalidURIError
      raise ConfigurationError, "#{relative(path)}: #{name} must be an HTTPS URL"
    end

    def assert_string(value, path, name)
      assert(value.is_a?(String) && !value.empty?, path, "#{name} must be a non-empty string")
    end

    def assert(condition, path, message)
      raise ConfigurationError, "#{relative(path)}: #{message}" unless condition
    end

    def relative(path)
      path.to_s.sub(%r{\A#{Regexp.escape(root)}/?}, "")
    end
  end
end
