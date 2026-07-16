# frozen_string_literal: true

require "yaml"

module UnoArenaCI
  class ConfigurationError < StandardError; end
  class ImpactError < StandardError; end

  class ImpactMap
    attr_reader :components, :platform_components, :platform_nodes, :shared_inputs, :contracts, :repository_inputs

    def self.load(path)
      raw = YAML.safe_load(File.read(path), [], [], true)
      new(raw, path)
    rescue Psych::Exception => e
      raise ConfigurationError, "cannot parse #{path}: #{e.message}"
    end

    def initialize(raw, source = "impact map")
      @raw = raw || {}
      @source = source
      @root = File.file?(source.to_s) ? File.expand_path("..", File.dirname(source.to_s)) : nil
      @components = fetch_hash("components")
      @platform_components = fetch_hash("platformComponents")
      @platform_nodes = fetch_hash("platformNodes")
      @shared_inputs = fetch_hash("sharedInputs")
      @contracts = fetch_hash("contracts")
      @repository_inputs = fetch_hash("repositoryInputs")
      @inventory_patterns = Array(@raw["inventoryPatterns"])
      @platform_inventory_patterns = Array(@raw["platformInventoryPatterns"])
      validate!
      @owners = build_owners
    end

    def component_names
      components.keys.sort
    end

    def component(name)
      components.fetch(name)
    rescue KeyError
      raise ImpactError, "unknown component #{name.inspect}"
    end

    def platform_component_names
      return platform_components.keys.sort unless @root
      platform_components.keys.select do |name|
        config = platform_components.fetch(name)
        chart = File.file?(File.join(@root, config["chartPath"], "Chart.yaml"))
        inventory = File.file?(File.join(@root, "environments/local-production/platform/#{name}.yaml"))
        raise ConfigurationError, "platform component #{name} must have both chart and inventory" if chart != inventory
        chart && inventory
      end.sort
    end

    def platform_component(name)
      platform_components.fetch(name)
    rescue KeyError
      raise ImpactError, "unknown platform component #{name.inspect}"
    end

    def release_names
      component_names | platform_component_names
    end

    def deployment_nodes
      components.keys | platform_components.keys | platform_nodes.keys
    end

    def transitive_dependents(names)
      selected = Array(names).dup
      loop do
        added = deployment_nodes.select do |candidate|
          !(selected.include?(candidate)) && (Array(node(candidate)["dependsOn"]) & selected).any?
        end
        break if added.empty?
        selected.concat(added)
      end
      selected.uniq
    end

    def owner_for(path)
      matches = @owners.select { |owner| owner[:patterns].any? { |pattern| glob_match?(pattern, path) } }
      raise ImpactError, "unowned changed path: #{path}" if matches.empty?
      if matches.length > 1
        names = matches.map { |owner| "#{owner[:type]}:#{owner[:name]}" }.join(", ")
        raise ImpactError, "ambiguous ownership for #{path}: #{names}"
      end
      matches.first
    end

    def matches_any?(patterns, path)
      Array(patterns).any? { |pattern| glob_match?(pattern, path) }
    end

    def glob_match?(pattern, path)
      expression = Regexp.escape(pattern.to_s)
      expression = expression.gsub("\\*\\*", ".*")
      expression = expression.gsub("\\*", "[^/]*")
      expression = expression.gsub("\\?", "[^/]")
      !!(path.to_s =~ /\A#{expression}\z/)
    end

    private

    def fetch_hash(key)
      value = @raw[key]
      raise ConfigurationError, "#{@source}: #{key} must be a map" unless value.is_a?(Hash)
      value
    end

    def validate!
      raise ConfigurationError, "#{@source}: schemaVersion must be 1" unless @raw["schemaVersion"] == 1
      raise ConfigurationError, "#{@source}: components must not be empty" if components.empty?
      duplicates = components.keys & platform_components.keys
      raise ConfigurationError, "#{@source}: service/platform component names overlap: #{duplicates.join(', ')}" unless duplicates.empty?

      components.each do |name, config|
        require_map(name, config)
        %w[owns testInputs imageInputs chartInputs].each { |key| require_string_array("components.#{name}.#{key}", config[key]) }
        %w[testModule dockerfile chartPath chartName].each do |key|
          value = config[key]
          raise ConfigurationError, "components.#{name}.#{key} must be a non-empty string" unless value.is_a?(String) && !value.empty?
        end
        validate_node_refs("components.#{name}.dependsOn", config["dependsOn"])
      end


      platform_components.each do |name, config|
        require_map(name, config)
        %w[owns chartInputs imageInputs].each { |key| require_string_array("platformComponents.#{name}.#{key}", config[key]) }
        require_string_array("platformComponents.#{name}.environments", config["environments"])
        unknown_environments = config["environments"] - %w[production local-production]
        unless unknown_environments.empty? && config["environments"].uniq.length == config["environments"].length
          raise ConfigurationError, "platformComponents.#{name}.environments must contain unique production/local-production entries"
        end
        %w[chartPath chartName].each do |key|
          value = config[key]
          raise ConfigurationError, "platformComponents.#{name}.#{key} must be a non-empty string" unless value.is_a?(String) && !value.empty?
        end
        local_contract = config["chartPath"] == "infrastructure/local-production/charts/#{name}" && config["chartName"] == name
        observability_contract = name == "observability" && config["chartPath"] == "infrastructure/observability/helm/uno-arena-observability" && config["chartName"] == "uno-arena-observability"
        unless (local_contract || observability_contract) && !config["chartPath"].include?("..")
          raise ConfigurationError, "platformComponents.#{name}.chartPath is outside the platform chart roots"
        end
        if config["imageInputs"].any?
          %w[dockerfile imageName].each do |key|
            value = config[key]
            raise ConfigurationError, "platformComponents.#{name}.#{key} must be a non-empty string" unless value.is_a?(String) && !value.empty?
          end
        elsif config.key?("dockerfile") || config.key?("imageName")
          raise ConfigurationError, "platformComponents.#{name} image metadata requires imageInputs"
        end
        if config.key?("chartValues") && (!config["chartValues"].is_a?(String) || !config["chartValues"].match?(/\Avalues[.a-z0-9-]*\.yaml\z/))
          raise ConfigurationError, "platformComponents.#{name}.chartValues must be a chart-local values YAML filename"
        end
        validate_node_refs("platformComponents.#{name}.dependsOn", config["dependsOn"])
      end


      platform_nodes.each do |name, config|
        require_map(name, config)
        require_string_array("platformNodes.#{name}.owns", config["owns"])
        validate_node_refs("platformNodes.#{name}.dependsOn", config["dependsOn"])
      end
      validate_acyclic_graph!

      shared_inputs.each do |name, config|
        require_map(name, config)
        require_string_array("sharedInputs.#{name}.owns", config["owns"])
        validate_component_refs("sharedInputs.#{name}.testConsumers", config["testConsumers"])
        validate_component_refs("sharedInputs.#{name}.imageConsumers", config["imageConsumers"])
      end

      contracts.each do |name, config|
        require_map(name, config)
        require_string_array("contracts.#{name}.owns", config["owns"])
        validate_component_refs("contracts.#{name}.producers", config["producers"])
        validate_component_refs("contracts.#{name}.consumers", config["consumers"])
      end

      repository_inputs.each do |name, config|
        require_map(name, config)
        require_string_array("repositoryInputs.#{name}.owns", config["owns"])
        unless %w[validate test-all image-all inventory-all].include?(config["effect"])
          raise ConfigurationError, "repositoryInputs.#{name}.effect is invalid"
        end
      end

      require_string_array("inventoryPatterns", @inventory_patterns)
      @inventory_patterns.each do |pattern|
        unless pattern.scan("%{component}").length == 1
          raise ConfigurationError, "inventory pattern must contain exactly one %{component}: #{pattern}"
        end
      end
      require_string_array("platformInventoryPatterns", @platform_inventory_patterns)
      @platform_inventory_patterns.each do |pattern|
        unless pattern.scan("%{component}").length == 1
          raise ConfigurationError, "platform inventory pattern must contain exactly one %{component}: #{pattern}"
        end
      end
    end

    def require_map(name, value)
      raise ConfigurationError, "#{name} must be a map" unless value.is_a?(Hash)
    end

    def require_string_array(name, value)
      unless value.is_a?(Array) && value.all? { |entry| entry.is_a?(String) && !entry.empty? }
        raise ConfigurationError, "#{name} must be an array of non-empty strings"
      end
    end

    def validate_component_refs(name, refs)
      require_string_array(name, refs)
      unknown = refs - components.keys
      raise ConfigurationError, "#{name} references unknown components: #{unknown.join(', ')}" unless unknown.empty?
    end

    def build_owners
      owners = []
      components.each { |name, config| owners << owner("component", name, config["owns"]) }
      platform_components.each { |name, config| owners << owner("platform_component", name, config["owns"]) }
      shared_inputs.each { |name, config| owners << owner("shared", name, config["owns"]) }
      contracts.each { |name, config| owners << owner("contract", name, config["owns"]) }
      repository_inputs.each { |name, config| owners << owner("repository", name, config["owns"]) }
      platform_nodes.each { |name, config| owners << owner("platform", name, config["owns"]) }
      @inventory_patterns.each do |pattern|
        components.each_key do |component_name|
          owners << owner("inventory", component_name, [format(pattern, component: component_name)])
        end
      end
      @platform_inventory_patterns.each do |pattern|
        platform_components.each_key do |component_name|
          owners << owner("platform_inventory", component_name, [format(pattern, component: component_name)])
        end
      end
      owners
    end


    def node(name)
      components[name] || platform_components[name] || platform_nodes[name] || raise(ConfigurationError, "unknown deployment node #{name}")
    end

    def validate_node_refs(name, refs)
      require_string_array(name, refs)
      unknown = refs - deployment_nodes
      raise ConfigurationError, "#{name} references unknown deployment nodes: #{unknown.join(', ')}" unless unknown.empty?
    end

    def validate_acyclic_graph!
      visiting = {}
      visited = {}
      visit = lambda do |name|
        raise ConfigurationError, "deployment dependency cycle includes #{name}" if visiting[name]
        return if visited[name]
        visiting[name] = true
        Array(node(name)["dependsOn"]).each { |dependency| visit.call(dependency) }
        visiting.delete(name)
        visited[name] = true
      end
      deployment_nodes.each { |name| visit.call(name) }
    end

    def owner(type, name, patterns)
      { type: type, name: name, patterns: patterns }
    end
  end
end
