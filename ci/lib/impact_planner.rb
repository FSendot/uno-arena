# frozen_string_literal: true

require "json"
require_relative "impact_map"

module UnoArenaCI
  class ImpactPlanner
    SOURCES = %w[merge_request_event push web].freeze
    DEPLOY_ENVIRONMENTS = %w[production local-production].freeze

    def initialize(impact_map, source:, changed_paths: [], run_component: nil, run_service: nil, branch: nil,
                   deploy_environments: nil, production_preflight_executable: false)
      @map = impact_map
      @source = source
      @changed_paths = Array(changed_paths).map(&:to_s).reject(&:empty?).uniq.sort
      @run_component = normalize_selector(run_component)
      @run_service = normalize_selector(run_service)
      @branch = branch
      @deploy_environments = normalize_deploy_environments(deploy_environments)
      @production_preflight_executable = production_preflight_executable
    end

    def plan
      raise ImpactError, "unsupported pipeline source #{@source.inspect}" unless SOURCES.include?(@source)
      if production_promotion? && @changed_paths.any? { |path| path.match?(%r{\Aservices/[^/]+/migrations/}) }
        raise ImpactError,
              "production promotion is blocked for schema changes until a production migration ordering gate exists"
      end
      if production_promotion? && !@production_preflight_executable
        raise ImpactError,
              "production promotion is blocked until external platform contracts have executable CI preflight evidence"
      end

      actions = empty_actions
      platform_actions = empty_platform_actions
      repository_validation = true
      inventory_validation = false

      if @source == "web"
        selected = manual_selection
        selected.each do |component|
          if actions.key?(component)
            actions[component].merge!("test" => true, "image" => true, "chart" => true)
          else
            config = @map.platform_component(component)
            platform_actions[component].merge!("image" => config["imageInputs"].any?, "chart" => true)
          end
        end
      else
        raise ImpactError, "no changed paths were supplied" if @changed_paths.empty?
        @changed_paths.each do |path|
          owner = @map.owner_for(path)
          apply_path_inputs(actions, path)
          apply_platform_path_inputs(platform_actions, path)
          owner_requires_inventory = apply_owner_effect(actions, platform_actions, owner, path)
          inventory_validation = inventory_validation || owner_requires_inventory
          inventory_validation = true if platform_actions.any? { |_name, action| action["image"] || action["chart"] }
        end
      end

      publish_eligible = %w[push web].include?(@source) && @branch == "main"
      actions.each_value do |action|
        action["publishImage"] = publish_eligible && action["image"]
        action["publishChart"] = publish_eligible && action["chart"]
        action["promote"] = action["publishImage"] || action["publishChart"]
      end
      platform_actions.each do |name, action|
        supported_delivery = @map.platform_component(name).fetch("environments") & @deploy_environments
        action["publishImage"] = publish_eligible && !supported_delivery.empty? && action["image"]
        action["publishChart"] = publish_eligible && !supported_delivery.empty? && action["chart"]
        action["promote"] = action["publishImage"] || action["publishChart"]
      end
      if publish_eligible
        seeds = actions.select { |_name, action| action["promote"] || action["reconcile"] }.merge(
          platform_actions.select { |_name, action| action["promote"] || action["reconcile"] }
        )
        seeds.each do |seed, action|
          environments = if action["promote"] && platform_actions.key?(seed)
                           @map.platform_component(seed).fetch("environments") & @deploy_environments
                         elsif action["promote"]
                           @deploy_environments
                         else
                           action["reconcileEnvironments"]
                         end
          @map.transitive_dependents([seed]).each do |node|
            mark_reconcile(actions[node], environments) if actions.key?(node)
            if platform_actions.key?(node)
              supported = @map.platform_component(node).fetch("environments") & environments
              mark_reconcile(platform_actions[node], supported)
            end
          end
        end
      end

      {
        "schemaVersion" => 1,
        "source" => @source,
        "branch" => @branch,
        "deployEnvironments" => @deploy_environments,
        "changedPaths" => @changed_paths,
        "repositoryValidation" => repository_validation,
        "inventoryValidation" => inventory_validation,
        "components" => actions,
        "platformComponents" => platform_actions,
        "controlPlaneApplications" => control_plane_applications(publish_eligible)
      }
    end

    private

    def production_promotion?
      %w[push web].include?(@source) && @branch == "main" && @deploy_environments.include?("production")
    end

    def control_plane_applications(publish_eligible)
      applications = {"production" => [], "local-production" => []}
      return applications unless publish_eligible

      local_control_change = @changed_paths.any? do |path|
        path.start_with?("environments/local-production/argocd/") ||
          path.start_with?("infrastructure/local-production/gitops/")
      end
      if local_control_change && @deploy_environments.include?("local-production")
        applications["local-production"] = %w[
          uno-arena-local-production-foundations
          uno-arena-local-production-root
        ]
      end
      applications
    end

    def empty_actions
      @map.component_names.each_with_object({}) do |name, memo|
        memo[name] = {
          "test" => false,
          "image" => false,
          "chart" => false,
          "publishImage" => false,
          "publishChart" => false,
          "promote" => false,
          "reconcile" => false,
          "reconcileEnvironments" => []
        }
      end
    end

    def empty_platform_actions
      @map.platform_component_names.each_with_object({}) do |name, memo|
        memo[name] = {
          "image" => false,
          "chart" => false,
          "publishImage" => false,
          "publishChart" => false,
          "promote" => false,
          "reconcile" => false,
          "reconcileEnvironments" => []
        }
      end
    end

    def normalize_selector(value)
      value = value.to_s.strip
      value.empty? ? nil : value
    end

    def normalize_deploy_environments(value)
      entries = if value.is_a?(String)
                  value.split(",")
                else
                  Array(value)
                end
      entries = entries.map { |entry| entry.to_s.strip }.reject(&:empty?)
      raise ImpactError, "DEPLOY_ENVIRONMENTS must select production and/or local-production" if entries.empty?
      unknown = entries - DEPLOY_ENVIRONMENTS
      raise ImpactError, "invalid DEPLOY_ENVIRONMENTS entries: #{unknown.join(', ')}" unless unknown.empty?
      raise ImpactError, "DEPLOY_ENVIRONMENTS entries must be unique" unless entries.uniq.length == entries.length
      DEPLOY_ENVIRONMENTS.select { |environment| entries.include?(environment) }
    end

    def manual_selection
      if @run_component && @run_service && @run_component != @run_service
        raise ImpactError, "RUN_COMPONENT and RUN_SERVICE disagree"
      end
      selector = @run_component || @run_service
      raise ImpactError, "manual web pipelines require RUN_COMPONENT=<component>|all" unless selector
      return @map.release_names if selector == "all"
      raise ImpactError, "unknown manual component #{selector.inspect}" unless @map.release_names.include?(selector)
      [selector]
    end

    def apply_path_inputs(actions, path)
      @map.components.each do |name, config|
        actions[name]["test"] = true if @map.matches_any?(config["testInputs"], path)
        actions[name]["image"] = true if @map.matches_any?(config["imageInputs"], path)
        actions[name]["chart"] = true if @map.matches_any?(config["chartInputs"], path)
      end
    end

    def apply_platform_path_inputs(actions, path)
      @map.platform_components.each do |name, config|
        actions[name]["image"] = true if @map.matches_any?(config["imageInputs"], path)
        actions[name]["chart"] = true if @map.matches_any?(config["chartInputs"], path)
      end
    end

    def apply_owner_effect(actions, platform_actions, owner, path)
      case owner[:type]
      when "shared"
        config = @map.shared_inputs.fetch(owner[:name])
        Array(config["testConsumers"]).each { |name| actions[name]["test"] = true }
        Array(config["imageConsumers"]).each { |name| actions[name]["image"] = true }
        false
      when "contract"
        config = @map.contracts.fetch(owner[:name])
        (Array(config["producers"]) | Array(config["consumers"])).each { |name| actions[name]["test"] = true }
        false
      when "repository"
        apply_repository_effect(actions, platform_actions, @map.repository_inputs.fetch(owner[:name])["effect"], path)
      when "inventory"
        mark_reconcile(actions.fetch(owner[:name]), [environment_for(path)])
        true
      when "platform_inventory"
        environment = environment_for(path)
        supported = @map.platform_component(owner[:name]).fetch("environments")
        raise ImpactError, "#{owner[:name]} is not managed in #{environment}" unless supported.include?(environment)
        mark_reconcile(platform_actions.fetch(owner[:name]), [environment])
        true
      when "platform_component"
        false
      when "platform"
        @map.transitive_dependents(owner[:name]).each do |node|
          mark_reconcile(actions[node], [environment_for(path)]) if actions.key?(node)
          if platform_actions.key?(node)
            environment = environment_for(path)
            supported = @map.platform_component(node).fetch("environments")
            mark_reconcile(platform_actions[node], [environment]) if supported.include?(environment)
          end
        end
        true
      else
        false
      end
    end

    def apply_repository_effect(actions, platform_actions, effect, path)
      case effect
      when "test-all"
        actions.each_value { |action| action["test"] = true }
        false
      when "image-all"
        actions.each_value { |action| action["image"] = true }
        false
      when "inventory-all"
        scoped_environment = path.match(%r{\Aenvironments/(production|local-production)/})&.[](1)
        environments = scoped_environment ? [scoped_environment] : @deploy_environments
        actions.each_value { |action| mark_reconcile(action, environments) }
        platform_actions.each do |name, action|
          mark_reconcile(action, @map.platform_component(name).fetch("environments") & environments)
        end
        true
      else
        false
      end
    end

    def mark_reconcile(action, environments)
      selected = Array(environments) & @deploy_environments
      return if selected.empty?
      action["reconcile"] = true
      action["reconcileEnvironments"] = (action["reconcileEnvironments"] | selected).sort_by { |environment| DEPLOY_ENVIRONMENTS.index(environment) }
    end

    def environment_for(path)
      match = path.match(%r{\Aenvironments/(production|local-production)/})
      raise ImpactError, "cannot determine deployment environment for #{path}" unless match
      match[1]
    end
  end
end
