# frozen_string_literal: true

require "yaml"

module UnoArenaCI
  class PipelineRenderer
    STAGES = %w[validate test build publish promote reconcile verify rollback].freeze

    def initialize(impact_map, impact)
      @map = impact_map
      @impact = impact
    end

    def render
      document = {
        "include" => [{ "local" => "ci/templates/impact-child.gitlab-ci.yml" }],
        "stages" => STAGES,
        "validate:repository" => { "extends" => ".validate-repository" }
      }
      if @impact["inventoryValidation"]
        document["validate:inventories"] = { "extends" => ".validate-inventories" }
      end

      @map.component_names.each do |name|
        config = @map.component(name)
        action = @impact.fetch("components").fetch(name)
        add_test(document, name, config) if action["test"]
        add_image_build(document, name, config, action) if action["image"] && !action["publishImage"]
        add_chart_lint(document, name, config, action) if action["chart"]
        add_image_publish(document, name, config, action) if action["publishImage"]
        add_chart_publish(document, name, config) if action["publishChart"]
      end

      @map.platform_component_names.each do |name|
        config = @map.platform_component(name)
        action = @impact.fetch("platformComponents").fetch(name)
        add_platform_image_build(document, name, config) if action["image"] && !action["publishImage"]
        add_platform_chart_lint(document, name, config) if action["chart"]
        add_platform_image_publish(document, name, config) if action["publishImage"]
        add_platform_chart_publish(document, name, config) if action["publishChart"]
      end

      add_delivery(document)

      YAML.dump(document)
    end

    private

    def variables(name, config)
      {
        "COMPONENT" => name,
        "TEST_MODULE" => config["testModule"],
        "DOCKERFILE" => config["dockerfile"],
        "CHART_PATH" => config["chartPath"],
        "CHART_NAME" => config["chartName"]
      }
    end

    def platform_variables(name, config)
      {
        "COMPONENT_KIND" => "platform",
        "COMPONENT" => name,
        "DOCKERFILE" => config.fetch("dockerfile", ""),
        "IMAGE_NAME" => config.fetch("imageName", name),
        "CHART_PATH" => config["chartPath"],
        "CHART_NAME" => config["chartName"],
        "CHART_VALUES" => config.fetch("chartValues", "")
      }
    end

    def add_test(document, name, config)
      document["test:#{name}"] = {
        "extends" => ".test-component",
        "variables" => variables(name, config)
      }
    end

    def add_image_build(document, name, config, action)
      job = {
        "extends" => ".build-component-image",
        "variables" => variables(name, config)
      }
      job["needs"] = ["test:#{name}"] if action["test"]
      document["build:image:#{name}"] = job
    end

    def add_chart_lint(document, name, config, action)
      job = {
        "extends" => ".lint-component-chart",
        "variables" => variables(name, config)
      }
      job["needs"] = ["test:#{name}"] if action["test"]
      document["lint:chart:#{name}"] = job
    end

    def add_image_publish(document, name, config, action)
      job = {
        "extends" => ".publish-component-image",
        "variables" => variables(name, config)
      }
      job["needs"] = ["test:#{name}"] if action["test"]
      document["publish:image:#{name}"] = job
    end

    def add_chart_publish(document, name, config)
      document["publish:chart:#{name}"] = {
        "extends" => ".publish-component-chart",
        "needs" => ["lint:chart:#{name}"],
        "variables" => variables(name, config)
      }
    end

    def add_platform_image_build(document, name, config)
      document["build:platform-image:#{name}"] = {
        "extends" => ".build-component-image",
        "variables" => platform_variables(name, config)
      }
    end

    def add_platform_chart_lint(document, name, config)
      document["lint:platform-chart:#{name}"] = {
        "extends" => ".lint-component-chart",
        "variables" => platform_variables(name, config)
      }
    end

    def add_platform_image_publish(document, name, config)
      document["publish:platform-image:#{name}"] = {
        "extends" => ".publish-component-image",
        "variables" => platform_variables(name, config)
      }
    end

    def add_platform_chart_publish(document, name, config)
      document["publish:platform-chart:#{name}"] = {
        "extends" => ".publish-component-chart",
        "needs" => ["lint:platform-chart:#{name}"],
        "variables" => platform_variables(name, config)
      }
    end

    def add_delivery(document)
      selected_environments = @impact.fetch("deployEnvironments")
      promotions = @impact.fetch("components").select { |_name, action| action["promote"] }.keys.sort
      platform_promotions = @impact.fetch("platformComponents").select { |_name, action| action["promote"] }.keys.sort

      applications = %w[production local-production].to_h do |environment|
        names = @impact.fetch("components").each_with_object([]) do |(component, action), out|
          next unless action.fetch("reconcileEnvironments", []).include?(environment)
          next unless promotions.include?(component) || released_inventory?(environment, component)
          out << "uno-arena-#{environment}-#{component}"
        end
        [environment, names.sort]
      end
      selected_environments.each do |environment|
        platform_names = @impact.fetch("platformComponents").each_with_object([]) do |(component, action), out|
          next unless action.fetch("reconcileEnvironments", []).include?(environment)
          next unless platform_promotions.include?(component) || released_platform_inventory?(environment, component)
          out << "uno-arena-#{environment}-#{component}"
        end
        applications[environment].concat(platform_names.sort)
        applications[environment].concat(
          Array(@impact.fetch("controlPlaneApplications", {}).fetch(environment, []))
        )
        applications[environment].uniq!
        applications[environment].sort!
      end
      # A disabled bootstrap-placeholder change validates inventory but must not
      # materialize or wait for an Argo CD Application.
      return if applications.values.all?(&:empty?)

      delivery_environments = applications.select { |_environment, names| !names.empty? }.keys
      delivery_variables = {
        "DEPLOY_ENVIRONMENTS" => delivery_environments.join(","),
        "ARGOCD_APPLICATIONS_PRODUCTION" => applications.fetch("production").join(","),
        "ARGOCD_APPLICATIONS_LOCAL_PRODUCTION" => applications.fetch("local-production").join(",")
      }

      publish_jobs = promotions.flat_map do |name|
        action = @impact.fetch("components").fetch(name)
        jobs = []
        jobs << "publish:image:#{name}" if action["publishImage"]
        jobs << "publish:chart:#{name}" if action["publishChart"]
        jobs
      end
      publish_jobs.concat(platform_promotions.flat_map do |name|
        action = @impact.fetch("platformComponents").fetch(name)
        jobs = []
        jobs << "publish:platform-image:#{name}" if action["publishImage"]
        jobs << "publish:platform-chart:#{name}" if action["publishChart"]
        jobs
      end)
      unless promotions.empty? && platform_promotions.empty?
        document["promote:desired-state"] = {
          "extends" => ".promote-releases",
          "needs" => publish_jobs.map { |job| { "job" => job, "artifacts" => true } },
          "variables" => {
            "PROMOTE_COMPONENTS" => promotions.join(","),
            "PROMOTE_PLATFORM_COMPONENTS" => platform_promotions.join(",")
          }.merge(delivery_variables)
        }
      end

      wait_needs = if promotions.empty? && platform_promotions.empty?
                     []
                   else
                     [{ "job" => "promote:desired-state", "artifacts" => true }]
                   end
      document["reconcile:wait"] = {
        "extends" => ".wait-for-reconciliation",
        "needs" => wait_needs,
        "variables" => delivery_variables
      }
      document["verify:post-deploy"] = {
        "extends" => ".post-deploy-evidence",
        "needs" => ["reconcile:wait"],
        "variables" => delivery_variables
      }
      return if promotions.empty?

      rollback_applications = applications.transform_values do |names|
        names.select { |application| promotions.any? { |component| application.end_with?("-#{component}") } }
      end
      document["rollback:stateless"] = {
        "extends" => ".rollback-releases",
        "dependencies" => ["promote:desired-state"],
        "variables" => {
          "PROMOTE_COMPONENTS" => promotions.join(","),
          "DEPLOY_ENVIRONMENTS" => delivery_environments.join(","),
          "ARGOCD_APPLICATIONS_PRODUCTION" => rollback_applications.fetch("production").join(","),
          "ARGOCD_APPLICATIONS_LOCAL_PRODUCTION" => rollback_applications.fetch("local-production").join(",")
        }
      }
    end

    def released_inventory?(environment, component)
      path = File.join("environments", environment, "services", "#{component}.yaml")
      document = YAML.safe_load(File.read(path), [], [], true)
      document["enabled"] == true && document["status"] == "released"
    rescue Errno::ENOENT, Psych::Exception
      false
    end

    def released_platform_inventory?(environment, component)
      directory = environment == "production" ? "platform-releases" : "platform"
      path = File.join("environments", environment, directory, "#{component}.yaml")
      document = YAML.safe_load(File.read(path), [], [], true)
      document["enabled"] == true && document["status"] == "released"
    rescue Errno::ENOENT, Psych::Exception
      false
    end
  end
end
