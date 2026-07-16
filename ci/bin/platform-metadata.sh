#!/usr/bin/env bash

configure_platform_component() {
  : "${COMPONENT:?COMPONENT is required}"
  : "${CHART_PATH:?CHART_PATH is required}"
  : "${CHART_NAME:?CHART_NAME is required}"

  [[ "$COMPONENT" =~ ^[a-z0-9]+([.-][a-z0-9]+)*$ ]] || { echo "invalid platform COMPONENT: $COMPONENT" >&2; return 1; }
  local_contract=false
  observability_contract=false
  [[ "$CHART_PATH" == "infrastructure/local-production/charts/$COMPONENT" && "$CHART_NAME" == "$COMPONENT" ]] && local_contract=true
  [[ "$COMPONENT" == "observability" && "$CHART_PATH" == "infrastructure/observability/helm/uno-arena-observability" && "$CHART_NAME" == "uno-arena-observability" ]] && observability_contract=true
  [[ "$local_contract" == true || "$observability_contract" == true ]] || {
    echo "unsafe platform CHART_PATH for $COMPONENT: $CHART_PATH" >&2
    return 1
  }
  [[ -f "$CHART_PATH/Chart.yaml" ]] || { echo "missing platform chart metadata: $CHART_PATH/Chart.yaml" >&2; return 1; }
  ruby -ryaml -e 'd=YAML.safe_load(File.read(ARGV[0])); abort "chart name mismatch" unless d.is_a?(Hash) && d["name"] == ENV.fetch("CHART_NAME")' "$CHART_PATH/Chart.yaml"
  if [[ -n "${CHART_VALUES:-}" ]]; then
    [[ "$CHART_VALUES" =~ ^values[.a-z0-9-]*\.yaml$ && -f "$CHART_PATH/$CHART_VALUES" ]] || {
      echo "unsafe or missing platform CHART_VALUES: $CHART_VALUES" >&2
      return 1
    }
  fi

  if [[ -n "${DOCKERFILE:-}" ]]; then
    [[ "$COMPONENT" == "context-bootstrap" && "$DOCKERFILE" == "infrastructure/bootstrap/Dockerfile" && "${IMAGE_NAME:-}" == "bootstrap" ]] || {
      echo "unsupported platform image contract for $COMPONENT" >&2
      return 1
    }
    [[ -f "$DOCKERFILE" ]] || { echo "missing platform Dockerfile: $DOCKERFILE" >&2; return 1; }
  fi
}

configure_release_component() {
  case "${COMPONENT_KIND:-service}" in
    service)
      source "$(dirname "${BASH_SOURCE[0]}")/component-metadata.sh"
      configure_component || return
      IMAGE_NAME="${IMAGE_NAME:-$COMPONENT}"
      ;;
    platform)
      configure_platform_component || return
      IMAGE_NAME="${IMAGE_NAME:-$COMPONENT}"
      ;;
    *) echo "unknown COMPONENT_KIND: ${COMPONENT_KIND:-}" >&2; return 1 ;;
  esac
  export IMAGE_NAME
}
