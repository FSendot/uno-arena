#!/usr/bin/env bash

configure_component() {
  case "${COMPONENT:-}" in
    gateway)
      expected_test_module=services/gateway/src
      expected_dockerfile=services/gateway/Dockerfile
      expected_chart_path=services/gateway/helm/gateway
      expected_chart_name=gateway
      ;;
    identity)
      expected_test_module=services/identity/src
      expected_dockerfile=services/identity/Dockerfile
      expected_chart_path=services/identity/helm/identity
      expected_chart_name=identity
      ;;
    room-gameplay)
      expected_test_module=services/room-gameplay/src
      expected_dockerfile=services/room-gameplay/Dockerfile
      expected_chart_path=services/room-gameplay/helm/room-gameplay
      expected_chart_name=room-gameplay
      ;;
    game-integrity)
      expected_test_module=services/game-integrity/src
      expected_dockerfile=services/game-integrity/Dockerfile
      expected_chart_path=services/game-integrity/helm/game-integrity
      expected_chart_name=game-integrity
      ;;
    tournament-orchestration)
      expected_test_module=services/tournament-orchestration/src
      expected_dockerfile=services/tournament-orchestration/Dockerfile
      expected_chart_path=services/tournament-orchestration/helm/tournament-orchestration
      expected_chart_name=tournament-orchestration
      ;;
    ranking)
      expected_test_module=services/ranking/src
      expected_dockerfile=services/ranking/Dockerfile
      expected_chart_path=services/ranking/helm/ranking
      expected_chart_name=ranking
      ;;
    spectator-view)
      expected_test_module=services/spectator-view/src
      expected_dockerfile=services/spectator-view/Dockerfile
      expected_chart_path=services/spectator-view/helm/spectator-view
      expected_chart_name=spectator-view
      ;;
    analytics)
      expected_test_module=services/analytics/src
      expected_dockerfile=services/analytics/Dockerfile
      expected_chart_path=services/analytics/helm/analytics
      expected_chart_name=analytics
      ;;
    *)
      echo "unknown COMPONENT: ${COMPONENT:-<unset>}" >&2
      return 1
      ;;
  esac

  assert_component_value TEST_MODULE "$expected_test_module"
  assert_component_value DOCKERFILE "$expected_dockerfile"
  assert_component_value CHART_PATH "$expected_chart_path"
  assert_component_value CHART_NAME "$expected_chart_name"
}

assert_component_value() {
  local variable_name="$1"
  local expected="$2"
  local actual="${!variable_name:-}"
  if [[ -n "$actual" && "$actual" != "$expected" ]]; then
    echo "$variable_name for $COMPONENT must be $expected, got $actual" >&2
    return 1
  fi
  printf -v "$variable_name" '%s' "$expected"
  export "$variable_name"
}
