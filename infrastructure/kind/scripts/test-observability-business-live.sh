#!/usr/bin/env bash
# Explicit kind acceptance for the three authoritative business counters.
# All mutations use the public Gateway through the checked-in CLI.
set -Eeuo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
# shellcheck source=lib.sh
source "${SCRIPT_DIR}/lib.sh"
# shellcheck source=observability-acceptance-lib.sh
source "${SCRIPT_DIR}/observability-acceptance-lib.sh"

require_cmd curl
require_cmd kubectl
require_cmd ruby
require_cmd python3
assert_kind_context
"${SCRIPT_DIR}/wait-observability.sh"

CLI="${ROOT}/client-checkpoint/bin/unoarena"
[[ -x "${CLI}" ]] || die "client CLI is not executable: ${CLI}"
RUN_DIR="$(mktemp -d "${TMPDIR:-/tmp}/uno-observability-business.XXXXXX")"
report_error() {
  local rc=$? line="$1"
  echo "error: business observability probe failed at line ${line} (exit ${rc})" >&2
  return "${rc}"
}
trap 'report_error ${LINENO}' ERR
cleanup() {
  local rc=$?
  local pid
  for pid in $(jobs -pr); do kill "${pid}" >/dev/null 2>&1 || true; done
  obs_cleanup_forwards
  rm -rf "${RUN_DIR}"
  return "${rc}"
}
trap cleanup EXIT INT TERM

PROM_PORT="${PROMETHEUS_LOCAL_PORT:-19090}"
GATEWAY_PORT="${GATEWAY_LOCAL_PORT:-18080}"
IDENTITY_METRICS_PORT="${IDENTITY_METRICS_LOCAL_PORT:-19091}"
ROOM_METRICS_PORT="${ROOM_METRICS_LOCAL_PORT:-19092}"
TOURNAMENT_METRICS_PORT="${TOURNAMENT_METRICS_LOCAL_PORT:-19093}"
obs_forward observability service/prometheus "${PROM_PORT}" 9090
obs_forward "${KIND_NAMESPACE}" service/gateway "${GATEWAY_PORT}" 8080
obs_forward "${KIND_NAMESPACE}" service/identity "${IDENTITY_METRICS_PORT}" 9090
obs_forward "${KIND_NAMESPACE}" service/room-gameplay "${ROOM_METRICS_PORT}" 9090
obs_forward "${KIND_NAMESPACE}" service/tournament-orchestration "${TOURNAMENT_METRICS_PORT}" 9090
PROM="http://127.0.0.1:${PROM_PORT}"
export UNOARENA_API_URL="http://127.0.0.1:${GATEWAY_PORT}"
bot_poll_interval="${OBS_BOT_POLL_INTERVAL_SECONDS:-5}"
bot_action_interval="${OBS_BOT_ACTION_INTERVAL_SECONDS:-2}"
obs_wait_http "${PROM}/-/ready"
obs_wait_http "${UNOARENA_API_URL}/ready"

obs_assert_counter_contract "http://127.0.0.1:${IDENTITY_METRICS_PORT}" \
  unoarena_players_created_total "Number of authoritative player creations committed by Identity."
obs_assert_counter_contract "http://127.0.0.1:${ROOM_METRICS_PORT}" \
  unoarena_games_completed_total "Number of authoritative game completion transitions committed by Room Gameplay."
obs_assert_counter_contract "http://127.0.0.1:${TOURNAMENT_METRICS_PORT}" \
  unoarena_tournaments_completed_total "Number of authoritative tournament completion transitions committed by Tournament Orchestration."

players_before="$(obs_wait_prom_present "${PROM}" 'sum(unoarena_players_created_total)' 45)"
players_increase_before="$(obs_prom_query "${PROM}" 'sum(increase(unoarena_players_created_total[30m]))')"
player_user="$(obs_unique obs-player | tr -d '-')"
player_pass="${player_user}-pass"
"${CLI}" register --user "${player_user}" --pass "${player_pass}" >"${RUN_DIR}/player-register.json"
players_after="$(obs_wait_prom_counter_progress "${PROM}" unoarena_players_created_total "${players_before}" "${players_increase_before}" 45)"
if "${CLI}" register --user "${player_user}" --pass "${player_pass}" >"${RUN_DIR}/player-reject.json" 2>&1; then
  die "duplicate player registration unexpectedly succeeded"
fi
obs_wait_prom_equal "${PROM}" 'sum(unoarena_players_created_total)' "${players_after}" 20

seed_prefix="obsg$(date +%s)$$"
"${CLI}" seed --count 2 --prefix "${seed_prefix}" >"${RUN_DIR}/game-seed.jsonl"
game_token_1="$(sed -n '1p' "${RUN_DIR}/game-seed.jsonl" | obs_json_field token)"
game_token_2="$(sed -n '2p' "${RUN_DIR}/game-seed.jsonl" | obs_json_field token)"
games_before="$(obs_wait_prom_present "${PROM}" 'sum(unoarena_games_completed_total)' 45)"
games_increase_before="$(obs_prom_query "${PROM}" 'sum(increase(unoarena_games_completed_total[30m]))')"
room_id="$(obs_unique obs-room | tr -d '-')"
room_payload="$(ruby -rjson -e 'puts JSON.generate({roomId:ARGV[0],maxSeats:2})' "${room_id}")"
"${CLI}" room create --token "${game_token_1}" --payload "${room_payload}" \
  --command-id "create-${room_id}" --correlation-id "corr-${room_id}" >"${RUN_DIR}/room-create.json"
# CreateRoom commits before its dedicated runtime is Ready. A known
# room_starting response proves the mutation was not admitted. The public
# snapshot can also race a runtime-owned transition; stale_sequence is an
# authoritative rejection, so resnapshot with a fresh command ID. Neither
# path replays an uncertain mutation.
room_joined=false
for attempt in $(seq 1 60); do
  if "${CLI}" room join "${room_id}" --token "${game_token_2}" \
      --command-id "join-${room_id}-${attempt}" --correlation-id "corr-join-${room_id}-${attempt}" \
      >"${RUN_DIR}/room-join.json" 2>"${RUN_DIR}/room-join.err"; then
    room_joined=true
    break
  fi
  if ! grep -Fq '"code":"room_starting"' "${RUN_DIR}/room-join.err" &&
      ! grep -Fq '"reason":"stale_sequence"' "${RUN_DIR}/room-join.err"; then
    cat "${RUN_DIR}/room-join.err" >&2
    die "JoinRoom failed without retryable room_starting or authoritative stale_sequence"
  fi
  sleep 1
done
[[ "${room_joined}" == true ]] || die "Room runtime did not become ready within 60 seconds"
if "${CLI}" command --token "${game_token_1}" --type PlayCard --room-id "${room_id}" \
    --command-id "stale-${room_id}" --expected-sequence 0 --payload '{"cardId":"not-a-card"}' \
    --correlation-id "corr-stale-${room_id}" >"${RUN_DIR}/game-reject.stdout" 2>"${RUN_DIR}/game-reject.err"; then
  die "stale gameplay command unexpectedly returned a successful HTTP status"
fi
grep -Fq 'returned HTTP 409' "${RUN_DIR}/game-reject.err" || \
  { cat "${RUN_DIR}/game-reject.err" >&2; die "stale gameplay command did not return HTTP 409"; }
tail -n 1 "${RUN_DIR}/game-reject.err" >"${RUN_DIR}/game-reject.json"
[[ "$(obs_json_field status <"${RUN_DIR}/game-reject.json")" == "rejected" ]] || \
  die "stale gameplay command did not return an authoritative rejection"
[[ "$(obs_json_field reason <"${RUN_DIR}/game-reject.json")" == "stale_sequence" ]] || \
  die "stale gameplay command did not return stale_sequence"
obs_wait_prom_equal "${PROM}" 'sum(unoarena_games_completed_total)' "${games_before}" 20
"${CLI}" bot --room "${room_id}" --token "${game_token_1}" --seed 101 \
  --poll-interval "${bot_poll_interval}" \
  --action-interval "${bot_action_interval}" \
  --max-wait "${OBS_GAME_TIMEOUT_SECONDS:-900}" >"${RUN_DIR}/game-bot-1.jsonl" 2>&1 &
game_bot_1=$!
"${CLI}" bot --room "${room_id}" --token "${game_token_2}" --seed 202 \
  --poll-interval "${bot_poll_interval}" \
  --action-interval "${bot_action_interval}" \
  --max-wait "${OBS_GAME_TIMEOUT_SECONDS:-900}" >"${RUN_DIR}/game-bot-2.jsonl" 2>&1 &
game_bot_2=$!
game_bot_failed=0
wait "${game_bot_1}" || game_bot_failed=1
wait "${game_bot_2}" || game_bot_failed=1
if (( game_bot_failed != 0 )); then
  tail -n 20 "${RUN_DIR}/game-bot-1.jsonl" >&2
  tail -n 20 "${RUN_DIR}/game-bot-2.jsonl" >&2
  die "casual bots did not complete successfully"
fi
games_after="$(obs_wait_prom_counter_progress "${PROM}" unoarena_games_completed_total "${games_before}" "${games_increase_before}" 45)"

# On the single-node kind profile, require every observability backend and
# collector to recover readiness after draining the completed game's logs,
# metrics, and traces, then retain the agreed settle window before starting the
# independent tournament flow. This is test sequencing only; it does not change
# any service timeout or production path.
"${SCRIPT_DIR}/wait-observability.sh"
sleep "${OBS_FLOW_SETTLE_SECONDS:-15}"

seed_prefix="obst$(date +%s)$$"
"${CLI}" seed --count 2 --prefix "${seed_prefix}" >"${RUN_DIR}/tournament-seed.jsonl"
tournament_token_1="$(sed -n '1p' "${RUN_DIR}/tournament-seed.jsonl" | obs_json_field token)"
tournament_token_2="$(sed -n '2p' "${RUN_DIR}/tournament-seed.jsonl" | obs_json_field token)"
tournament_id="$(obs_unique obs-tournament | tr -d '-')"
tournament_payload="$(ruby -rjson -e 'puts JSON.generate({tournamentId:ARGV[0],capacity:2})' "${tournament_id}")"
tournaments_before="$(obs_wait_prom_present "${PROM}" 'sum(unoarena_tournaments_completed_total)' 45)"
tournaments_increase_before="$(obs_prom_query "${PROM}" 'sum(increase(unoarena_tournaments_completed_total[30m]))')"
"${CLI}" tournament create --token "${tournament_token_1}" --payload "${tournament_payload}" \
  --command-id "create-${tournament_id}" --correlation-id "corr-${tournament_id}" >"${RUN_DIR}/tournament-create.json"
# Each bot owns its one public registration. Capacity two is the server-owned
# threshold that closes/seeds the local tournament; the client never submits
# internal lifecycle policy commands.
"${CLI}" bot --tournament "${tournament_id}" --token "${tournament_token_1}" --seed 303 \
  --poll-interval "${bot_poll_interval}" \
  --action-interval "${bot_action_interval}" \
  --max-wait "${OBS_TOURNAMENT_TIMEOUT_SECONDS:-900}" >"${RUN_DIR}/tournament-bot-1.jsonl" 2>&1 &
tournament_bot_1=$!
"${CLI}" bot --tournament "${tournament_id}" --token "${tournament_token_2}" --seed 404 \
  --poll-interval "${bot_poll_interval}" \
  --action-interval "${bot_action_interval}" \
  --max-wait "${OBS_TOURNAMENT_TIMEOUT_SECONDS:-900}" >"${RUN_DIR}/tournament-bot-2.jsonl" 2>&1 &
tournament_bot_2=$!
tournament_bot_failed=0
wait "${tournament_bot_1}" || tournament_bot_failed=1
wait "${tournament_bot_2}" || tournament_bot_failed=1
if (( tournament_bot_failed != 0 )); then
  tail -n 20 "${RUN_DIR}/tournament-bot-1.jsonl" >&2
  tail -n 20 "${RUN_DIR}/tournament-bot-2.jsonl" >&2
  die "tournament bots did not complete successfully"
fi
tournaments_after="$(obs_wait_prom_counter_progress "${PROM}" unoarena_tournaments_completed_total "${tournaments_before}" "${tournaments_increase_before}" 45)"
"${CLI}" tournament create --token "${tournament_token_1}" --payload "${tournament_payload}" \
  --command-id "create-${tournament_id}" --correlation-id "corr-${tournament_id}" >"${RUN_DIR}/tournament-create-replay.json"
obs_wait_prom_equal "${PROM}" 'sum(unoarena_tournaments_completed_total)' "${tournaments_after}" 20

for metric in unoarena_players_created_total unoarena_games_completed_total unoarena_tournaments_completed_total; do
  delta="$(obs_prom_query "${PROM}" "sum(increase(${metric}[30m]))")"
  obs_assert_greater "${delta}" 0 "bounded interval delta for ${metric}"
done

echo "ok kind-observability-business players=${players_before}->${players_after} games=${games_before}->${games_after} tournaments=${tournaments_before}->${tournaments_after} room=${room_id} tournament=${tournament_id}"
