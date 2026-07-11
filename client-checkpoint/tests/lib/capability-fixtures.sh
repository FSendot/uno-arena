# Payload builders for capability-stack scenarios (python3 JSON only).

capability_payload_create_room() {
  local room_id="$1"
  local max_seats="${2:-2}"
  python3 -c 'import json,sys; print(json.dumps({"roomId":sys.argv[1],"visibility":"public","maxSeats":int(sys.argv[2])},separators=(",",":")))' \
    "$room_id" "$max_seats"
}

capability_payload_start_match() {
  local game_id="$1"
  python3 -c 'import json,sys; print(json.dumps({"gameId":sys.argv[1]},separators=(",",":")))' "$game_id"
}

capability_payload_tournament_create() {
  local tournament_id="$1"
  local capacity="${2:-8}"
  python3 -c 'import json,sys; print(json.dumps({"tournamentId":sys.argv[1],"capacity":int(sys.argv[2])},separators=(",",":")))' \
    "$tournament_id" "$capacity"
}

capability_payload_tournament_id() {
  local tournament_id="$1"
  python3 -c 'import json,sys; print(json.dumps({"tournamentId":sys.argv[1]},separators=(",",":")))' "$tournament_id"
}

capability_unique_run_id() {
  python3 - <<'PY'
import time, os
print(f"{int(time.time())}-{os.getpid()}")
PY
}
