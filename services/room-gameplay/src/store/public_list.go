package store

import (
	"context"
	"fmt"
	"strings"

	"unoarena/services/room-gameplay/app"
	"unoarena/services/room-gameplay/domain"
)

// ListPublicRoomRows returns a bounded public-only keyset page ordered by room_id ASC.
// Uses rooms_public_list_idx (status, room_id) WHERE visibility='public' (keyset-only; no SQL skip).
// limit includes one-row lookahead when requested by the application service.
func (s *SessionStore) ListPublicRoomRows(ctx context.Context, status domain.RoomStatus, afterRoomID string, limit int) ([]app.PublicRoomSummary, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("nil session store")
	}
	if limit < 1 {
		return nil, nil
	}
	switch status {
	case domain.RoomStatusWaiting, domain.RoomStatusLocked, domain.RoomStatusInProgress:
	default:
		return nil, fmt.Errorf("unsupported public list status %q", status)
	}
	afterRoomID = strings.TrimSpace(afterRoomID)

	const q = `
SELECT r.room_id,
       r.status,
       r.visibility,
       r.capacity,
       (SELECT count(*)::int FROM room_roster rr WHERE rr.room_id = r.room_id AND rr.occupied),
       COALESCE(r.host_player_id, ''),
       r.room_type,
       COALESCE(r.tournament_id, '')
FROM rooms r
WHERE r.visibility = 'public'
  AND r.status = $1
  AND ($2 = '' OR r.room_id > $2)
ORDER BY r.room_id ASC
LIMIT $3`

	rows, err := s.pool.Query(ctx, q, string(status), afterRoomID, limit)
	if err != nil {
		return nil, fmt.Errorf("public list query: %w", err)
	}
	defer rows.Close()

	out := make([]app.PublicRoomSummary, 0, limit)
	for rows.Next() {
		var sum app.PublicRoomSummary
		var visibility string
		if err := rows.Scan(
			&sum.RoomID, &sum.Status, &visibility, &sum.MaxSeats, &sum.CurrentPlayers,
			&sum.HostID, &sum.RoomType, &sum.TournamentID,
		); err != nil {
			return nil, fmt.Errorf("public list scan: %w", err)
		}
		if visibility != string(domain.VisibilityPublic) {
			continue
		}
		sum.Visibility = string(domain.VisibilityPublic)
		if sum.RoomType != string(domain.RoomTypeTournament) {
			sum.TournamentID = ""
		}
		out = append(out, sum)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("public list rows: %w", err)
	}
	return out, nil
}
