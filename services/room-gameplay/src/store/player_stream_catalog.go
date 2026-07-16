package store

import (
	"context"
	"fmt"
	"strings"
)

// ListPlayerStreamRoomIDs returns one keyset page of streams known by Room's
// authoritative player-feed high-water metadata. It intentionally never scans
// Redis keys and never walks the full catalog in a single call.
func (s *SessionStore) ListPlayerStreamRoomIDs(ctx context.Context, afterRoomID string, limit int) ([]string, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("nil session store")
	}
	if limit < 1 {
		return nil, nil
	}
	rows, err := s.pool.Query(ctx, `
SELECT room_id
FROM player_stream_highwater
WHERE ($1 = '' OR room_id > $1)
ORDER BY room_id ASC
LIMIT $2`, strings.TrimSpace(afterRoomID), limit)
	if err != nil {
		return nil, fmt.Errorf("list player streams: %w", err)
	}
	defer rows.Close()
	out := make([]string, 0, limit)
	for rows.Next() {
		var roomID string
		if err := rows.Scan(&roomID); err != nil {
			return nil, fmt.Errorf("scan player stream: %w", err)
		}
		out = append(out, roomID)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("player stream rows: %w", err)
	}
	return out, nil
}
