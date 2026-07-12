package app

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"unoarena/services/room-gameplay/domain"
)

const (
	// PublicListDefaultLimit is the OpenAPI/BFF default for public room pages.
	PublicListDefaultLimit = 50
	// PublicListMaxLimit is the OpenAPI hard maximum.
	PublicListMaxLimit = 100
)

// ErrPublicListBadRequest is a client validation failure for the public room list.
var ErrPublicListBadRequest = errors.New("public list bad request")

// ErrPublicListUnavailable is returned when no list adapter is wired.
var ErrPublicListUnavailable = errors.New("public list unavailable")

// PublicRoomSummary is the privacy-safe public room list entry (CLI matchmaking read).
// Never includes private rooms, hands, session data, invite material, or GI data.
type PublicRoomSummary struct {
	RoomID         string `json:"roomId"`
	Status         string `json:"status"`
	Visibility     string `json:"visibility"`
	MaxSeats       int    `json:"maxSeats"`
	CurrentPlayers int    `json:"currentPlayers"`
	HostID         string `json:"hostId"`
	RoomType       string `json:"roomType"`
	TournamentID   string `json:"tournamentId,omitempty"`
}

// PublicListPage is the Room-owned public list response body.
type PublicListPage struct {
	SchemaVersion int                 `json:"schemaVersion"`
	Rooms         []PublicRoomSummary `json:"rooms"`
	NextCursor    string              `json:"nextCursor,omitempty"`
}

// PublicListQuery is the bounded public-list read input after query validation.
type PublicListQuery struct {
	Status string
	Cursor string
	Limit  int
}

// PublicRoomLister reads public room summaries with keyset semantics (no OFFSET).
// limit is the fetch size including one-row lookahead (caller requests limit+1).
type PublicRoomLister interface {
	ListPublicRoomRows(ctx context.Context, status domain.RoomStatus, afterRoomID string, limit int) ([]PublicRoomSummary, error)
}

// ClampPublicListLimit returns a page size in [1, PublicListMaxLimit], defaulting empty/0 to 50.
func ClampPublicListLimit(limit int) int {
	if limit <= 0 {
		return PublicListDefaultLimit
	}
	if limit > PublicListMaxLimit {
		return PublicListMaxLimit
	}
	return limit
}

// ParsePublicListQuery validates status/limit/cursor query parameters.
// Default status is waiting (joinable). Allowed: waiting|locked|in_progress.
func ParsePublicListQuery(statusRaw, cursorRaw, limitRaw string) (PublicListQuery, error) {
	statusRaw = strings.TrimSpace(statusRaw)
	if statusRaw == "" {
		statusRaw = string(domain.RoomStatusWaiting)
	}
	switch domain.RoomStatus(statusRaw) {
	case domain.RoomStatusWaiting, domain.RoomStatusLocked, domain.RoomStatusInProgress:
	default:
		return PublicListQuery{}, fmt.Errorf("%w: status must be waiting, locked, or in_progress", ErrPublicListBadRequest)
	}

	limit := PublicListDefaultLimit
	if strings.TrimSpace(limitRaw) != "" {
		n, err := strconv.Atoi(strings.TrimSpace(limitRaw))
		if err != nil || n < 1 || n > PublicListMaxLimit {
			return PublicListQuery{}, fmt.Errorf("%w: limit must be an integer between 1 and %d", ErrPublicListBadRequest, PublicListMaxLimit)
		}
		limit = n
	}

	return PublicListQuery{
		Status: statusRaw,
		Cursor: strings.TrimSpace(cursorRaw),
		Limit:  limit,
	}, nil
}

// SetPublicListReader wires the public room list adapter (durable or memory).
func (s *Service) SetPublicListReader(r PublicRoomLister) {
	s.publicList = r
}

// PublicList returns one privacy-safe public room page with opaque nextCursor.
func (s *Service) PublicList(ctx context.Context, q PublicListQuery) (PublicListPage, error) {
	if s.publicList == nil {
		return PublicListPage{}, ErrPublicListUnavailable
	}
	limit := ClampPublicListLimit(q.Limit)
	status := domain.RoomStatus(q.Status)
	if status == "" {
		status = domain.RoomStatusWaiting
	}
	switch status {
	case domain.RoomStatusWaiting, domain.RoomStatusLocked, domain.RoomStatusInProgress:
	default:
		return PublicListPage{}, fmt.Errorf("%w: status must be waiting, locked, or in_progress", ErrPublicListBadRequest)
	}

	var afterRoomID string
	if strings.TrimSpace(q.Cursor) != "" {
		cur, err := DecodePublicListCursor(q.Cursor)
		if err != nil {
			return PublicListPage{}, fmt.Errorf("%w: %v", ErrPublicListBadRequest, err)
		}
		if cur.Status != string(status) {
			return PublicListPage{}, fmt.Errorf("%w: cursor status filter mismatch", ErrPublicListBadRequest)
		}
		afterRoomID = cur.RoomID
	}

	batch, err := s.publicList.ListPublicRoomRows(ctx, status, afterRoomID, limit+1)
	if err != nil {
		return PublicListPage{}, err
	}

	page := PublicListPage{
		SchemaVersion: 1,
		Rooms:         make([]PublicRoomSummary, 0, min(len(batch), limit)),
	}
	for i, row := range batch {
		if i >= limit {
			break
		}
		page.Rooms = append(page.Rooms, sanitizePublicRoomSummary(row))
	}
	if len(batch) > limit {
		last := page.Rooms[len(page.Rooms)-1]
		enc, err := EncodePublicListCursor(PublicListCursor{
			V: publicListCursorVersion, Status: string(status), RoomID: last.RoomID,
		})
		if err != nil {
			return PublicListPage{}, err
		}
		page.NextCursor = enc
	}
	return page, nil
}

func sanitizePublicRoomSummary(in PublicRoomSummary) PublicRoomSummary {
	out := PublicRoomSummary{
		RoomID:         strings.TrimSpace(in.RoomID),
		Status:         strings.TrimSpace(in.Status),
		Visibility:     string(domain.VisibilityPublic),
		MaxSeats:       in.MaxSeats,
		CurrentPlayers: in.CurrentPlayers,
		HostID:         strings.TrimSpace(in.HostID),
		RoomType:       strings.TrimSpace(in.RoomType),
	}
	if out.RoomType == string(domain.RoomTypeTournament) {
		if tid := strings.TrimSpace(in.TournamentID); tid != "" {
			out.TournamentID = tid
		}
	}
	return out
}
