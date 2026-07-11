package main

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"unoarena/services/game-integrity/domain"
)

type deckErrRepo struct {
	*MemoryStreamRepository
	deckErr error
	findErr error
}

func (r *deckErrRepo) WithExistingDeck(ctx context.Context, roomID domain.RoomID, gameID domain.GameID, fn func(*DeckState) error) error {
	if r.deckErr != nil {
		return r.deckErr
	}
	return r.MemoryStreamRepository.WithExistingDeck(ctx, roomID, gameID, fn)
}

func (r *deckErrRepo) FindByGameID(ctx context.Context, gameID domain.GameID) (domain.RoomID, bool, error) {
	if r.findErr != nil {
		return "", false, r.findErr
	}
	return r.MemoryStreamRepository.FindByGameID(ctx, gameID)
}

func TestExport_RequestedMissingDeckIsFailure(t *testing.T) {
	srv := newTestServer(t)
	mux := srv.routes()
	room := "room-export-miss"
	_ = doJSON(t, mux, http.MethodPost, "/internal/v1/game-logs/"+room+"/append", map[string]any{
		"eventId": "e1", "expectedRevision": 0, "eventType": "PlayCard", "gameId": "g-miss",
	}, withRoomCred(nil))

	export := doJSON(t, mux, http.MethodGet, "/internal/v1/audit/exports/g-miss?roomId="+room, nil, withAuditCred(nil))
	if export.Code == http.StatusOK {
		t.Fatalf("requested missing deck must not return partial 200: %s", export.Body.String())
	}
	if export.Code != http.StatusNotFound && export.Code != http.StatusBadRequest && export.Code != http.StatusInternalServerError {
		t.Fatalf("unexpected status %d %s", export.Code, export.Body.String())
	}
}

func TestExport_DeckStoreOutageIsServiceError(t *testing.T) {
	base := NewMemoryStreamRepository()
	repo := &deckErrRepo{MemoryStreamRepository: base, deckErr: errors.New("kurrent unavailable")}
	svc := NewService(repo)
	_, _, err := svc.Append(context.Background(), AppendRequest{
		RoomID: "room-out", EventID: "e1", ExpectedRevision: 0, EventType: "PlayCard", GameID: "g-out",
	})
	if err != nil {
		t.Fatal(err)
	}
	_, rej, err := svc.Export(context.Background(), "room-out", "g-out", false)
	if rej == nil && err == nil {
		t.Fatal("store outage must surface as service error")
	}
	if err == nil {
		t.Fatal("expected err from outage, not only rejection")
	}
}

func TestExportByGameID_FindErrorIsNotFalseNotFound(t *testing.T) {
	base := NewMemoryStreamRepository()
	repo := &deckErrRepo{MemoryStreamRepository: base, findErr: errors.New("bind stream read failed")}
	svc := NewService(repo)
	_, rej, err := svc.ExportByGameID(context.Background(), "g-any", false)
	if err == nil {
		t.Fatalf("find error must not become false not-found; rej=%v", rej)
	}
}
