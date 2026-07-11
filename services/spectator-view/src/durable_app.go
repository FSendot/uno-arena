package main

import (
	"context"
	"encoding/json"
	"errors"
	"sync"

	"unoarena/services/spectator-view/domain"
	"unoarena/services/spectator-view/store"
)

// durableApp adapts store.RedisProjectionStore to SpectatorApplication + LiveFeed.
type durableApp struct {
	store *store.RedisProjectionStore
}

func newDurableApp(s *store.RedisProjectionStore) *durableApp {
	return &durableApp{store: s}
}

func (a *durableApp) Apply(ctx context.Context, roomID domain.RoomID, events []domain.SpectatorSafeEvent) (domain.ApplyOutcome, error) {
	return a.store.Apply(ctx, roomID, events)
}

func (a *durableApp) Rebuild(ctx context.Context, roomID domain.RoomID, events []domain.SpectatorSafeEvent) (RebuildResult, error) {
	r, err := a.store.Rebuild(ctx, roomID, events)
	if err != nil {
		return RebuildResult{}, err
	}
	return RebuildResult{
		Outcomes: r.Outcomes, Sequence: r.Sequence, Status: r.Status,
		StreamClosed: r.StreamClosed, EventCount: r.EventCount,
	}, nil
}

func (a *durableApp) Admission(ctx context.Context, roomID domain.RoomID, auth domain.SpectatorAuth) (domain.SpectatorAdmissionDecision, error) {
	return a.store.Admission(ctx, roomID, auth)
}

func (a *durableApp) SnapshotJSON(ctx context.Context, roomID domain.RoomID) ([]byte, error) {
	return a.store.SnapshotJSON(ctx, roomID)
}

func (a *durableApp) RegisterInvite(ctx context.Context, roomID domain.RoomID, token string) (bool, error) {
	return a.store.RegisterInvite(ctx, roomID, token)
}

func (a *durableApp) HasInvite(ctx context.Context, roomID domain.RoomID, token string) (bool, error) {
	return a.store.HasInvite(ctx, roomID, token)
}

func (a *durableApp) RoomMeta(ctx context.Context, roomID domain.RoomID) (RoomMeta, error) {
	seq, status, closed, count, exists, err := a.store.RoomMeta(ctx, roomID)
	if err != nil {
		return RoomMeta{}, err
	}
	return RoomMeta{Sequence: seq, Status: status, StreamClosed: closed, EventCount: count, Exists: exists}, nil
}

func (a *durableApp) Ready(ctx context.Context) error {
	return a.store.Ready(ctx)
}

// redisLiveFeed adapts Redis stream sessions to SpectatorLiveFeed.
type redisLiveFeed struct {
	store *store.RedisProjectionStore
}

func newRedisLiveFeed(s *store.RedisProjectionStore) *redisLiveFeed {
	return &redisLiveFeed{store: s}
}

func (f *redisLiveFeed) BeginSession(ctx context.Context, roomID domain.RoomID, lastEventID string) (LiveSession, error) {
	sess, err := f.store.BeginLiveSession(ctx, roomID, lastEventID)
	if sess == nil {
		return nil, err
	}
	adapted := &redisLiveSessionAdapter{sess: sess}
	if errors.Is(err, store.ErrSnapshotRequired) {
		return adapted, errSnapshotRequired
	}
	if errors.Is(err, store.ErrTerminalRoom) {
		adapted.Close()
		return nil, errTerminalRoom
	}
	if err != nil {
		adapted.Close()
		return nil, err
	}
	return adapted, nil
}

func (f *redisLiveFeed) MarkTerminal(roomID domain.RoomID) {
	// Terminal is durable in Redis meta; active readers observe stream_closed.
	_ = roomID
}

func (f *redisLiveFeed) IsTerminal(roomID domain.RoomID) bool {
	ok, err := f.store.IsTerminal(context.Background(), roomID)
	return err == nil && ok
}

type redisLiveSessionAdapter struct {
	sess *store.RedisLiveSession
	once sync.Once
	ch   <-chan StreamEvent
}

func (a *redisLiveSessionAdapter) Events() <-chan StreamEvent {
	a.once.Do(func() {
		out := make(chan StreamEvent)
		go func() {
			defer close(out)
			for ev := range a.sess.Events() {
				out <- StreamEvent{
					ID: ev.ID, Event: ev.Event, Data: json.RawMessage(ev.Data), Sequence: ev.Sequence,
				}
			}
		}()
		a.ch = out
	})
	return a.ch
}

func (a *redisLiveSessionAdapter) Replay() []StreamEvent {
	in := a.sess.Replay()
	out := make([]StreamEvent, 0, len(in))
	for _, ev := range in {
		out = append(out, StreamEvent{
			ID: ev.ID, Event: ev.Event, Data: json.RawMessage(ev.Data), Sequence: ev.Sequence,
		})
	}
	return out
}

func (a *redisLiveSessionAdapter) SnapshotRequired() bool { return a.sess.SnapshotRequired() }
func (a *redisLiveSessionAdapter) Close()                 { a.sess.Close() }
