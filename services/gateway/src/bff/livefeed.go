package bff

import (
	"context"
)

// LiveFeed opens authorized SSE sessions after admission/auth at the HTTP layer.
// Capability/fakes use HubLiveFeed; durable configured mode uses RedisLiveFeed
// for player and spectator streams while retaining Hub for control-state gates.
type LiveFeed interface {
	BeginSession(ctx context.Context, kind StreamKind, roomID, sessionID, playerID, lastEventID string) (LiveSession, error)
}

// LiveSession is one SSE subscription.
type LiveSession interface {
	Events() <-chan StreamEvent
	Replay() []StreamEvent
	Close()
}

// HubLiveFeed adapts the in-process Hub to LiveFeed (fakes + capability).
type HubLiveFeed struct {
	hub *Hub
}

// NewHubLiveFeed wraps hub. hub must be non-nil.
func NewHubLiveFeed(hub *Hub) *HubLiveFeed {
	if hub == nil {
		hub = NewHub()
	}
	return &HubLiveFeed{hub: hub}
}

type hubLiveSession struct {
	events <-chan StreamEvent
	replay []StreamEvent
	cancel func()
}

func (s *hubLiveSession) Events() <-chan StreamEvent { return s.events }
func (s *hubLiveSession) Replay() []StreamEvent      { return s.replay }
func (s *hubLiveSession) Close() {
	if s.cancel != nil {
		s.cancel()
	}
}

// BeginSession implements LiveFeed via Hub.Subscribe.
func (f *HubLiveFeed) BeginSession(_ context.Context, kind StreamKind, roomID, sessionID, playerID, lastEventID string) (LiveSession, error) {
	_, events, replay, cancel, err := f.hub.Subscribe(kind, roomID, sessionID, playerID, lastEventID)
	if err != nil {
		return nil, err
	}
	return &hubLiveSession{events: events, replay: replay, cancel: cancel}, nil
}
