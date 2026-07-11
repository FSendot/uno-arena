package main

import (
	"context"

	"unoarena/services/spectator-view/domain"
)

// SpectatorApplication is the deep application/store seam for Spectator View.
// Durable Redis and capability memory adapters share this contract.
// Store failures return error (HTTP 503); domain dispositions remain in ApplyOutcome.
type SpectatorApplication interface {
	Apply(ctx context.Context, roomID domain.RoomID, events []domain.SpectatorSafeEvent) (domain.ApplyOutcome, error)
	Rebuild(ctx context.Context, roomID domain.RoomID, events []domain.SpectatorSafeEvent) (RebuildResult, error)
	Admission(ctx context.Context, roomID domain.RoomID, auth domain.SpectatorAuth) (domain.SpectatorAdmissionDecision, error)
	SnapshotJSON(ctx context.Context, roomID domain.RoomID) ([]byte, error)
	RegisterInvite(ctx context.Context, roomID domain.RoomID, token string) (bool, error)
	HasInvite(ctx context.Context, roomID domain.RoomID, token string) (bool, error)
	RoomMeta(ctx context.Context, roomID domain.RoomID) (RoomMeta, error)
	Ready(ctx context.Context) error
}

// RoomMeta is the rebuild-status / readiness projection summary.
type RoomMeta struct {
	Sequence     uint64
	Status       string
	StreamClosed bool
	EventCount   int
	Exists       bool
}

// RebuildResult is the atomic rebuild response.
type RebuildResult struct {
	Outcomes     []domain.ApplyOutcome
	Sequence     uint64
	Status       string
	StreamClosed bool
	EventCount   int
}

// SpectatorLiveFeed delivers SSE frames after admission.
// Capability mode may use process-local StreamHub; durable mode reads Redis streams.
type SpectatorLiveFeed interface {
	// BeginSession registers for live updates. lastEventID enables resume.
	// errSnapshotRequired means the caller must emit a fresh sanitized snapshot first.
	// errTerminalRoom means new admission/streams are denied.
	BeginSession(ctx context.Context, roomID domain.RoomID, lastEventID string) (LiveSession, error)
	MarkTerminal(roomID domain.RoomID)
	IsTerminal(roomID domain.RoomID) bool
}

// LiveSession is one authorized SSE subscription.
type LiveSession interface {
	Events() <-chan StreamEvent
	Replay() []StreamEvent
	SnapshotRequired() bool
	Close()
}
