package app

import (
	"context"
	"errors"
	"time"

	"unoarena/services/room-gameplay/domain"
	"unoarena/shared/audit"
)

// ErrPostgresAdapterBlocked is returned by misconfigured mode when DATABASE_URL
// is absent and capability mode is not enabled.
var ErrPostgresAdapterBlocked = errors.New("postgres_adapter_blocked")

// BlockedSessionRepository refuses all persistence. Used when Room is
// misconfigured so it never silently falls back to an in-memory repository.
type BlockedSessionRepository struct{}

func (BlockedSessionRepository) Get(context.Context, domain.RoomID) (*domain.Session, bool) {
	return nil, false
}

func (BlockedSessionRepository) Commit(context.Context, CommitRequest) error {
	return ErrPostgresAdapterBlocked
}

func (BlockedSessionRepository) Delete(context.Context, domain.RoomID) error {
	return ErrPostgresAdapterBlocked
}

func (BlockedSessionRepository) ListPendingOutbox(context.Context, int) ([]OutboxEntry, error) {
	return nil, ErrPostgresAdapterBlocked
}

func (BlockedSessionRepository) MarkOutboxEntryPublished(context.Context, string, time.Time) error {
	return ErrPostgresAdapterBlocked
}

func (BlockedSessionRepository) PlayerSession(context.Context, string, string) (string, bool) {
	return "", false
}

func (BlockedSessionRepository) GetProvision(context.Context, ProvisionKey) (string, bool) {
	return "", false
}

func (BlockedSessionRepository) IntegrityRevision(context.Context, string) int64 { return 0 }

func (BlockedSessionRepository) PeekStreamSeq(context.Context, string) int64 { return 0 }

func (BlockedSessionRepository) GetGlobalOutcome(context.Context, string) (domain.CommandOutcome, bool) {
	return domain.CommandOutcome{}, false
}

func (BlockedSessionRepository) GetPendingAudit(context.Context, string) (audit.RejectionRecord, bool) {
	return audit.RejectionRecord{}, false
}

func (BlockedSessionRepository) MarkAuditComplete(context.Context, string) error {
	return ErrPostgresAdapterBlocked
}
