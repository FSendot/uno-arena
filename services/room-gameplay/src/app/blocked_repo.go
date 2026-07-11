package app

import (
	"errors"
	"time"

	"unoarena/services/room-gameplay/domain"
	"unoarena/shared/audit"
)

// ErrPostgresAdapterBlocked is returned by configured-mode session storage when
// the durable Postgres adapter is not implemented.
var ErrPostgresAdapterBlocked = errors.New("postgres_adapter_blocked")

// BlockedSessionRepository refuses all persistence. Used in configured mode so
// Room never silently falls back to an in-memory repository.
type BlockedSessionRepository struct{}

func (BlockedSessionRepository) Get(domain.RoomID) (*domain.Session, bool) {
	return nil, false
}

func (BlockedSessionRepository) Commit(CommitRequest) error {
	return ErrPostgresAdapterBlocked
}

func (BlockedSessionRepository) Delete(domain.RoomID) error {
	return ErrPostgresAdapterBlocked
}

func (BlockedSessionRepository) ListPendingOutbox(int) ([]OutboxEntry, error) {
	return nil, ErrPostgresAdapterBlocked
}

func (BlockedSessionRepository) MarkOutboxEntryPublished(string, time.Time) error {
	return ErrPostgresAdapterBlocked
}

func (BlockedSessionRepository) PlayerSession(string, string) (string, bool) {
	return "", false
}

func (BlockedSessionRepository) GetProvision(ProvisionKey) (string, bool) {
	return "", false
}

func (BlockedSessionRepository) IntegrityRevision(string) int64 { return 0 }

func (BlockedSessionRepository) PeekStreamSeq(string) int64 { return 0 }

func (BlockedSessionRepository) GetGlobalOutcome(string) (domain.CommandOutcome, bool) {
	return domain.CommandOutcome{}, false
}

func (BlockedSessionRepository) GetPendingAudit(string) (audit.RejectionRecord, bool) {
	return audit.RejectionRecord{}, false
}

func (BlockedSessionRepository) MarkAuditComplete(string) error {
	return ErrPostgresAdapterBlocked
}
