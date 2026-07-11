package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/kurrent-io/KurrentDB-Client-Go/kurrentdb"
)

const (
	auditActorHeader  = "X-Audit-Actor"
	auditReasonHeader = "X-Audit-Reason"
	auditStreamName   = "gi.audit.decrypt.v1"
	auditEventType    = "gi.decrypt_export_audit.v1"
)

// DecryptAuditRecord is a durable, non-secret decrypt/export audit event (ADR-0024).
type DecryptAuditRecord struct {
	EventID       string `json:"eventId"`
	Action        string `json:"action"` // replay | export
	Actor         string `json:"actor"`
	Reason        string `json:"reason"`
	RoomID        string `json:"roomId,omitempty"`
	GameID        string `json:"gameId,omitempty"`
	Stream        string `json:"stream,omitempty"`
	CorrelationID string `json:"correlationId"`
	// KeyVersions is the sorted unique set of envelope key versions encountered.
	// Empty means unknown (failure before any version observed, or memory mode).
	KeyVersions []int `json:"keyVersions"`
	// KeyVersion is set only when exactly one version was observed (compat).
	KeyVersion int    `json:"keyVersion,omitempty"`
	Outcome    string `json:"outcome"` // success | failure
	Error      string `json:"error,omitempty"`
	At         string `json:"at"`
}

// AuditRecorder persists decrypt/export audit attempts; append failure fails the request closed.
type AuditRecorder interface {
	Record(ctx context.Context, rec DecryptAuditRecord) error
}

// MemoryAuditRecorder is an injectable in-memory audit sink for offline/tests.
type MemoryAuditRecorder struct {
	mu      sync.Mutex
	Records []DecryptAuditRecord
	Fail    error
}

func (m *MemoryAuditRecorder) Record(ctx context.Context, rec DecryptAuditRecord) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if m.Fail != nil {
		return m.Fail
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Records = append(m.Records, rec)
	return nil
}

func (m *MemoryAuditRecorder) Snapshot() []DecryptAuditRecord {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]DecryptAuditRecord, len(m.Records))
	copy(out, m.Records)
	return out
}

// KurrentAuditRecorder appends audit events to a dedicated Kurrent stream.
type KurrentAuditRecorder struct {
	client *kurrentdb.Client
}

func (a *KurrentAuditRecorder) Record(ctx context.Context, rec DecryptAuditRecord) error {
	if a == nil || a.client == nil {
		return fmt.Errorf("audit recorder unwired")
	}
	body, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	eventUUID, err := uuid.Parse(rec.EventID)
	if err != nil {
		return fmt.Errorf("audit event id: %w", err)
	}
	event := kurrentdb.EventData{
		EventID:     eventUUID,
		EventType:   auditEventType,
		ContentType: kurrentdb.ContentTypeJson,
		Data:        body,
		Metadata:    []byte(`{"audit":true}`),
	}
	opCtx, cancel := context.WithTimeout(ctx, kurrentOpTimeout)
	defer cancel()
	_, err = a.client.AppendToStream(opCtx, auditStreamName, kurrentdb.AppendToStreamOptions{
		StreamState: kurrentdb.Any{},
	}, event)
	return err
}

func newAuditRecord(action, actor, reason, roomID, gameID, stream, correlationID string, keyVersions []int, outcome, errMsg string) DecryptAuditRecord {
	versions := append([]int(nil), keyVersions...)
	sort.Ints(versions)
	rec := DecryptAuditRecord{
		EventID:       newAuditAttemptID(),
		Action:        action,
		Actor:         actor,
		Reason:        reason,
		RoomID:        roomID,
		GameID:        gameID,
		Stream:        stream,
		CorrelationID: correlationID,
		KeyVersions:   versions,
		Outcome:       outcome,
		Error:         errMsg,
		At:            time.Now().UTC().Format(time.RFC3339Nano),
	}
	if len(versions) == 1 {
		rec.KeyVersion = versions[0]
	}
	return rec
}

func newAuditAttemptID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// Extremely unlikely; still produce a unique-looking id without panicking callers.
		return "audit-" + hex.EncodeToString(b[:]) + fmt.Sprintf("%d", time.Now().UnixNano())
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return uuid.UUID(b).String()
}
