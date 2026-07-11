package bff

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"

	"unoarena/shared/audit"
)

// ClosedAudit is the fail-closed default when no audit sink is configured.
// It never silently accepts rejections into an in-memory buffer.
type ClosedAudit struct{}

// RecordRejection always fails closed.
func (ClosedAudit) RecordRejection(audit.RejectionRecord) error {
	return errors.New("audit sink not configured")
}

// AuditSink receives structured operational/security rejection records.
// These are never domain events and must never be treated as Game Integrity entries.
type AuditSink interface {
	RecordRejection(record audit.RejectionRecord) error
}

// MemoryAudit is an in-process audit sink for offline/tests.
type MemoryAudit struct {
	mu      sync.Mutex
	records []audit.RejectionRecord
}

// NewMemoryAudit creates an empty memory audit sink.
func NewMemoryAudit() *MemoryAudit {
	return &MemoryAudit{records: make([]audit.RejectionRecord, 0)}
}

// RecordRejection stores a validated rejection audit record.
func (a *MemoryAudit) RecordRejection(record audit.RejectionRecord) error {
	if err := record.Validate(); err != nil {
		return err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.records = append(a.records, record)
	return nil
}

// Records returns a copy of recorded rejections.
func (a *MemoryAudit) Records() []audit.RejectionRecord {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make([]audit.RejectionRecord, len(a.records))
	copy(out, a.records)
	return out
}

// Len returns the number of recorded rejections.
func (a *MemoryAudit) Len() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.records)
}

// JSONLAudit is an append-only JSONL audit sink (file or stderr). Fail-closed on write errors.
type JSONLAudit struct {
	mu     sync.Mutex
	Writer io.Writer
	closer io.Closer
}

// NewJSONLAudit wraps an io.Writer as a fail-closed JSONL audit sink.
func NewJSONLAudit(w io.Writer) *JSONLAudit {
	return &JSONLAudit{Writer: w}
}

// OpenJSONLAudit opens path for append-only JSONL audit output.
func OpenJSONLAudit(path string) (*JSONLAudit, error) {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, err
	}
	return &JSONLAudit{Writer: f, closer: f}, nil
}

// NewStderrJSONLAudit writes rejection records to stderr as JSONL.
func NewStderrJSONLAudit() *JSONLAudit {
	return NewJSONLAudit(os.Stderr)
}

// RecordRejection validates and appends one JSON line. Write failures fail closed.
func (a *JSONLAudit) RecordRejection(record audit.RejectionRecord) error {
	if err := record.Validate(); err != nil {
		return err
	}
	line, err := json.Marshal(record)
	if err != nil {
		return err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.Writer == nil {
		return fmt.Errorf("audit sink writer not configured")
	}
	payload := append(line, '\n')
	n, err := a.Writer.Write(payload)
	if err != nil {
		return fmt.Errorf("audit sink write failed: %w", err)
	}
	if n != len(payload) {
		return fmt.Errorf("audit sink write failed: %w", io.ErrShortWrite)
	}
	if f, ok := a.Writer.(*os.File); ok && f != os.Stderr && f != os.Stdout {
		if err := f.Sync(); err != nil {
			return fmt.Errorf("audit sink sync failed: %w", err)
		}
	}
	return nil
}

// Close closes the underlying file when opened via OpenJSONLAudit.
func (a *JSONLAudit) Close() error {
	if a.closer != nil {
		return a.closer.Close()
	}
	return nil
}
