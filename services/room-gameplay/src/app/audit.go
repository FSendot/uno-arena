package app

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"

	"unoarena/shared/audit"
)

// JSONLAuditSink is an append-only JSONL rejection audit sink (file or stderr).
// Write failures fail closed — callers must not ignore Record errors.
// Record is idempotent by commandId: exact duplicates are no-ops; conflicting
// payloads for the same commandId error.
type JSONLAuditSink struct {
	mu     sync.Mutex
	Writer io.Writer
	closer io.Closer
	seen   map[string]string // commandId -> canonical JSON identity
}

// NewJSONLAuditSink wraps an io.Writer as a fail-closed JSONL audit sink.
func NewJSONLAuditSink(w io.Writer) *JSONLAuditSink {
	return &JSONLAuditSink{Writer: w, seen: make(map[string]string)}
}

// NewStderrJSONLAuditSink writes rejection records to stderr as JSONL.
func NewStderrJSONLAuditSink() *JSONLAuditSink {
	return NewJSONLAuditSink(os.Stderr)
}

// OpenJSONLAuditSink opens path for append-only JSONL audit output and scans
// existing lines so commandId idempotency survives process restart.
// Malformed/partial JSON, a nonempty file whose final record lacks a trailing
// newline, missing commandId, or conflicting historical payloads for the same
// commandId fail closed — the sink is not opened for append onto a corrupt
// tail. Exact historical duplicates are accepted.
func OpenJSONLAuditSink(path string) (*JSONLAuditSink, error) {
	seen, err := scanJSONLAuditIdentities(path)
	if err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, err
	}
	return &JSONLAuditSink{Writer: f, closer: f, seen: seen}, nil
}

func scanJSONLAuditIdentities(path string) (map[string]string, error) {
	seen := make(map[string]string)
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return seen, nil
		}
		return nil, err
	}
	defer f.Close()

	// Fail closed before append: a nonempty file whose final byte is not '\n'
	// means the last record (even if valid JSON) would be concatenated with the
	// next Write under O_APPEND. Prefer reject over silent repair.
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if info.Size() > 0 {
		if _, err := f.Seek(-1, io.SeekEnd); err != nil {
			return nil, err
		}
		var last [1]byte
		if _, err := f.Read(last[:]); err != nil {
			return nil, err
		}
		if last[0] != '\n' {
			return nil, fmt.Errorf("audit log corrupt: final record missing trailing newline")
		}
		if _, err := f.Seek(0, io.SeekStart); err != nil {
			return nil, err
		}
	}

	sc := bufio.NewScanner(f)
	// Rejection records are small; allow a generous line size.
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	lineNo := 0
	for sc.Scan() {
		lineNo++
		line := bytes.TrimSpace(sc.Bytes())
		if len(line) == 0 {
			continue
		}
		var rec audit.RejectionRecord
		if err := json.Unmarshal(line, &rec); err != nil {
			return nil, fmt.Errorf("audit log corrupt at line %d: malformed JSON: %w", lineNo, err)
		}
		if strings.TrimSpace(rec.CommandID) == "" {
			return nil, fmt.Errorf("audit log corrupt at line %d: missing commandId", lineNo)
		}
		if err := rec.Validate(); err != nil {
			return nil, fmt.Errorf("audit log corrupt at line %d: %w", lineNo, err)
		}
		canon, err := canonicalAuditIdentity(rec)
		if err != nil {
			return nil, fmt.Errorf("audit log corrupt at line %d: %w", lineNo, err)
		}
		if prev, ok := seen[rec.CommandID]; ok {
			if prev != canon {
				return nil, fmt.Errorf("audit record conflict for commandId %q", rec.CommandID)
			}
			continue // exact historical duplicate
		}
		seen[rec.CommandID] = canon
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	return seen, nil
}

func canonicalAuditIdentity(rec audit.RejectionRecord) (string, error) {
	b, err := json.Marshal(rec)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func checkAuditIdempotency(seen map[string]string, rec audit.RejectionRecord) (canon string, skip bool, err error) {
	canon, err = canonicalAuditIdentity(rec)
	if err != nil {
		return "", false, err
	}
	prev, ok := seen[rec.CommandID]
	if !ok {
		return canon, false, nil
	}
	if prev == canon {
		return canon, true, nil
	}
	return "", false, fmt.Errorf("audit record conflict for commandId %q", rec.CommandID)
}

// Record validates and appends one JSON line.
func (a *JSONLAuditSink) Record(_ context.Context, rec audit.RejectionRecord) error {
	if err := rec.Validate(); err != nil {
		return err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.seen == nil {
		a.seen = make(map[string]string)
	}
	canon, skip, err := checkAuditIdempotency(a.seen, rec)
	if err != nil {
		return err
	}
	if skip {
		return nil
	}
	if a.Writer == nil {
		return fmt.Errorf("audit sink writer not configured")
	}
	payload := append([]byte(canon), '\n')
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
	a.seen[rec.CommandID] = canon
	return nil
}

// Close closes the underlying file when opened via OpenJSONLAuditSink.
func (a *JSONLAuditSink) Close() error {
	if a.closer != nil {
		return a.closer.Close()
	}
	return nil
}
