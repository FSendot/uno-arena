package main

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/plugin/kotel"

	"unoarena/services/spectator-view/domain"
)

// HeldSpectatorRecordQuery scopes a bounded post-gap DLQ replay for one recovery job.
type HeldSpectatorRecordQuery struct {
	RoomID           string
	RecoveryJobID    string
	FailedCheckpoint int64
	ResumeCheckpoint int64 // replay sequences strictly after this
}

// HeldSpectatorRecordSource loads contiguous held spectator-safe events for recovery.
// Implementations must never embed unbounded arrays into rebuild-request messages.
type HeldSpectatorRecordSource interface {
	LoadHeldAfterCheckpoint(ctx context.Context, q HeldSpectatorRecordQuery) ([]domain.SpectatorSafeEvent, error)
}

// MemoryHeldSpectatorRecordSource is a test/fake source keyed by roomId.
type MemoryHeldSpectatorRecordSource struct {
	ByRoom map[string][]ConsumerRecord
}

// LoadHeldAfterCheckpoint filters one room, sorts by sequence, bounds, and proves continuity.
func (m *MemoryHeldSpectatorRecordSource) LoadHeldAfterCheckpoint(
	ctx context.Context,
	q HeldSpectatorRecordQuery,
) ([]domain.SpectatorSafeEvent, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	roomID := strings.TrimSpace(q.RoomID)
	if roomID == "" {
		return nil, fmt.Errorf("%w: roomId required", domain.ErrHeldContinuityInvalid)
	}
	var matched []ConsumerRecord
	if m != nil {
		matched = append(matched, m.ByRoom[roomID]...)
	}
	return collectHeldFromRecords(matched, q)
}

const (
	// DefaultHeldDLQMaxScanRecords caps total polled records (matched + unrelated).
	DefaultHeldDLQMaxScanRecords = 10_000
	// DefaultHeldDLQMaxScanBytes caps total polled payload bytes.
	DefaultHeldDLQMaxScanBytes int64 = 32 << 20 // 32 MiB
	// DefaultHeldDLQMaxPollCycles caps poll iterations regardless of idle/unrelated traffic.
	DefaultHeldDLQMaxPollCycles = 64
)

// KafkaHeldSpectatorDLQSource reads the Spectator-owned safe-events DLQ for one room.
type KafkaHeldSpectatorDLQSource struct {
	Brokers           []string
	DLQTopic          string
	ExpectedSourceTop string
	ExpectedConsumer  string
	IdleEmptyPolls    int
	PollTimeout       time.Duration
	// MaxScanRecords hard-stops the scan after this many polled records (0 = default).
	MaxScanRecords int
	// MaxScanBytes hard-stops after this many polled key+value bytes (0 = default).
	MaxScanBytes int64
	// MaxPollCycles hard-stops after this many Poll calls (0 = default).
	MaxPollCycles int
	newClient     func(brokers []string, topic, group string) (heldKafkaScanner, error)
}

type heldKafkaScanner interface {
	Poll(ctx context.Context) ([]ConsumerRecord, error)
	Close() error
}

type franzHeldScanner struct {
	cl *kgo.Client
}

func (s *franzHeldScanner) Poll(ctx context.Context) ([]ConsumerRecord, error) {
	fetches := s.cl.PollFetches(ctx)
	if errs := fetches.Errors(); len(errs) > 0 {
		for _, fe := range errs {
			if fe.Err != nil {
				if ctx.Err() != nil {
					return nil, ctx.Err()
				}
				return nil, fe.Err
			}
		}
	}
	var out []ConsumerRecord
	fetches.EachRecord(func(r *kgo.Record) {
		out = append(out, consumerRecordFromKgo(r))
	})
	return out, nil
}

func (s *franzHeldScanner) Close() error {
	s.cl.Close()
	return nil
}

func defaultHeldScanner(brokers []string, topic, group string) (heldKafkaScanner, error) {
	cl, err := kgo.NewClient(
		kgo.WithHooks(kotel.NewKotel(kotel.WithTracer(kotel.NewTracer(
			kotel.TracerProvider(processTracerProvider()),
			kotel.TracerPropagator(processPropagator()),
		))).Hooks()...),
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
	)
	if err != nil {
		return nil, err
	}
	return &franzHeldScanner{cl: cl}, nil
}

// LoadHeldAfterCheckpoint scans the DLQ for one room. Unrelated rooms are skipped
// and never fail the job. Target-room records fail closed on invalid original
// metadata or schema. Uses an ephemeral consumer group so live spectator-view
// offsets are untouched.
func (s *KafkaHeldSpectatorDLQSource) LoadHeldAfterCheckpoint(
	ctx context.Context,
	q HeldSpectatorRecordQuery,
) ([]domain.SpectatorSafeEvent, error) {
	roomID := strings.TrimSpace(q.RoomID)
	if roomID == "" {
		return nil, fmt.Errorf("%w: roomId required", domain.ErrHeldContinuityInvalid)
	}
	if len(s.Brokers) == 0 || strings.TrimSpace(s.DLQTopic) == "" {
		return nil, fmt.Errorf("held DLQ kafka not configured")
	}
	expectedSource := firstNonEmpty(s.ExpectedSourceTop, DefaultSpectatorSafeTopic)
	expectedConsumer := firstNonEmpty(s.ExpectedConsumer, DefaultSpectatorKafkaGroup)
	factory := s.newClient
	if factory == nil {
		factory = defaultHeldScanner
	}
	job := strings.TrimSpace(q.RecoveryJobID)
	if job == "" {
		job = "anon"
	}
	// Ephemeral group: one-shot scan from earliest; never the live spectator-view group.
	group := fmt.Sprintf("spectator-view-held-replay-%s-%s", sanitizeGroupToken(job), sanitizeGroupToken(roomID))
	scanner, err := factory(s.Brokers, s.DLQTopic, group)
	if err != nil {
		return nil, fmt.Errorf("held dlq client: %w", err)
	}
	defer func() { _ = scanner.Close() }()

	idleLimit := s.IdleEmptyPolls
	if idleLimit < 1 {
		idleLimit = 3
	}
	pollTimeout := s.PollTimeout
	if pollTimeout <= 0 {
		pollTimeout = 2 * time.Second
	}
	maxRecords := s.MaxScanRecords
	if maxRecords < 1 {
		maxRecords = DefaultHeldDLQMaxScanRecords
	}
	maxBytes := s.MaxScanBytes
	if maxBytes < 1 {
		maxBytes = DefaultHeldDLQMaxScanBytes
	}
	maxPolls := s.MaxPollCycles
	if maxPolls < 1 {
		maxPolls = DefaultHeldDLQMaxPollCycles
	}

	var matched []ConsumerRecord
	emptyRounds := 0
	scannedRecords := 0
	var scannedBytes int64
	pollCycles := 0
	for emptyRounds < idleLimit {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if pollCycles >= maxPolls {
			return nil, fmt.Errorf("%w: held DLQ poll-cycle budget exhausted (%d)", domain.ErrHeldContinuityBound, maxPolls)
		}
		if scannedRecords >= maxRecords {
			return nil, fmt.Errorf("%w: held DLQ record scan budget exhausted (%d)", domain.ErrHeldContinuityBound, maxRecords)
		}
		if scannedBytes >= maxBytes {
			return nil, fmt.Errorf("%w: held DLQ byte scan budget exhausted (%d)", domain.ErrHeldContinuityBound, maxBytes)
		}

		pollCycles++
		pollCtx, cancel := context.WithTimeout(ctx, pollTimeout)
		recs, err := scanner.Poll(pollCtx)
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			if pollCtx.Err() != nil && len(recs) == 0 {
				emptyRounds++
				continue
			}
			return nil, err
		}
		if len(recs) == 0 {
			emptyRounds++
			continue
		}
		// Unrelated batches must NOT reset idle forever — only empty polls and
		// batches that match the target room reset idle. Hard scan budgets bound
		// active DLQs with continuous unrelated traffic.
		batchMatched := 0
		for _, rec := range recs {
			scannedRecords++
			scannedBytes += int64(len(rec.Key) + len(rec.Value))
			if scannedRecords > maxRecords {
				return nil, fmt.Errorf("%w: held DLQ record scan budget exhausted (%d)", domain.ErrHeldContinuityBound, maxRecords)
			}
			if scannedBytes > maxBytes {
				return nil, fmt.Errorf("%w: held DLQ byte scan budget exhausted (%d)", domain.ErrHeldContinuityBound, maxBytes)
			}

			keep, err := acceptHeldDLQRecord(rec, roomID, expectedSource, expectedConsumer)
			if err != nil {
				return nil, err
			}
			if !keep {
				continue
			}
			matched = append(matched, rec)
			batchMatched++
			if len(matched) > domain.MaxHeldRecoveryEvents {
				return nil, fmt.Errorf("%w: matched > %d before filter", domain.ErrHeldContinuityBound, domain.MaxHeldRecoveryEvents)
			}
		}
		if batchMatched > 0 {
			emptyRounds = 0
		}
	}
	return collectHeldFromRecords(matched, q)
}

// acceptHeldDLQRecord returns whether rec belongs to the target room for replay.
// Unrelated rooms are ignored without parsing payloads when the key differs.
// Any record identifiable as the target room fails closed unless original DLQ
// metadata and the strict safe-event schema are valid.
func acceptHeldDLQRecord(rec ConsumerRecord, roomID, expectedSource, expectedConsumer string) (bool, error) {
	key := strings.TrimSpace(string(rec.Key))
	if key != "" && key != roomID {
		return false, nil
	}

	if key != roomID {
		// Key empty/missing: light roomId peek only — do not full-parse unrelated junk.
		peeked := peekSafeRoomID(rec.Value)
		if peeked == "" || peeked != roomID {
			return false, nil
		}
		return false, fmt.Errorf("%w: held DLQ key must equal roomId %q", domain.ErrHeldContinuityInvalid, roomID)
	}

	if err := validateHeldDLQOriginalMeta(rec, expectedSource, expectedConsumer); err != nil {
		return false, err
	}
	parsed, _, err := ParseSpectatorSafeRecord(rec.Value)
	if err != nil {
		return false, fmt.Errorf("%w: held DLQ target schema invalid", domain.ErrHeldContinuityInvalid)
	}
	if parsed.RoomID != roomID {
		return false, fmt.Errorf("%w: held DLQ payload roomId mismatch", domain.ErrHeldContinuityInvalid)
	}
	return true, nil
}

func validateHeldDLQOriginalMeta(rec ConsumerRecord, expectedSource, expectedConsumer string) error {
	src := dlqHeaderValue(rec, dlqHeaderSourceTopic)
	if src != expectedSource {
		return fmt.Errorf("%w: held DLQ original source topic want %q got %q", domain.ErrHeldContinuityInvalid, expectedSource, src)
	}
	if _, err := parseNonNegativeInt64Header(rec, dlqHeaderSourcePartition); err != nil {
		return fmt.Errorf("%w: held DLQ original partition: %v", domain.ErrHeldContinuityInvalid, err)
	}
	if _, err := parseNonNegativeInt64Header(rec, dlqHeaderSourceOffset); err != nil {
		return fmt.Errorf("%w: held DLQ original offset: %v", domain.ErrHeldContinuityInvalid, err)
	}
	consumer := dlqHeaderValue(rec, dlqHeaderConsumer)
	if consumer == "" || consumer != expectedConsumer {
		return fmt.Errorf("%w: held DLQ consumer identity want %q got %q", domain.ErrHeldContinuityInvalid, expectedConsumer, consumer)
	}
	return nil
}

func parseNonNegativeInt64Header(rec ConsumerRecord, key string) (int64, error) {
	raw := dlqHeaderValue(rec, key)
	if raw == "" {
		return 0, fmt.Errorf("missing %s", key)
	}
	n, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("unparseable %s", key)
	}
	if n < 0 {
		return 0, fmt.Errorf("negative %s", key)
	}
	return n, nil
}

func collectHeldFromRecords(recs []ConsumerRecord, q HeldSpectatorRecordQuery) ([]domain.SpectatorSafeEvent, error) {
	roomID := strings.TrimSpace(q.RoomID)
	type heldItem struct {
		seq int64
		evt domain.SpectatorSafeEvent
	}
	var items []heldItem
	seenSeq := map[int64]domain.SpectatorSafeEvent{}
	for _, rec := range recs {
		key := strings.TrimSpace(string(rec.Key))
		parsed, evt, err := ParseSpectatorSafeRecord(rec.Value)
		if err != nil {
			return nil, fmt.Errorf("%w: held dlq record parse", domain.ErrHeldContinuityInvalid)
		}
		if parsed.RoomID != roomID {
			continue
		}
		if key != "" && key != roomID {
			continue
		}
		if parsed.Sequence <= q.ResumeCheckpoint {
			continue
		}
		if prior, dup := seenSeq[parsed.Sequence]; dup {
			if !reflect.DeepEqual(prior, evt) {
				return nil, fmt.Errorf("%w: conflicting held event at sequence %d", domain.ErrHeldContinuityInvalid, parsed.Sequence)
			}
			continue
		}
		seenSeq[parsed.Sequence] = evt
		items = append(items, heldItem{seq: parsed.Sequence, evt: evt})
	}
	sort.Slice(items, func(i, j int) bool { return items[i].seq < items[j].seq })
	if len(items) > domain.MaxHeldRecoveryEvents {
		return nil, fmt.Errorf("%w: %d > %d", domain.ErrHeldContinuityBound, len(items), domain.MaxHeldRecoveryEvents)
	}
	out := make([]domain.SpectatorSafeEvent, 0, len(items))
	for _, it := range items {
		out = append(out, it.evt)
	}
	if err := domain.ValidateHeldContinuity(domain.SequenceNumber(q.ResumeCheckpoint), out); err != nil {
		return nil, err
	}
	return out, nil
}

func sanitizeGroupToken(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return "x"
	}
	var b strings.Builder
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_':
			b.WriteRune(r)
		default:
			b.WriteByte('-')
		}
		if b.Len() >= 48 {
			break
		}
	}
	return b.String()
}

func dlqHeaderValue(rec ConsumerRecord, key string) string {
	if rec.Headers == nil {
		return ""
	}
	return strings.TrimSpace(rec.Headers[key])
}

// EncodeHeldDLQTestRecord builds a ConsumerRecord for MemoryHeldSpectatorRecordSource tests.
func EncodeHeldDLQTestRecord(roomID string, offset int64, value []byte) ConsumerRecord {
	return ConsumerRecord{
		Topic:     DefaultSpectatorSafeDLQTopic,
		Partition: 0,
		Offset:    offset,
		Key:       []byte(roomID),
		Value:     value,
	}
}

// heldScanCheckpointLabel documents query identity for logs/ops (no PII).
func heldScanCheckpointLabel(q HeldSpectatorRecordQuery) string {
	return q.RoomID + "/" + q.RecoveryJobID + "/fc=" + strconv.FormatInt(q.FailedCheckpoint, 10)
}
