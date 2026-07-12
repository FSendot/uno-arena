package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"unoarena/services/spectator-view/domain"
)

const (
	maxCASRetries       = 8
	defaultGeneration   = "1"
	defaultStreamMaxLen = 1024 // approximate MAXLEN for local proof; snapshot covers trim
)

var (
	// ErrCASConflict indicates a concurrent writer won; callers retry Apply.
	ErrCASConflict = errors.New("spectator redis cas conflict")
	// ErrTerminalRoom denies new spectator streams.
	ErrTerminalRoom = errors.New("room_terminal")
	// ErrSnapshotRequired means Last-Event-ID is missing/trimmed.
	ErrSnapshotRequired = errors.New("snapshot_required")
)

// RedisProjectionStore is the durable Spectator View projection + stream adapter.
type RedisProjectionStore struct {
	rdb          redis.UniversalClient
	keys         KeySpace
	streamMaxLen int64
	// Optional Kafka consumer identity enables atomic ADR-0017 quarantine on
	// OutcomeQuarantined / OutcomeDropped commits. HTTP-only stores leave these empty.
	kafkaGroup string
	kafkaTopic string
}

// NewRedisProjectionStore constructs a durable store.
func NewRedisProjectionStore(rdb redis.UniversalClient, keyPrefix string) *RedisProjectionStore {
	return &RedisProjectionStore{
		rdb:          rdb,
		keys:         NewKeySpace(keyPrefix),
		streamMaxLen: defaultStreamMaxLen,
	}
}

// WithKeyPrefix returns a copy with a different key namespace (tests).
func (s *RedisProjectionStore) WithKeyPrefix(prefix string) *RedisProjectionStore {
	cp := *s
	cp.keys = NewKeySpace(prefix)
	return &cp
}

// WithStreamMaxLen sets approximate Redis stream MAXLEN (~). Non-positive resets to default.
func (s *RedisProjectionStore) WithStreamMaxLen(n int64) *RedisProjectionStore {
	cp := *s
	if n <= 0 {
		cp.streamMaxLen = defaultStreamMaxLen
	} else {
		cp.streamMaxLen = n
	}
	return &cp
}

// WithKafkaIdentity enables atomic Kafka aggregate quarantine on domain drop/quarantine.
func (s *RedisProjectionStore) WithKafkaIdentity(consumerGroup, sourceTopic string) *RedisProjectionStore {
	cp := *s
	cp.kafkaGroup = strings.TrimSpace(consumerGroup)
	cp.kafkaTopic = strings.TrimSpace(sourceTopic)
	return &cp
}

// StreamMaxLen returns the configured approximate MAXLEN.
func (s *RedisProjectionStore) StreamMaxLen() int64 { return s.streamMaxLen }

// KeyPrefix returns the configured prefix.
func (s *RedisProjectionStore) KeyPrefix() string { return s.keys.Prefix() }

// Client exposes the underlying Redis client (tests/reconnect).
func (s *RedisProjectionStore) Client() redis.UniversalClient { return s.rdb }

// Ready pings Redis (fail-closed readiness).
func (s *RedisProjectionStore) Ready(ctx context.Context) error {
	return PingRedis(ctx, s.rdb)
}

// LoadScripts caches Lua on the server.
func (s *RedisProjectionStore) LoadScripts(ctx context.Context) error {
	if err := applyCommitScript.Load(ctx, s.rdb).Err(); err != nil {
		return err
	}
	if err := rebuildSwapScript.Load(ctx, s.rdb).Err(); err != nil {
		return err
	}
	if err := recoveryRebuildSwapScript.Load(ctx, s.rdb).Err(); err != nil {
		return err
	}
	if err := kafkaQuarantineReleaseScript.Load(ctx, s.rdb).Err(); err != nil {
		return err
	}
	return kafkaQuarantineScript.Load(ctx, s.rdb).Err()
}

// FlushPrefixedKeys deletes only keys under this store's prefix (integration cleanup).
func (s *RedisProjectionStore) FlushPrefixedKeys(ctx context.Context) error {
	var cursor uint64
	pattern := s.keys.ScanPattern()
	for {
		keys, next, err := s.rdb.Scan(ctx, cursor, pattern, 100).Result()
		if err != nil {
			return err
		}
		if len(keys) > 0 {
			if err := s.rdb.Del(ctx, keys...).Err(); err != nil {
				return err
			}
		}
		cursor = next
		if cursor == 0 {
			return nil
		}
	}
}

type roomLoad struct {
	revision   uint64
	sequence   uint64
	status     string
	closed     bool
	eventCount int
	generation string
	proj       *domain.SpectatorRoomProjection
	exists     bool
}

func (s *RedisProjectionStore) loadRoom(ctx context.Context, roomID domain.RoomID) (roomLoad, error) {
	if err := ValidateRoomID(roomID); err != nil {
		return roomLoad{}, err
	}
	metaKey := s.keys.Meta(roomID)
	stateKey := s.keys.State(roomID)
	outcomesKey := s.keys.Outcomes(roomID)

	pipe := s.rdb.Pipeline()
	metaCmd := pipe.HGetAll(ctx, metaKey)
	stateCmd := pipe.Get(ctx, stateKey)
	outcomesCmd := pipe.HGetAll(ctx, outcomesKey)
	_, pipeErr := pipe.Exec(ctx)
	if pipeErr != nil && !isPipelineNil(pipeErr) {
		return roomLoad{}, pipeErr
	}

	meta, _ := metaCmd.Result()
	load := roomLoad{
		generation: defaultGeneration,
		proj:       domain.NewSpectatorRoomProjection(roomID),
	}
	if len(meta) > 0 {
		load.exists = true
	}
	if v := meta["revision"]; v != "" {
		load.revision, _ = strconv.ParseUint(v, 10, 64)
	}
	if v := meta["sequence"]; v != "" {
		load.sequence, _ = strconv.ParseUint(v, 10, 64)
	}
	load.status = meta["status"]
	load.closed = meta["streamClosed"] == "1" || meta["terminal"] == "1"
	if v := meta["eventCount"]; v != "" {
		n, _ := strconv.Atoi(v)
		load.eventCount = n
	}
	if g := meta["generation"]; g != "" {
		load.generation = g
	}

	outMap, _ := outcomesCmd.Result()
	stateRaw, stateErr := stateCmd.Bytes()
	if stateErr == nil && len(stateRaw) > 0 {
		load.exists = true
		exp, err := domain.UnmarshalExport(stateRaw)
		if err != nil {
			return roomLoad{}, fmt.Errorf("decode projection state: %w", err)
		}
		if exp.Outcomes == nil {
			exp.Outcomes = map[domain.EventID]domain.OutcomeExport{}
		}
		for eid, raw := range outMap {
			o, err := domain.UnmarshalOutcome([]byte(raw))
			if err != nil {
				return roomLoad{}, fmt.Errorf("decode outcome %s: %w", eid, err)
			}
			exp.Outcomes[domain.EventID(eid)] = toOutcomeExport(o)
		}
		proj, err := domain.RestoreProjection(exp)
		if err != nil {
			return roomLoad{}, err
		}
		load.proj = proj
		load.sequence = uint64(proj.Sequence())
		load.status = string(proj.Status())
		load.closed = proj.StreamClosed()
		return load, nil
	}

	if len(outMap) > 0 {
		load.exists = true
		exp := domain.ProjectionExport{
			RoomID:   roomID,
			Outcomes: map[domain.EventID]domain.OutcomeExport{},
		}
		for eid, raw := range outMap {
			o, err := domain.UnmarshalOutcome([]byte(raw))
			if err != nil {
				return roomLoad{}, err
			}
			exp.Outcomes[domain.EventID(eid)] = toOutcomeExport(o)
		}
		proj, err := domain.RestoreProjection(exp)
		if err != nil {
			return roomLoad{}, err
		}
		load.proj = proj
	}
	return load, nil
}

func toOutcomeExport(o domain.ApplyOutcome) domain.OutcomeExport {
	return domain.OutcomeExport{
		Kind: o.Kind, EventID: o.EventID, Sequence: o.Sequence,
		Rejection: o.Rejection, Facts: o.Facts,
	}
}

func isPipelineNil(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, redis.Nil) || strings.Contains(err.Error(), "redis: nil")
}

// Apply loads state, applies domain policy, and CAS-commits via Lua.
func (s *RedisProjectionStore) Apply(ctx context.Context, roomID domain.RoomID, events []domain.SpectatorSafeEvent) (domain.ApplyOutcome, error) {
	if err := ValidateRoomID(roomID); err != nil {
		return domain.ApplyOutcome{}, err
	}
	if len(events) == 0 {
		return domain.ApplyOutcome{
			Kind: domain.OutcomeQuarantined,
			Rejection: &domain.Rejection{
				Code: domain.RejectInvalidSchema, Message: "batch requires at least one fact",
			},
		}, nil
	}

	eventIDs := make([]domain.EventID, 0, len(events))
	for _, e := range events {
		if e.EventID.Valid() {
			eventIDs = append(eventIDs, e.EventID)
		}
	}

	var lastCAS error
	for attempt := 0; attempt < maxCASRetries; attempt++ {
		load, err := s.loadRoom(ctx, roomID)
		if err != nil {
			return domain.ApplyOutcome{}, err
		}
		if prior, ok := priorOutcome(load.proj, eventIDs); ok {
			return prior, nil
		}

		var out domain.ApplyOutcome
		if len(events) == 1 {
			out = load.proj.Apply(events[0])
		} else {
			out = load.proj.ApplyBatch(events)
		}
		if out.Kind == domain.OutcomeDuplicate {
			return out, nil
		}

		// Persist outcomes for every inbound eventId the domain recorded.
		idsToPersist := eventIDs
		if len(idsToPersist) == 0 && out.EventID.Valid() {
			idsToPersist = []domain.EventID{out.EventID}
		}

		res, err := s.commitApply(ctx, roomID, load, out, idsToPersist)
		if errors.Is(err, ErrCASConflict) {
			lastCAS = err
			if again, loadErr := s.loadRoom(ctx, roomID); loadErr == nil {
				if prior, ok := priorOutcome(again.proj, eventIDs); ok {
					return prior, nil
				}
			}
			time.Sleep(time.Duration(attempt+1) * time.Millisecond)
			continue
		}
		if err != nil {
			return domain.ApplyOutcome{}, err
		}
		// Handle Lua result before treating nil error as accepted — ok_dup is
		// success-shaped but must surface as OutcomeDuplicate (no stream append).
		if res == "ok_dup" {
			if again, loadErr := s.loadRoom(ctx, roomID); loadErr == nil {
				if prior, ok := priorOutcome(again.proj, eventIDs); ok {
					return prior, nil
				}
			}
			dup := out
			dup.Kind = domain.OutcomeDuplicate
			return dup, nil
		}
		if res == "ok" {
			return out, nil
		}
		return domain.ApplyOutcome{}, fmt.Errorf("unexpected apply commit result %q", res)
	}
	if lastCAS != nil {
		return domain.ApplyOutcome{}, lastCAS
	}
	return domain.ApplyOutcome{}, ErrCASConflict
}

func priorOutcome(proj *domain.SpectatorRoomProjection, eventIDs []domain.EventID) (domain.ApplyOutcome, bool) {
	exp := proj.ExportState()
	for _, eid := range eventIDs {
		if o, ok := exp.Outcomes[eid]; ok {
			return domain.ApplyOutcome{
				Kind: domain.OutcomeDuplicate, EventID: o.EventID, Sequence: o.Sequence,
				Rejection: o.Rejection, Facts: o.Facts,
			}, true
		}
	}
	return domain.ApplyOutcome{}, false
}

func (s *RedisProjectionStore) commitApply(
	ctx context.Context,
	roomID domain.RoomID,
	load roomLoad,
	out domain.ApplyOutcome,
	eventIDs []domain.EventID,
) (string, error) {
	mutated := out.Kind == domain.OutcomeAccepted
	newEventCount := load.eventCount
	if mutated {
		newEventCount++
	}
	newRev := load.revision + 1
	newSeq := load.sequence
	status := load.status
	if status == "" {
		status = string(domain.RoomStatusWaiting)
	}
	closed := "0"
	if load.closed {
		closed = "1"
	}
	stateJSON := ""
	if mutated {
		exp := load.proj.ExportState()
		exp.Outcomes = map[domain.EventID]domain.OutcomeExport{}
		raw, err := domain.MarshalExport(exp)
		if err != nil {
			return "", err
		}
		stateJSON = string(raw)
		newSeq = uint64(load.proj.Sequence())
		status = string(load.proj.Status())
		if load.proj.StreamClosed() {
			closed = "1"
		}
	}

	expAfter := load.proj.ExportState()
	pairs := make([]string, 0, len(eventIDs)*2)
	seen := map[domain.EventID]struct{}{}
	for _, eid := range eventIDs {
		if !eid.Valid() {
			continue
		}
		if _, ok := seen[eid]; ok {
			continue
		}
		seen[eid] = struct{}{}
		if oexp, ok := expAfter.Outcomes[eid]; ok {
			raw, err := domain.MarshalOutcome(domain.ApplyOutcome{
				Kind: oexp.Kind, EventID: oexp.EventID, Sequence: oexp.Sequence,
				Rejection: oexp.Rejection, Facts: oexp.Facts,
			})
			if err != nil {
				return "", err
			}
			pairs = append(pairs, string(eid), string(raw))
			continue
		}
		raw, err := domain.MarshalOutcome(out)
		if err != nil {
			return "", err
		}
		pairs = append(pairs, string(eid), string(raw))
	}
	if len(pairs) == 0 && out.EventID.Valid() {
		raw, err := domain.MarshalOutcome(out)
		if err != nil {
			return "", err
		}
		pairs = append(pairs, string(out.EventID), string(raw))
	}

	gen := load.generation
	if gen == "" {
		gen = defaultGeneration
	}
	streamKey := s.keys.Stream(roomID, gen)

	appendStream := "0"
	eventType := ""
	sseID := ""
	dataJSON := "{}"
	seqStr := strconv.FormatUint(newSeq, 10)
	closeFlag := "0"
	if mutated {
		appendStream = "1"
		sseID = "seq_" + seqStr
		if closed == "1" {
			eventType = "stream_closed"
			closeFlag = "1"
			if snap, err := load.proj.SnapshotJSON(); err == nil {
				dataJSON = string(snap)
			}
		} else {
			eventType = "projection_updated"
			if snap, err := load.proj.SnapshotJSON(); err == nil {
				dataJSON = string(snap)
			}
		}
	}

	mutatedFlag := "0"
	if mutated {
		mutatedFlag = "1"
	}

	args := []interface{}{
		strconv.FormatUint(load.revision, 10),
		strconv.FormatUint(load.sequence, 10),
		strconv.FormatUint(newRev, 10),
		strconv.FormatUint(newSeq, 10),
		status,
		closed,
		strconv.Itoa(newEventCount),
		mutatedFlag,
		stateJSON,
		strconv.Itoa(len(pairs) / 2),
	}
	for _, p := range pairs {
		args = append(args, p)
	}
	explicitID := ExplicitStreamID(newSeq)
	args = append(args, appendStream, eventType, sseID, dataJSON, seqStr, closeFlag,
		strconv.FormatInt(s.streamMaxLen, 10), explicitID)

	markQuarantine := "0"
	if s.kafkaGroup != "" && s.kafkaTopic != "" &&
		(out.Kind == domain.OutcomeQuarantined || out.Kind == domain.OutcomeDropped) {
		markQuarantine = "1"
		class := ClassificationForDomainOutcome(out)
		reason := "quarantined"
		if out.Rejection != nil {
			if out.Rejection.Message != "" {
				reason = out.Rejection.Message
			} else {
				reason = string(out.Rejection.Code)
			}
		}
		reason = sanitizeQuarantineReason(reason)
		args = append(args, markQuarantine,
			s.kafkaGroup,
			s.kafkaTopic,
			class,
			reason,
			string(out.EventID),
			"", // correlationId not available on domain outcome
			"0",
			"0",
			time.Now().UTC().Format(time.RFC3339Nano),
		)
	} else {
		args = append(args, markQuarantine)
	}

	res, err := applyCommitScript.Run(ctx, s.rdb, []string{
		s.keys.Meta(roomID),
		s.keys.State(roomID),
		s.keys.Outcomes(roomID),
		streamKey,
		s.keys.KafkaQuarantine(roomID),
	}, args...).Text()
	if err != nil {
		return "", err
	}
	switch res {
	case "ok":
		return "ok", nil
	case "cas":
		return "cas", ErrCASConflict
	case "ok_dup":
		return "ok_dup", nil
	default:
		return res, fmt.Errorf("unexpected apply lua result %q", res)
	}
}

// RebuildResult is returned by Rebuild.
type RebuildResult struct {
	Outcomes     []domain.ApplyOutcome
	Sequence     uint64
	Status       string
	StreamClosed bool
	EventCount   int
}

// Rebuild validates/replays all events first, then atomically swaps generation state.
func (s *RedisProjectionStore) Rebuild(ctx context.Context, roomID domain.RoomID, events []domain.SpectatorSafeEvent) (RebuildResult, error) {
	if err := ValidateRoomID(roomID); err != nil {
		return RebuildResult{}, err
	}
	// Validate/replay entirely in memory first — never partial Redis writes on failure.
	proj, outcomes := domain.RebuildFrom(roomID, events)
	exp := proj.ExportState()
	outcomePairs := make([]string, 0, len(exp.Outcomes)*2)
	for eid, oexp := range exp.Outcomes {
		raw, err := domain.MarshalOutcome(domain.ApplyOutcome{
			Kind: oexp.Kind, EventID: oexp.EventID, Sequence: oexp.Sequence,
			Rejection: oexp.Rejection, Facts: oexp.Facts,
		})
		if err != nil {
			return RebuildResult{}, err
		}
		outcomePairs = append(outcomePairs, string(eid), string(raw))
	}
	exp.Outcomes = map[domain.EventID]domain.OutcomeExport{}
	stateJSON, err := domain.MarshalExport(exp)
	if err != nil {
		return RebuildResult{}, err
	}

	load, err := s.loadRoom(ctx, roomID)
	if err != nil {
		return RebuildResult{}, err
	}
	oldGen := load.generation
	if oldGen == "" {
		oldGen = defaultGeneration
	}
	newGenNum, _ := strconv.ParseUint(oldGen, 10, 64)
	newGen := strconv.FormatUint(newGenNum+1, 10)
	if newGenNum == 0 {
		newGen = "2"
	}

	closed := "0"
	if proj.StreamClosed() {
		closed = "1"
	}
	seq := uint64(proj.Sequence())
	status := string(proj.Status())
	eventCount := len(events)

	appendStream := "0"
	eventType := ""
	sseID := ""
	dataJSON := "{}"
	closeFlag := "0"
	if seq > 0 {
		appendStream = "1"
		sseID = "seq_" + strconv.FormatUint(seq, 10)
		if snap, err := proj.SnapshotJSON(); err == nil {
			dataJSON = string(snap)
		}
		if proj.StreamClosed() {
			eventType = "stream_closed"
			closeFlag = "1"
		} else {
			eventType = "projection_updated"
		}
	}

	args := []interface{}{
		newGen,
		"1", // revision resets on rebuild
		strconv.FormatUint(seq, 10),
		status,
		closed,
		strconv.Itoa(eventCount),
		string(stateJSON),
		strconv.Itoa(len(outcomePairs) / 2),
	}
	for _, p := range outcomePairs {
		args = append(args, p)
	}
	args = append(args, appendStream, eventType, sseID, dataJSON, strconv.FormatUint(seq, 10), closeFlag,
		strconv.FormatInt(s.streamMaxLen, 10), ExplicitStreamID(seq))

	newStream := s.keys.Stream(roomID, newGen)
	_, err = rebuildSwapScript.Run(ctx, s.rdb, []string{
		s.keys.Meta(roomID),
		s.keys.State(roomID),
		s.keys.Outcomes(roomID),
		s.keys.Generation(roomID),
		newStream,
	}, args...).Text()
	if err != nil {
		return RebuildResult{}, err
	}
	// Old generation stream cleanup is deferred (not deleted here).
	_ = oldGen

	return RebuildResult{
		Outcomes:     outcomes,
		Sequence:     seq,
		Status:       status,
		StreamClosed: proj.StreamClosed(),
		EventCount:   eventCount,
	}, nil
}

// Admission evaluates spectator admission against Redis durable state.
func (s *RedisProjectionStore) Admission(ctx context.Context, roomID domain.RoomID, auth domain.SpectatorAuth) (domain.SpectatorAdmissionDecision, error) {
	load, err := s.loadRoom(ctx, roomID)
	if err != nil {
		return domain.SpectatorAdmissionDecision{}, err
	}
	return load.proj.Admission(auth), nil
}

// SnapshotJSON returns sanitized public snapshot JSON from Redis.
func (s *RedisProjectionStore) SnapshotJSON(ctx context.Context, roomID domain.RoomID) ([]byte, error) {
	load, err := s.loadRoom(ctx, roomID)
	if err != nil {
		return nil, err
	}
	return load.proj.SnapshotJSON()
}

// RoomMeta returns rebuild-status fields from Redis.
func (s *RedisProjectionStore) RoomMeta(ctx context.Context, roomID domain.RoomID) (sequence uint64, status string, streamClosed bool, eventCount int, exists bool, err error) {
	load, err := s.loadRoom(ctx, roomID)
	if err != nil {
		return 0, "", false, 0, false, err
	}
	status = load.status
	if status == "" {
		status = string(load.proj.Status())
	}
	return uint64(load.proj.Sequence()), status, load.proj.StreamClosed(), load.eventCount, load.exists, nil
}

// IsTerminal reports durable terminal flag / streamClosed.
func (s *RedisProjectionStore) IsTerminal(ctx context.Context, roomID domain.RoomID) (bool, error) {
	load, err := s.loadRoom(ctx, roomID)
	if err != nil {
		return false, err
	}
	return load.closed || load.proj.StreamClosed(), nil
}

// RegisterInvite stores SHA-256 digest only.
func (s *RedisProjectionStore) RegisterInvite(ctx context.Context, roomID domain.RoomID, token string) (bool, error) {
	token = strings.TrimSpace(token)
	if err := ValidateRoomID(roomID); err != nil || token == "" {
		return false, nil
	}
	err := s.rdb.SAdd(ctx, s.keys.Invites(roomID), inviteDigest(token)).Err()
	if err != nil {
		return false, err
	}
	return true, nil
}

// HasInvite checks digest membership.
func (s *RedisProjectionStore) HasInvite(ctx context.Context, roomID domain.RoomID, token string) (bool, error) {
	token = strings.TrimSpace(token)
	if token == "" {
		return false, nil
	}
	if err := ValidateRoomID(roomID); err != nil {
		return false, err
	}
	return s.rdb.SIsMember(ctx, s.keys.Invites(roomID), inviteDigest(token)).Result()
}

// InvitePlaintextStored reports whether any invite set member equals plaintext (tests).
func (s *RedisProjectionStore) InvitePlaintextStored(ctx context.Context, roomID domain.RoomID, token string) (bool, error) {
	return s.rdb.SIsMember(ctx, s.keys.Invites(roomID), token).Result()
}

func inviteDigest(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}
