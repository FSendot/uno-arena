package store

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"unoarena/services/spectator-view/domain"
)

var (
	// ErrRecoveryConflict means fence generation/sequence did not match in a
	// fail-closed way (not "newer Apply won").
	ErrRecoveryConflict = errors.New("spectator recovery rebuild conflict")
	// ErrInvalidReleaseNote means release_note is not on the closed allowlist.
	ErrInvalidReleaseNote = errors.New("invalid quarantine release_note")
)

// Closed allowlist for quarantine release_note (no raw operator text).
const (
	ReleaseNoteRecoveryContinuityProven = "recovery_continuity_proven"
	ReleaseNoteRebuildJobComplete       = "rebuild_job_complete"

	// RebuildDoneRetention is the bounded TTL for atomic recovery completion markers.
	RebuildDoneRetention = 7 * 24 * time.Hour
)

var allowedReleaseNotes = map[string]struct{}{
	ReleaseNoteRecoveryContinuityProven: {},
	ReleaseNoteRebuildJobComplete:       {},
}

// ValidateQuarantineReleaseNote accepts only closed allowlist codes.
func ValidateQuarantineReleaseNote(note string) error {
	note = strings.TrimSpace(note)
	if note == "" {
		return fmt.Errorf("%w: empty", ErrInvalidReleaseNote)
	}
	if _, ok := allowedReleaseNotes[note]; !ok {
		return fmt.Errorf("%w: %q", ErrInvalidReleaseNote, note)
	}
	return nil
}

// RecoveryRelease controls atomic quarantine release on successful fenced swap.
type RecoveryRelease struct {
	Enabled bool
	Note    string // must be allowlisted when Enabled
}

// RecoveryIdempotency is the declared AsyncAPI (recoveryJobId, roomId, failedCheckpoint)
// identity written atomically with a successful fenced swap + quarantine release.
// Empty Identity disables the marker (compatibility wrapper for store tests).
type RecoveryIdempotency struct {
	Identity string
}

// RecoveryRebuildResult is returned by RecoveryRebuildFromSnapshot.
type RecoveryRebuildResult struct {
	Stale              bool // newer live Apply won; Redis not mutated; treat as success/no-op
	AlreadyDone        bool // identical idempotency key already committed; no mutation
	QuarantineReleased bool
	Generation         string
	Outcomes           []domain.ApplyOutcome
	Sequence           uint64
	Status             string
	StreamClosed       bool
	EventCount         int
}

// RecoveryRebuildFromSnapshot rebuilds from SnapshotSanitized + contiguous held
// events under a CAS-fenced generation swap. Quarantine releases only in the
// same Lua commit when release is requested and the fence succeeds (not stale).
// Compatibility wrapper: does not write an idempotency marker.
func (s *RedisProjectionStore) RecoveryRebuildFromSnapshot(
	ctx context.Context,
	roomID domain.RoomID,
	snapshotEvent domain.SpectatorSafeEvent,
	fenceGen string,
	fenceSeq uint64,
	heldEvents []domain.SpectatorSafeEvent,
	release RecoveryRelease,
) (RecoveryRebuildResult, error) {
	return s.RecoveryRebuildFromSnapshotWithIdempotency(
		ctx, roomID, snapshotEvent, fenceGen, fenceSeq, heldEvents, release, RecoveryIdempotency{},
	)
}

// RecoveryRebuildFromSnapshotWithIdempotency is the production recovery path that
// atomically persists the declared idempotency identity with a successful swap.
func (s *RedisProjectionStore) RecoveryRebuildFromSnapshotWithIdempotency(
	ctx context.Context,
	roomID domain.RoomID,
	snapshotEvent domain.SpectatorSafeEvent,
	fenceGen string,
	fenceSeq uint64,
	heldEvents []domain.SpectatorSafeEvent,
	release RecoveryRelease,
	idemp RecoveryIdempotency,
) (RecoveryRebuildResult, error) {
	if err := ValidateRoomID(roomID); err != nil {
		return RecoveryRebuildResult{}, err
	}
	fenceGen = strings.TrimSpace(fenceGen)
	if fenceGen == "" {
		fenceGen = defaultGeneration
	}
	if release.Enabled {
		if err := ValidateQuarantineReleaseNote(release.Note); err != nil {
			return RecoveryRebuildResult{}, err
		}
	}

	// Prove continuity + privacy in memory first — never Redis write / release on failure.
	proj, outcomes, err := domain.RebuildFromRecoverySnapshot(roomID, snapshotEvent, heldEvents)
	if err != nil {
		return RecoveryRebuildResult{}, err
	}
	// Rebuilt projection must not regress below the fence sequence.
	if uint64(proj.Sequence()) < fenceSeq {
		return RecoveryRebuildResult{}, fmt.Errorf("%w: rebuilt seq %d < fence seq %d",
			ErrRecoveryConflict, proj.Sequence(), fenceSeq)
	}

	return s.fencedRebuildSwap(ctx, roomID, fenceGen, fenceSeq, proj, outcomes, 1+len(heldEvents), release, idemp)
}

// FencedRebuildSwap commits an already-built projection under CAS fencing.
// Prefer RecoveryRebuildFromSnapshot for production recovery so continuity is proven first.
// Compatibility wrapper: does not write an idempotency marker.
func (s *RedisProjectionStore) FencedRebuildSwap(
	ctx context.Context,
	roomID domain.RoomID,
	fenceGen string,
	fenceSeq uint64,
	proj *domain.SpectatorRoomProjection,
	outcomes []domain.ApplyOutcome,
	eventCount int,
	release RecoveryRelease,
) (RecoveryRebuildResult, error) {
	return s.FencedRebuildSwapWithIdempotency(
		ctx, roomID, fenceGen, fenceSeq, proj, outcomes, eventCount, release, RecoveryIdempotency{},
	)
}

// FencedRebuildSwapWithIdempotency commits under CAS fencing with optional atomic marker.
func (s *RedisProjectionStore) FencedRebuildSwapWithIdempotency(
	ctx context.Context,
	roomID domain.RoomID,
	fenceGen string,
	fenceSeq uint64,
	proj *domain.SpectatorRoomProjection,
	outcomes []domain.ApplyOutcome,
	eventCount int,
	release RecoveryRelease,
	idemp RecoveryIdempotency,
) (RecoveryRebuildResult, error) {
	if err := ValidateRoomID(roomID); err != nil {
		return RecoveryRebuildResult{}, err
	}
	if proj == nil {
		return RecoveryRebuildResult{}, fmt.Errorf("nil projection")
	}
	fenceGen = strings.TrimSpace(fenceGen)
	if fenceGen == "" {
		fenceGen = defaultGeneration
	}
	if release.Enabled {
		if err := ValidateQuarantineReleaseNote(release.Note); err != nil {
			return RecoveryRebuildResult{}, err
		}
	}
	return s.fencedRebuildSwap(ctx, roomID, fenceGen, fenceSeq, proj, outcomes, eventCount, release, idemp)
}

func (s *RedisProjectionStore) fencedRebuildSwap(
	ctx context.Context,
	roomID domain.RoomID,
	fenceGen string,
	fenceSeq uint64,
	proj *domain.SpectatorRoomProjection,
	outcomes []domain.ApplyOutcome,
	eventCount int,
	release RecoveryRelease,
	idemp RecoveryIdempotency,
) (RecoveryRebuildResult, error) {
	exp := proj.ExportState()
	outcomePairs := make([]string, 0, len(exp.Outcomes)*2)
	for eid, oexp := range exp.Outcomes {
		raw, err := domain.MarshalOutcome(domain.ApplyOutcome{
			Kind: oexp.Kind, EventID: oexp.EventID, Sequence: oexp.Sequence,
			Rejection: oexp.Rejection, Facts: oexp.Facts,
		})
		if err != nil {
			return RecoveryRebuildResult{}, err
		}
		outcomePairs = append(outcomePairs, string(eid), string(raw))
	}
	exp.Outcomes = map[domain.EventID]domain.OutcomeExport{}
	stateJSON, err := domain.MarshalExport(exp)
	if err != nil {
		return RecoveryRebuildResult{}, err
	}

	newGenNum, _ := strconv.ParseUint(fenceGen, 10, 64)
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

	releaseFlag := "0"
	releaseNote := ""
	releasedAt := ""
	if release.Enabled {
		releaseFlag = "1"
		releaseNote = strings.TrimSpace(release.Note)
		releasedAt = time.Now().UTC().Format(time.RFC3339Nano)
	}

	markDone := "0"
	ttlSeconds := int64(0)
	identity := strings.TrimSpace(idemp.Identity)
	doneKey := s.keys.RebuildDone(roomID, identity)
	if identity != "" {
		markDone = "1"
		ttlSeconds = int64(RebuildDoneRetention / time.Second)
		if ttlSeconds < 1 {
			ttlSeconds = 1
		}
	}

	args := []interface{}{
		fenceGen,
		strconv.FormatUint(fenceSeq, 10),
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
		strconv.FormatInt(s.streamMaxLen, 10), ExplicitStreamID(seq),
		releaseFlag, releaseNote, releasedAt,
		markDone, strconv.FormatInt(ttlSeconds, 10))

	newStream := s.keys.Stream(roomID, newGen)
	res, err := recoveryRebuildSwapScript.Run(ctx, s.rdb, []string{
		s.keys.Meta(roomID),
		s.keys.State(roomID),
		s.keys.Outcomes(roomID),
		s.keys.Generation(roomID),
		newStream,
		s.keys.KafkaQuarantine(roomID),
		doneKey,
	}, args...).Text()
	if err != nil {
		return RecoveryRebuildResult{}, err
	}
	switch res {
	case "ok":
		return RecoveryRebuildResult{
			Stale:              false,
			AlreadyDone:        false,
			QuarantineReleased: release.Enabled,
			Generation:         newGen,
			Outcomes:           outcomes,
			Sequence:           seq,
			Status:             status,
			StreamClosed:       proj.StreamClosed(),
			EventCount:         eventCount,
		}, nil
	case "already_done":
		// Identical key already committed with owned-state mutation — idempotent success.
		return RecoveryRebuildResult{AlreadyDone: true}, nil
	case "stale":
		// Newer live Apply won — idempotent no-op; do not release quarantine or mark done.
		return RecoveryRebuildResult{Stale: true}, nil
	case "conflict":
		return RecoveryRebuildResult{}, ErrRecoveryConflict
	default:
		return RecoveryRebuildResult{}, fmt.Errorf("unexpected recovery rebuild lua result %q", res)
	}
}

// IsRebuildIdempotencyDone reports whether the atomic recovery completion marker exists.
func (s *RedisProjectionStore) IsRebuildIdempotencyDone(ctx context.Context, roomID domain.RoomID, identity string) (bool, error) {
	if s == nil || s.rdb == nil {
		return false, fmt.Errorf("nil store")
	}
	if err := ValidateRoomID(roomID); err != nil {
		return false, err
	}
	identity = strings.TrimSpace(identity)
	if identity == "" {
		return false, fmt.Errorf("empty rebuild idempotency identity")
	}
	n, err := s.rdb.Exists(ctx, s.keys.RebuildDone(roomID, identity)).Result()
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

// ReleaseKafkaAggregateQuarantine clears an active quarantine with an allowlisted note.
// Production recovery must release via RecoveryRebuildFromSnapshot's atomic Lua path;
// this standalone API exists for tests and exceptional ops tooling.
func (s *RedisProjectionStore) ReleaseKafkaAggregateQuarantine(
	ctx context.Context,
	roomID domain.RoomID,
	releaseNote string,
) error {
	if s == nil || s.rdb == nil {
		return fmt.Errorf("nil store")
	}
	if err := ValidateRoomID(roomID); err != nil {
		return err
	}
	if err := ValidateQuarantineReleaseNote(releaseNote); err != nil {
		return err
	}
	ts := time.Now().UTC().Format(time.RFC3339Nano)
	_, err := kafkaQuarantineReleaseScript.Run(ctx, s.rdb, []string{s.keys.KafkaQuarantine(roomID)},
		strings.TrimSpace(releaseNote), ts,
	).Text()
	return err
}

// KafkaQuarantineFields returns the quarantine hash for audit/tests.
func (s *RedisProjectionStore) KafkaQuarantineFields(ctx context.Context, roomID domain.RoomID) (map[string]string, error) {
	if err := ValidateRoomID(roomID); err != nil {
		return nil, err
	}
	return s.rdb.HGetAll(ctx, s.keys.KafkaQuarantine(roomID)).Result()
}

// CurrentFence returns the durable generation/sequence used as a recovery CAS fence.
func (s *RedisProjectionStore) CurrentFence(ctx context.Context, roomID domain.RoomID) (gen string, seq uint64, err error) {
	load, err := s.loadRoom(ctx, roomID)
	if err != nil {
		return "", 0, err
	}
	gen = load.generation
	if gen == "" {
		gen = defaultGeneration
	}
	return gen, load.sequence, nil
}
