package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/kurrent-io/KurrentDB-Client-Go/kurrentdb"

	"unoarena/services/game-integrity/domain"
)

const (
	kurrentOpTimeout         = 8 * time.Second
	gameBindEventType        = "gi.gamebind.v1"
	gameBindClaimEventType   = "gi.gamebind.claim.v1"
	gameBindReleaseEventType = "gi.gamebind.release.v1"
	gameBindPhaseClaimed     = "claimed"
	gameBindPhaseActive      = "active"
	readinessSentinelStream  = "gi.envelope.readiness.sentinel.v1"
	readinessSentinelType    = "gi.envelope.readiness.v1"
	streamLockStripeCount    = 64
	bindingRepairMaxAttempts = 8
)

var (
	errInjectedDeckAppend          = errors.New("injected deck append failure")
	errInjectedUncertainDeckAppend = errors.New("injected uncertain deck append failure")
	errCommittedFirstEventConflict = errors.New("committed first event conflicts with candidate")
)

var eventUUIDNamespace = uuid.MustParse("6ba7b810-9dad-11d1-80b4-00c04fd430c8") // NameSpaceURL

// KurrentStreamRepository is the durable StreamRepository backed by KurrentDB 26.
type KurrentStreamRepository struct {
	client          *kurrentdb.Client
	provider        KeyProvider
	pageSize        uint64
	readinessStream string
	mu              sync.Mutex
	dekCache        map[string]cachedStreamDEK // stream -> committed DEK + wrap identity
	roomStripes     [streamLockStripeCount]sync.Mutex
	deckStripes     [streamLockStripeCount]sync.Mutex
	// failNextBindingAppend is a test seam that simulates binding append failure once.
	failNextBindingAppend bool
	// failBindingAppendsRemaining fails the next N binding appends (claim/finalize).
	failBindingAppendsRemaining int
	// failNextDeckAppend is a test seam that simulates a known first deck snapshot append failure once.
	failNextDeckAppend bool
	// failNextDeckAppendUncertain simulates an uncertain first-append outcome once.
	// When uncertainAfterWrite is also set, the append is performed before returning the error.
	failNextDeckAppendUncertain   bool
	uncertainDeckAppendAfterWrite bool
	// unitLoadDeckState / unitClaimGameBinding are optional test doubles for claim-lifecycle unit tests.
	unitLoadDeckState    func(ctx context.Context, roomID domain.RoomID, gameID domain.GameID, stream string) (*DeckState, []*kurrentdb.ResolvedEvent, error)
	unitClaimGameBinding func(ctx context.Context, gameID domain.GameID, roomID domain.RoomID) (string, error)
}

type cachedStreamDEK struct {
	dek        []byte
	keyVersion int
	wrapped    string
	wrapNonce  string
}

const defaultReadPageSize uint64 = 512

// NewKurrentStreamRepository constructs a durable repository.
func NewKurrentStreamRepository(client *kurrentdb.Client, provider KeyProvider) *KurrentStreamRepository {
	return NewKurrentStreamRepositoryWithPageSize(client, provider, defaultReadPageSize)
}

// NewKurrentStreamRepositoryWithPageSize constructs a durable repository with an injectable read page size.
func NewKurrentStreamRepositoryWithPageSize(client *kurrentdb.Client, provider KeyProvider, pageSize uint64) *KurrentStreamRepository {
	if pageSize == 0 {
		pageSize = defaultReadPageSize
	}
	return &KurrentStreamRepository{
		client:          client,
		provider:        provider,
		pageSize:        pageSize,
		readinessStream: resolveReadinessStreamName(),
		dekCache:        map[string]cachedStreamDEK{},
	}
}

func resolveReadinessStreamName() string {
	base := readinessSentinelStream
	suffix := strings.TrimSpace(os.Getenv("GAME_INTEGRITY_READINESS_STREAM_SUFFIX"))
	if suffix == "" {
		return base
	}
	env := strings.ToLower(resolveDeploymentEnv())
	if env != "test" && env != "local" {
		return base
	}
	cleaned := make([]rune, 0, len(suffix))
	for _, r := range suffix {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' {
			cleaned = append(cleaned, r)
		}
	}
	if len(cleaned) == 0 {
		return base
	}
	return base + "." + string(cleaned)
}

func stripeIndex(key string) int {
	var h uint32 = 2166136261
	for i := 0; i < len(key); i++ {
		h ^= uint32(key[i])
		h *= 16777619
	}
	return int(h % streamLockStripeCount)
}

func (r *KurrentStreamRepository) lockRoom(id domain.RoomID) *sync.Mutex {
	return &r.roomStripes[stripeIndex(string(id))]
}

func (r *KurrentStreamRepository) lockDeck(k deckKey) *sync.Mutex {
	return &r.deckStripes[stripeIndex(string(k.RoomID)+"\x00"+string(k.GameID))]
}

// Close closes the underlying Kurrent client.
func (r *KurrentStreamRepository) Close() error {
	if r == nil || r.client == nil {
		return nil
	}
	return r.client.Close()
}

// Ready probes Kurrent with a bounded read, runs the key-provider ready check,
// and decrypts a fixed encrypted readiness sentinel (create-once if absent).
func (r *KurrentStreamRepository) Ready(ctx context.Context) error {
	if r == nil || r.client == nil || r.provider == nil {
		return errors.New("kurrent repository unwired")
	}
	if err := r.provider.Ready(ctx); err != nil {
		return fmt.Errorf("envelope provider: %w", err)
	}
	if err := r.ensureReadinessSentinel(ctx); err != nil {
		return fmt.Errorf("readiness sentinel: %w", err)
	}
	probeCtx, cancel := context.WithTimeout(ctx, kurrentOpTimeout)
	defer cancel()
	stream, err := r.client.ReadStream(probeCtx, "$scavenging-info", kurrentdb.ReadStreamOptions{
		Direction: kurrentdb.Forwards,
		From:      kurrentdb.Start{},
	}, 1)
	if err != nil {
		if isStreamNotFound(err) {
			return nil
		}
		// Fallback probe: attempt reading a non-existent app stream — not-found still proves connectivity.
		stream2, err2 := r.client.ReadStream(probeCtx, "gi.ready.probe."+uuid.NewString(), kurrentdb.ReadStreamOptions{
			Direction: kurrentdb.Forwards,
			From:      kurrentdb.Start{},
		}, 1)
		if err2 != nil {
			if isStreamNotFound(err2) {
				return nil
			}
			return fmt.Errorf("kurrent probe: %w", err2)
		}
		defer stream2.Close()
		_, recvErr := stream2.Recv()
		if recvErr != nil && !isStreamNotFound(recvErr) && !errors.Is(recvErr, io.EOF) {
			return fmt.Errorf("kurrent probe: %w", recvErr)
		}
		return nil
	}
	defer stream.Close()
	_, recvErr := stream.Recv()
	if recvErr != nil && !isStreamNotFound(recvErr) && !errors.Is(recvErr, io.EOF) {
		return fmt.Errorf("kurrent probe: %w", recvErr)
	}
	return nil
}

func (r *KurrentStreamRepository) ensureReadinessSentinel(ctx context.Context) error {
	opCtx, cancel := context.WithTimeout(ctx, kurrentOpTimeout)
	defer cancel()
	stream := r.readinessStream
	if stream == "" {
		stream = readinessSentinelStream
	}
	events, err := r.readAllEvents(opCtx, stream)
	if err != nil {
		return err
	}
	if len(events) == 0 {
		plain := map[string]string{"purpose": "envelope-readiness-sentinel", "v": "1"}
		err = r.appendEncryptedJSON(opCtx, stream, "", "", "readiness-sentinel-v1", readinessSentinelType, plain, 1, kurrentdb.NoStream{})
		if err != nil && !isRevisionConflict(err) {
			return err
		}
		events, err = r.readAllEvents(opCtx, stream)
		if err != nil {
			return err
		}
		if len(events) == 0 {
			return errors.New("readiness sentinel missing after create")
		}
	}
	_, _, err = r.decryptEvent(opCtx, stream, 0, events[0])
	return err
}

// WithRoom implements StreamRepository.
func (r *KurrentStreamRepository) WithRoom(ctx context.Context, roomID domain.RoomID, fn func(*RoomState) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !roomID.Valid() {
		return fmt.Errorf("roomId required")
	}
	lk := r.lockRoom(roomID)
	lk.Lock()
	defer lk.Unlock()
	return r.withRoomAttempt(ctx, roomID, fn, true)
}

func (r *KurrentStreamRepository) withRoomAttempt(ctx context.Context, roomID domain.RoomID, fn func(*RoomState) error, allowRetry bool) error {
	stream := roomStreamName(roomID)
	state, events, err := r.loadRoomState(ctx, roomID, stream)
	if err != nil {
		return err
	}
	before := state.Log.Len()
	if err := fn(state); err != nil {
		return err
	}
	after := state.Log.Len()
	if after == before {
		return nil
	}
	if after != before+1 {
		return fmt.Errorf("WithRoom callback must append at most one log entry")
	}
	entry := state.Log.Entries()[after-1]
	expected := expectedStreamState(len(events))
	err = r.appendRoomEntry(ctx, stream, roomID, entry, uint64(after), expected)
	if err != nil && allowRetry && (isRevisionConflict(err) || errors.Is(err, errCommittedFirstEventConflict)) {
		return r.withRoomAttempt(ctx, roomID, fn, false)
	}
	return err
}

// WithExistingRoom implements StreamRepository (read-only).
func (r *KurrentStreamRepository) WithExistingRoom(ctx context.Context, roomID domain.RoomID, fn func(*RoomState) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !roomID.Valid() {
		return fmt.Errorf("roomId required")
	}
	lk := r.lockRoom(roomID)
	lk.Lock()
	defer lk.Unlock()
	stream := roomStreamName(roomID)
	state, events, err := r.loadRoomState(ctx, roomID, stream)
	if err != nil {
		return err
	}
	if len(events) == 0 {
		return ErrStreamNotFound
	}
	return fn(state)
}

// WithDeck implements StreamRepository.
func (r *KurrentStreamRepository) WithDeck(ctx context.Context, roomID domain.RoomID, gameID domain.GameID, create bool, fn func(*DeckState) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !roomID.Valid() || !gameID.Valid() {
		return fmt.Errorf("roomId and gameId required")
	}
	lk := r.lockDeck(deckKey{RoomID: roomID, GameID: gameID})
	lk.Lock()
	defer lk.Unlock()
	return r.withDeckAttempt(ctx, roomID, gameID, create, fn, "", true)
}

func (r *KurrentStreamRepository) withDeckAttempt(ctx context.Context, roomID domain.RoomID, gameID domain.GameID, create bool, fn func(*DeckState) error, ownerToken string, allowRetry bool) error {
	stream := deckStreamName(roomID, gameID)
	state, events, err := r.loadDeckState(ctx, roomID, gameID, stream)
	if err != nil {
		return err
	}
	if !create && len(events) == 0 && state.Deck == nil {
		return ErrStreamNotFound
	}
	firstPersist := len(events) == 0
	if !firstPersist && state.Deck != nil {
		// Existing deck: fail-closed on conflicting bindings; repair missing/active.
		if err := r.ensureGameBinding(ctx, gameID, roomID); err != nil {
			return err
		}
	}
	before, err := snapshotFromDeckState(roomID, gameID, state)
	if err != nil {
		return err
	}
	if err := fn(state); err != nil {
		return err
	}
	after, err := snapshotFromDeckState(roomID, gameID, state)
	if err != nil {
		return err
	}
	if deckSnapshotsEqual(before, after) {
		if state.Deck != nil {
			return r.ensureGameBinding(ctx, gameID, roomID)
		}
		return nil
	}
	expected, domainRev := appendExpectation(events)
	eventID := fmt.Sprintf("deck-snap-%s-%s-%d", roomID, gameID, domainRev)
	if firstPersist {
		// Acquire or revalidate the owner-token claim only immediately before first durable append.
		// Callback/snapshot failure and no-op mutation must never leave a stranded claim.
		if ownerToken == "" {
			token, claimErr := r.claimGameBinding(ctx, gameID, roomID)
			if claimErr != nil {
				return claimErr
			}
			ownerToken = token
		} else if err := r.revalidateClaimOwnership(ctx, gameID, roomID, ownerToken); err != nil {
			return err
		}
		r.mu.Lock()
		failKnown := r.failNextDeckAppend
		if failKnown {
			r.failNextDeckAppend = false
		}
		failUncertain := r.failNextDeckAppendUncertain
		uncertainAfterWrite := r.uncertainDeckAppendAfterWrite
		if failUncertain {
			r.failNextDeckAppendUncertain = false
			r.uncertainDeckAppendAfterWrite = false
		}
		r.mu.Unlock()
		if failKnown {
			_ = r.releaseGameBinding(ctx, gameID, roomID, ownerToken)
			return errInjectedDeckAppend
		}
		if failUncertain && !uncertainAfterWrite {
			return r.recoverFirstDeckAppendError(ctx, gameID, roomID, ownerToken, errInjectedUncertainDeckAppend)
		}
		err = r.appendEncryptedJSON(ctx, stream, roomID, gameID, eventID, deckSnapshotEventType, after, domainRev, expected)
		if failUncertain && uncertainAfterWrite {
			if err != nil {
				return r.recoverFirstDeckAppendError(ctx, gameID, roomID, ownerToken, err)
			}
			return r.recoverFirstDeckAppendError(ctx, gameID, roomID, ownerToken, errInjectedUncertainDeckAppend)
		}
		if err != nil && allowRetry && (isRevisionConflict(err) || errors.Is(err, errCommittedFirstEventConflict)) {
			return r.withDeckAttempt(ctx, roomID, gameID, create, fn, ownerToken, false)
		}
		if err != nil {
			return r.recoverFirstDeckAppendError(ctx, gameID, roomID, ownerToken, err)
		}
		return r.finalizeGameBinding(ctx, gameID, roomID, ownerToken)
	}
	err = r.appendEncryptedJSON(ctx, stream, roomID, gameID, eventID, deckSnapshotEventType, after, domainRev, expected)
	if err != nil && allowRetry && (isRevisionConflict(err) || errors.Is(err, errCommittedFirstEventConflict)) {
		return r.withDeckAttempt(ctx, roomID, gameID, create, fn, ownerToken, false)
	}
	if err != nil {
		return err
	}
	return r.ensureGameBinding(ctx, gameID, roomID)
}

// recoverFirstDeckAppendError applies known vs uncertain first-append recovery.
func (r *KurrentStreamRepository) recoverFirstDeckAppendError(ctx context.Context, gameID domain.GameID, roomID domain.RoomID, ownerToken string, appendErr error) error {
	known := isKnownDeckAppendFailure(appendErr)
	deckExists := false
	if !known {
		exists, err := r.deckStreamExists(ctx, roomID, gameID)
		if err != nil {
			// Cannot classify outcome; do not steal by releasing blindly without existence check.
			return appendErr
		}
		deckExists = exists
	}
	switch evaluateAppendRecovery(known, deckExists) {
	case recoveryFinalize:
		if err := r.finalizeGameBinding(ctx, gameID, roomID, ownerToken); err != nil {
			// Repair path if token finalize races: matching claimed room with existing deck.
			if repairErr := r.ensureGameBinding(ctx, gameID, roomID); repairErr != nil {
				return appendErr
			}
		}
		return appendErr
	default:
		_ = r.releaseGameBinding(ctx, gameID, roomID, ownerToken)
		return appendErr
	}
}

func isKnownDeckAppendFailure(err error) bool {
	return errors.Is(err, errInjectedDeckAppend)
}

// WithExistingDeck implements StreamRepository.
func (r *KurrentStreamRepository) WithExistingDeck(ctx context.Context, roomID domain.RoomID, gameID domain.GameID, fn func(*DeckState) error) error {
	return r.WithDeck(ctx, roomID, gameID, false, fn)
}

// FindByGameID implements StreamRepository.
func (r *KurrentStreamRepository) FindByGameID(ctx context.Context, gameID domain.GameID) (domain.RoomID, bool, error) {
	if !gameID.Valid() {
		return "", false, nil
	}
	opCtx, cancel := context.WithTimeout(ctx, kurrentOpTimeout)
	defer cancel()
	stream := gameBindStreamName(gameID)
	events, err := r.readAllEvents(opCtx, stream)
	if err != nil {
		return "", false, err
	}
	st, err := bindingStateFromEvents(gameID, events)
	if err != nil {
		return "", false, err
	}
	roomID, ok := st.BoundRoom()
	if !ok {
		return "", false, nil
	}
	return domain.RoomID(roomID), true, nil
}

type gameBindingRecord struct {
	RoomID     string `json:"roomId"`
	GameID     string `json:"gameId"`
	Phase      string `json:"phase,omitempty"`
	OwnerToken string `json:"ownerToken,omitempty"`
}

func bindingPhase(rec gameBindingRecord) string {
	if rec.Phase == "" {
		return gameBindPhaseActive // legacy plaintext binds are active/immutable
	}
	return rec.Phase
}

func bindingStateFromEvents(gameID domain.GameID, events []*kurrentdb.ResolvedEvent) (bindingState, error) {
	records := make([]gameBindingRecord, 0, len(events))
	for _, ev := range events {
		var body gameBindingRecord
		if err := json.Unmarshal(ev.Event.Data, &body); err != nil {
			return bindingState{}, err
		}
		records = append(records, body)
	}
	return foldBindingRecords(gameID, records)
}

func (r *KurrentStreamRepository) deckStreamExists(ctx context.Context, roomID domain.RoomID, gameID domain.GameID) (bool, error) {
	opCtx, cancel := context.WithTimeout(ctx, kurrentOpTimeout)
	defer cancel()
	first, err := r.readFirstEvent(opCtx, deckStreamName(roomID, gameID))
	if err != nil {
		return false, err
	}
	return first != nil, nil
}

func newClaimOwnerToken() (string, error) {
	buf := make([]byte, 16)
	if _, err := io.ReadFull(rand.Reader, buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf), nil
}

func bindingExpectedRevision(st bindingState) kurrentdb.StreamState {
	if st.EventCount == 0 {
		return kurrentdb.NoStream{}
	}
	return kurrentdb.StreamRevision{Value: uint64(st.EventCount - 1)}
}

func (r *KurrentStreamRepository) appendBindingEvent(ctx context.Context, gameID domain.GameID, roomID domain.RoomID, phase, ownerToken string, expected kurrentdb.StreamState, eventKey string) error {
	r.mu.Lock()
	fail := r.failNextBindingAppend
	if fail {
		r.failNextBindingAppend = false
	}
	if !fail && r.failBindingAppendsRemaining > 0 {
		r.failBindingAppendsRemaining--
		if r.failBindingAppendsRemaining == 0 {
			fail = true
		}
	}
	r.mu.Unlock()
	if fail {
		return errors.New("injected binding append failure")
	}
	body, err := json.Marshal(gameBindingRecord{
		RoomID: string(roomID), GameID: string(gameID), Phase: phase, OwnerToken: ownerToken,
	})
	if err != nil {
		return err
	}
	eventType := gameBindClaimEventType
	switch phase {
	case gameBindPhaseActive:
		eventType = gameBindEventType
	case gameBindPhaseReleased:
		eventType = gameBindReleaseEventType
	}
	stream := gameBindStreamName(gameID)
	event := kurrentdb.EventData{
		EventID:     deterministicEventUUID(stream, eventKey),
		EventType:   eventType,
		ContentType: kurrentdb.ContentTypeJson,
		Data:        body,
		Metadata:    []byte(`{"readable":true}`),
	}
	opCtx, cancel := context.WithTimeout(ctx, kurrentOpTimeout)
	defer cancel()
	_, err = r.client.AppendToStream(opCtx, stream, kurrentdb.AppendToStreamOptions{
		StreamState: expected,
	}, event)
	return err
}

func (r *KurrentStreamRepository) loadBindingState(ctx context.Context, gameID domain.GameID) (bindingState, error) {
	opCtx, cancel := context.WithTimeout(ctx, kurrentOpTimeout)
	defer cancel()
	events, err := r.readAllEvents(opCtx, gameBindStreamName(gameID))
	cancel()
	if err != nil {
		return bindingState{}, err
	}
	return bindingStateFromEvents(gameID, events)
}

// claimGameBinding establishes a revision-checked owner-token claim immediately before first deck persist.
// released/empty → claimed(ownerToken, room). In-flight claims are not stealable by foreign or
// same-room callers without the exact owner token. Active bindings are immutable.
func (r *KurrentStreamRepository) claimGameBinding(ctx context.Context, gameID domain.GameID, roomID domain.RoomID) (string, error) {
	if r.unitClaimGameBinding != nil {
		return r.unitClaimGameBinding(ctx, gameID, roomID)
	}
	ownerToken, err := newClaimOwnerToken()
	if err != nil {
		return "", err
	}
	for attempt := 0; attempt < bindingRepairMaxAttempts; attempt++ {
		st, err := r.loadBindingState(ctx, gameID)
		if err != nil {
			return "", err
		}
		switch evaluateClaim(st, string(roomID), ownerToken) {
		case claimOwned, claimActiveOK:
			return ownerToken, nil
		case claimConflict:
			return "", fmt.Errorf("conflicting gameId binding")
		case claimAppend:
			expected := bindingExpectedRevision(st)
			err = r.appendBindingEvent(ctx, gameID, roomID, gameBindPhaseClaimed, ownerToken, expected,
				fmt.Sprintf("claim:%s:%s:%s:%d", gameID, roomID, ownerToken, st.EventCount))
			if err == nil {
				return ownerToken, nil
			}
			if isRevisionConflict(err) {
				continue
			}
			return "", err
		default:
			return "", fmt.Errorf("conflicting gameId binding")
		}
	}
	return "", fmt.Errorf("game binding claim exhausted after %d attempts", bindingRepairMaxAttempts)
}

func (r *KurrentStreamRepository) revalidateClaimOwnership(ctx context.Context, gameID domain.GameID, roomID domain.RoomID, ownerToken string) error {
	st, err := r.loadBindingState(ctx, gameID)
	if err != nil {
		return err
	}
	switch evaluateClaim(st, string(roomID), ownerToken) {
	case claimOwned, claimActiveOK:
		return nil
	default:
		return fmt.Errorf("conflicting gameId binding")
	}
}

func (r *KurrentStreamRepository) finalizeGameBinding(ctx context.Context, gameID domain.GameID, roomID domain.RoomID, ownerToken string) error {
	for attempt := 0; attempt < bindingRepairMaxAttempts; attempt++ {
		st, err := r.loadBindingState(ctx, gameID)
		if err != nil {
			return err
		}
		switch evaluateFinalize(st, string(roomID), ownerToken) {
		case finalizeAlreadyActive:
			return nil
		case finalizeConflict, finalizeNotClaimed:
			return fmt.Errorf("conflicting gameId binding")
		case finalizeAppend:
			expected := bindingExpectedRevision(st)
			err = r.appendBindingEvent(ctx, gameID, roomID, gameBindPhaseActive, ownerToken, expected,
				fmt.Sprintf("active:%s:%s:%s:%d", gameID, roomID, ownerToken, st.EventCount))
			if err == nil {
				return nil
			}
			if isRevisionConflict(err) {
				continue
			}
			return err
		default:
			return fmt.Errorf("conflicting gameId binding")
		}
	}
	return fmt.Errorf("game binding finalize exhausted after %d attempts", bindingRepairMaxAttempts)
}

func (r *KurrentStreamRepository) releaseGameBinding(ctx context.Context, gameID domain.GameID, roomID domain.RoomID, ownerToken string) error {
	for attempt := 0; attempt < bindingRepairMaxAttempts; attempt++ {
		st, err := r.loadBindingState(ctx, gameID)
		if err != nil {
			return err
		}
		switch evaluateRelease(st, ownerToken) {
		case releaseNoop:
			return nil
		case releaseAppend:
			expected := bindingExpectedRevision(st)
			err = r.appendBindingEvent(ctx, gameID, roomID, gameBindPhaseReleased, ownerToken, expected,
				fmt.Sprintf("release:%s:%s:%s:%d", gameID, roomID, ownerToken, st.EventCount))
			if err == nil {
				return nil
			}
			if isRevisionConflict(err) {
				continue
			}
			return err
		default:
			return nil
		}
	}
	return fmt.Errorf("game binding release exhausted after %d attempts", bindingRepairMaxAttempts)
}

func (r *KurrentStreamRepository) ensureGameBinding(ctx context.Context, gameID domain.GameID, roomID domain.RoomID) error {
	// Repair/read path for existing decks: claim if missing/released, finalize claimed matching room.
	for attempt := 0; attempt < bindingRepairMaxAttempts; attempt++ {
		st, err := r.loadBindingState(ctx, gameID)
		if err != nil {
			return err
		}
		switch evaluateFinalizeRepair(st, string(roomID)) {
		case finalizeAlreadyActive:
			return nil
		case finalizeAppend:
			expected := bindingExpectedRevision(st)
			token := st.OwnerToken
			err = r.appendBindingEvent(ctx, gameID, roomID, gameBindPhaseActive, token, expected,
				fmt.Sprintf("active-repair:%s:%s:%d", gameID, roomID, st.EventCount))
			if err == nil {
				return nil
			}
			if isRevisionConflict(err) {
				continue
			}
			return err
		case finalizeNotClaimed:
			token, err := r.claimGameBinding(ctx, gameID, roomID)
			if err != nil {
				return err
			}
			if err := r.finalizeGameBinding(ctx, gameID, roomID, token); err != nil {
				return err
			}
			return nil
		default:
			return fmt.Errorf("conflicting gameId binding")
		}
	}
	return fmt.Errorf("game binding ensure exhausted after %d attempts", bindingRepairMaxAttempts)
}

type roomPlaintextV1 struct {
	EventID   string          `json:"eventId"`
	EventType string          `json:"eventType"`
	GameID    string          `json:"gameId,omitempty"`
	Payload   json.RawMessage `json:"payload"`
}

func (r *KurrentStreamRepository) loadRoomState(ctx context.Context, roomID domain.RoomID, stream string) (*RoomState, []*kurrentdb.ResolvedEvent, error) {
	opCtx, cancel := context.WithTimeout(ctx, kurrentOpTimeout)
	defer cancel()
	events, err := r.readAllEvents(opCtx, stream)
	if err != nil {
		return nil, nil, err
	}
	entries := make([]domain.GameLogEntry, 0, len(events))
	for i, ev := range events {
		entry, err := r.decryptRoomEntry(opCtx, stream, roomID, uint64(i), ev)
		if err != nil {
			return nil, nil, err
		}
		entries = append(entries, entry)
	}
	log, err := domain.RestoreGameLog(roomID, entries)
	if err != nil {
		return nil, nil, err
	}
	return &RoomState{Log: log}, events, nil
}

func (r *KurrentStreamRepository) decryptRoomEntry(ctx context.Context, stream string, roomID domain.RoomID, kurrentRev uint64, ev *kurrentdb.ResolvedEvent) (domain.GameLogEntry, error) {
	if stream != roomStreamName(roomID) {
		return domain.GameLogEntry{}, errors.New("room stream name mismatch")
	}
	plain, meta, err := r.decryptEventForAggregate(ctx, stream, kurrentRev, ev, string(roomID), "")
	if err != nil {
		return domain.GameLogEntry{}, err
	}
	var rec roomPlaintextV1
	if err := json.Unmarshal(plain, &rec); err != nil {
		return domain.GameLogEntry{}, fmt.Errorf("room plaintext: %w", err)
	}
	if rec.EventID != meta.OriginalEventID || rec.EventType != meta.OriginalEventType {
		return domain.GameLogEntry{}, errors.New("room plaintext/metadata mismatch")
	}
	if meta.RoomID != "" {
		// room stream name encodes room; plaintext gameId must match metadata when set
		if meta.GameID != "" && rec.GameID != "" && meta.GameID != rec.GameID {
			return domain.GameLogEntry{}, errors.New("room plaintext gameId mismatch")
		}
	}
	payload := []byte(rec.Payload)
	if len(payload) == 0 || bytesEqualJSONNull(payload) {
		payload = nil
	}
	return domain.GameLogEntry{
		Offset:    domain.LogOffset(kurrentRev),
		EventID:   domain.EventID(rec.EventID),
		EventType: rec.EventType,
		GameID:    domain.GameID(rec.GameID),
		Payload:   payload,
	}, nil
}

func bytesEqualJSONNull(b []byte) bool {
	return string(b) == "null"
}

func (r *KurrentStreamRepository) appendRoomEntry(ctx context.Context, stream string, roomID domain.RoomID, entry domain.GameLogEntry, domainRev uint64, expected kurrentdb.StreamState) error {
	payload := json.RawMessage(entry.Payload)
	if len(payload) == 0 {
		payload = json.RawMessage("null")
	} else if !json.Valid(payload) {
		b, err := json.Marshal(string(entry.Payload))
		if err != nil {
			return err
		}
		payload = b
	}
	rec := roomPlaintextV1{
		EventID:   string(entry.EventID),
		EventType: entry.EventType,
		GameID:    string(entry.GameID),
		Payload:   payload,
	}
	return r.appendEncryptedJSON(ctx, stream, roomID, entry.GameID, string(entry.EventID), entry.EventType, rec, domainRev, expected)
}

func (r *KurrentStreamRepository) loadDeckState(ctx context.Context, roomID domain.RoomID, gameID domain.GameID, stream string) (*DeckState, []*kurrentdb.ResolvedEvent, error) {
	if r.unitLoadDeckState != nil {
		return r.unitLoadDeckState(ctx, roomID, gameID, stream)
	}
	opCtx, cancel := context.WithTimeout(ctx, kurrentOpTimeout)
	defer cancel()
	first, err := r.readFirstEvent(opCtx, stream)
	if err != nil {
		return nil, nil, err
	}
	if first == nil {
		return emptyDeckState(), nil, nil
	}
	latest, err := r.readLatestEvent(opCtx, stream)
	if err != nil {
		return nil, nil, err
	}
	if latest == nil {
		return nil, nil, errors.New("deck stream lost latest event after first read")
	}
	firstMeta, err := parseEnvelopeMetadata(first.Event.UserMetadata)
	if err != nil {
		return nil, nil, err
	}
	latestMeta, err := parseEnvelopeMetadata(latest.Event.UserMetadata)
	if err != nil {
		return nil, nil, err
	}
	if firstMeta.KeyVersion != latestMeta.KeyVersion ||
		firstMeta.WrappedDEK != latestMeta.WrappedDEK ||
		firstMeta.WrapNonce != latestMeta.WrapNonce {
		return nil, nil, errors.New("stream key identity changed across events")
	}
	// Anchor stream DEK cache to the first event's wrapper identity before decrypting latest.
	if _, err := r.cachedOrUnwrap(opCtx, stream, firstMeta); err != nil {
		return nil, nil, err
	}
	plain, meta, err := r.decryptEventForAggregate(opCtx, stream, latest.Event.EventNumber, latest, string(roomID), string(gameID))
	if err != nil {
		return nil, nil, err
	}
	var snap DeckStateSnapshotV1
	if err := json.Unmarshal(plain, &snap); err != nil {
		return nil, nil, fmt.Errorf("deck snapshot: %w", err)
	}
	if snap.RoomID != string(roomID) || snap.GameID != string(gameID) {
		return nil, nil, errors.New("deck snapshot identity mismatch")
	}
	if meta.RoomID != string(roomID) || meta.GameID != string(gameID) {
		return nil, nil, errors.New("deck envelope identity mismatch")
	}
	st, err := deckStateFromSnapshot(&snap)
	if err != nil {
		return nil, nil, err
	}
	return st, []*kurrentdb.ResolvedEvent{latest}, nil
}

func (r *KurrentStreamRepository) appendEncryptedJSON(ctx context.Context, stream string, roomID domain.RoomID, gameID domain.GameID, originalEventID, originalEventType string, body any, domainRev uint64, expected kurrentdb.StreamState) error {
	plain, err := json.Marshal(body)
	if err != nil {
		return err
	}
	firstWrite := false
	switch expected.(type) {
	case kurrentdb.NoStream:
		firstWrite = true
	}
	dek, wrapped, wrapNonce, keyVersion, err := r.streamDEK(ctx, stream, firstWrite)
	if err != nil {
		return err
	}
	kurrentRev := uint64(0)
	switch s := expected.(type) {
	case kurrentdb.NoStream:
		kurrentRev = 0
	case kurrentdb.StreamRevision:
		kurrentRev = s.Value + 1
	}
	payloadNonce := make([]byte, gcmNonceSize)
	if _, err := io.ReadFull(rand.Reader, payloadNonce); err != nil {
		return err
	}
	eventUUID := deterministicEventUUID(stream, originalEventID)
	meta := envelopeMetadataV1{
		EnvelopeVersion:   envelopeVersionV1,
		KeyVersion:        keyVersion,
		WrappedDEK:        hexBytes(wrapped),
		WrapNonce:         hexBytes(wrapNonce),
		PayloadNonce:      hexBytes(payloadNonce),
		OriginalEventID:   originalEventID,
		OriginalEventType: originalEventType,
		Stream:            stream,
		RoomID:            string(roomID),
		GameID:            string(gameID),
		KurrentRevision:   kurrentRev,
		DomainRevision:    domainRev,
		EventUUID:         eventUUID.String(),
	}
	aad := meta.canonicalAAD()
	ct, err := SealPayloadWithNonce(dek, aad, payloadNonce, plain)
	if err != nil {
		return err
	}
	metaBytes, err := meta.marshal()
	if err != nil {
		return err
	}
	event := kurrentdb.EventData{
		EventID:     eventUUID,
		EventType:   originalEventType,
		ContentType: kurrentdb.ContentTypeBinary,
		Data:        ct,
		Metadata:    metaBytes,
	}
	opCtx, cancel := context.WithTimeout(ctx, kurrentOpTimeout)
	defer cancel()
	_, err = r.client.AppendToStream(opCtx, stream, kurrentdb.AppendToStreamOptions{StreamState: expected}, event)
	if err != nil {
		// Candidate DEK lived only on the stack; append failure leaves no cache entry.
		return err
	}
	if firstWrite {
		// Kurrent may report success for an idempotent same-event-ID append even when
		// another process won the NoStream race with different encrypted bytes. Anchor
		// this repository to the key identity that actually reached the stream.
		anchorCtx, anchorCancel := context.WithTimeout(ctx, kurrentOpTimeout)
		defer anchorCancel()
		if err := r.anchorCommittedStreamDEK(anchorCtx, stream, keyVersion, dek, wrapped, wrapNonce, meta, plain); err != nil {
			return fmt.Errorf("anchor committed stream DEK: %w", err)
		}
	}
	return nil
}

func (r *KurrentStreamRepository) anchorCommittedStreamDEK(ctx context.Context, stream string, candidateKeyVersion int, candidateDEK, candidateWrapped, candidateWrapNonce []byte, candidateMeta envelopeMetadataV1, candidatePlain []byte) error {
	first, err := r.readFirstEvent(ctx, stream)
	if err != nil {
		return err
	}
	if first == nil {
		return errors.New("first event missing after successful append")
	}
	meta, err := parseEnvelopeMetadata(first.Event.UserMetadata)
	if err != nil {
		return err
	}
	if err := authenticateEnvelopeMetadata(meta, stream, 0, first); err != nil {
		return err
	}
	if meta.KeyVersion == candidateKeyVersion &&
		meta.WrappedDEK == hexBytes(candidateWrapped) &&
		meta.WrapNonce == hexBytes(candidateWrapNonce) {
		r.commitStreamDEK(stream, candidateKeyVersion, candidateDEK, candidateWrapped, candidateWrapNonce)
	} else {
		committedWrapped, err := meta.wrappedDEKBytes()
		if err != nil {
			return err
		}
		committedWrapNonce, err := meta.wrapNonceBytes()
		if err != nil {
			return err
		}
		committedDEK, err := r.provider.UnwrapDEK(ctx, meta.KeyVersion, committedWrapNonce, committedWrapped)
		if err != nil {
			return err
		}
		r.commitStreamDEK(stream, meta.KeyVersion, committedDEK, committedWrapped, committedWrapNonce)
	}

	// Kurrent may acknowledge a same-UUID append as idempotent even when this
	// candidate's encrypted bytes lost a cross-replica NoStream race. The DEK
	// wrapper alone proves decryptability, not idempotency: authenticate and
	// decrypt the event that actually committed, then compare its complete
	// logical identity and plaintext to the candidate.
	committedPlain, committedMeta, err := r.decryptEvent(ctx, stream, 0, first)
	if err != nil {
		return err
	}
	if !committedFirstEventMatchesCandidate(committedMeta, candidateMeta, committedPlain, candidatePlain) {
		return fmt.Errorf("%w: eventId=%s stream=%s", errCommittedFirstEventConflict, candidateMeta.OriginalEventID, stream)
	}
	return nil
}

func committedFirstEventMatchesCandidate(committed, candidate envelopeMetadataV1, committedPlain, candidatePlain []byte) bool {
	return committed.OriginalEventID == candidate.OriginalEventID &&
		committed.OriginalEventType == candidate.OriginalEventType &&
		committed.Stream == candidate.Stream &&
		committed.RoomID == candidate.RoomID &&
		committed.GameID == candidate.GameID &&
		committed.KurrentRevision == candidate.KurrentRevision &&
		committed.DomainRevision == candidate.DomainRevision &&
		committed.EventUUID == candidate.EventUUID &&
		bytes.Equal(committedPlain, candidatePlain)
}

func (r *KurrentStreamRepository) decryptEvent(ctx context.Context, stream string, kurrentRev uint64, ev *kurrentdb.ResolvedEvent) ([]byte, envelopeMetadataV1, error) {
	return r.decryptEventForAggregate(ctx, stream, kurrentRev, ev, "", "")
}

func (r *KurrentStreamRepository) decryptEventForAggregate(ctx context.Context, stream string, kurrentRev uint64, ev *kurrentdb.ResolvedEvent, expectRoomID, expectGameID string) ([]byte, envelopeMetadataV1, error) {
	if ev == nil || ev.Event == nil {
		return nil, envelopeMetadataV1{}, errors.New("missing recorded event")
	}
	meta, err := parseEnvelopeMetadata(ev.Event.UserMetadata)
	if err != nil {
		return nil, envelopeMetadataV1{}, err
	}
	noteDecryptKeyVersion(ctx, meta.KeyVersion)
	if expectRoomID == "" && expectGameID == "" {
		// Readiness / generic path: authenticate using metadata self-identity.
		if err := authenticateEnvelopeMetadata(meta, stream, kurrentRev, ev); err != nil {
			return nil, envelopeMetadataV1{}, err
		}
	} else {
		if err := authenticateEnvelopeMetadataForAggregate(meta, stream, kurrentRev, ev, expectRoomID, expectGameID); err != nil {
			return nil, envelopeMetadataV1{}, err
		}
	}
	payloadNonce, err := meta.payloadNonceBytes()
	if err != nil {
		return nil, envelopeMetadataV1{}, err
	}
	dek, err := r.cachedOrUnwrap(ctx, stream, meta)
	if err != nil {
		return nil, envelopeMetadataV1{}, err
	}
	plain, err := OpenPayload(dek, meta.canonicalAAD(), payloadNonce, ev.Event.Data)
	if err != nil {
		return nil, envelopeMetadataV1{}, fmt.Errorf("decrypt payload: %w", err)
	}
	return plain, meta, nil
}

func authenticateEnvelopeMetadata(meta envelopeMetadataV1, stream string, kurrentRev uint64, ev *kurrentdb.ResolvedEvent) error {
	return authenticateEnvelopeMetadataForAggregate(meta, stream, kurrentRev, ev, meta.RoomID, meta.GameID)
}

func authenticateEnvelopeMetadataForAggregate(meta envelopeMetadataV1, stream string, kurrentRev uint64, ev *kurrentdb.ResolvedEvent, expectRoomID, expectGameID string) error {
	if meta.EnvelopeVersion != envelopeVersionV1 {
		return fmt.Errorf("envelope version mismatch")
	}
	if meta.Stream != stream {
		return errors.New("envelope stream metadata mismatch")
	}
	if meta.KurrentRevision != kurrentRev {
		return fmt.Errorf("envelope revision metadata mismatch: meta=%d actual=%d", meta.KurrentRevision, kurrentRev)
	}
	// Domain revision is 1-based event ordinal (event number + 1).
	if meta.DomainRevision != kurrentRev+1 {
		return fmt.Errorf("envelope domainRevision mismatch: meta=%d want=%d", meta.DomainRevision, kurrentRev+1)
	}
	if meta.OriginalEventID == "" || meta.OriginalEventType == "" {
		return errors.New("envelope missing original event identity")
	}
	if meta.EventUUID == "" {
		return errors.New("envelope missing eventUuid")
	}
	if meta.KeyVersion <= 0 {
		return errors.New("envelope missing keyVersion")
	}
	if ev == nil || ev.Event == nil {
		return errors.New("missing recorded event")
	}
	if ev.Event.EventType != meta.OriginalEventType {
		return errors.New("envelope event type mismatch")
	}
	wantUUID := deterministicEventUUID(stream, meta.OriginalEventID)
	if meta.EventUUID != wantUUID.String() {
		return errors.New("envelope event UUID metadata mismatch")
	}
	if ev.Event.EventID != wantUUID {
		return errors.New("envelope event UUID mismatch against recorded event")
	}
	if ev.Event.EventNumber != kurrentRev {
		return fmt.Errorf("recorded event number mismatch: got %d want %d", ev.Event.EventNumber, kurrentRev)
	}
	if meta.WrappedDEK == "" || meta.WrapNonce == "" || meta.PayloadNonce == "" {
		return errors.New("envelope missing key material metadata")
	}
	if expectRoomID != "" && meta.RoomID != expectRoomID {
		return errors.New("envelope roomId mismatch")
	}
	if expectGameID != "" && meta.GameID != expectGameID {
		return errors.New("envelope gameId mismatch")
	}
	return nil
}

func (r *KurrentStreamRepository) streamDEK(ctx context.Context, stream string, firstWrite bool) (dek, wrapped, wrapNonce []byte, keyVersion int, err error) {
	r.mu.Lock()
	if cached, ok := r.dekCache[stream]; ok {
		cp := append([]byte(nil), cached.dek...)
		wrappedHex, wrapNonceHex := cached.wrapped, cached.wrapNonce
		kv := cached.keyVersion
		r.mu.Unlock()
		wb, err := hex.DecodeString(wrappedHex)
		if err != nil {
			return nil, nil, nil, 0, err
		}
		nb, err := hex.DecodeString(wrapNonceHex)
		if err != nil {
			return nil, nil, nil, 0, err
		}
		// Reuse identical wrapping metadata for every subsequent event on this stream.
		return cp, wb, nb, kv, nil
	}
	r.mu.Unlock()

	opCtx, cancel := context.WithTimeout(ctx, kurrentOpTimeout)
	defer cancel()
	events, err := r.readAllEvents(opCtx, stream)
	if err != nil {
		return nil, nil, nil, 0, err
	}
	if len(events) > 0 {
		meta, err := parseEnvelopeMetadata(events[0].Event.UserMetadata)
		if err != nil {
			return nil, nil, nil, 0, err
		}
		wrapped, err = meta.wrappedDEKBytes()
		if err != nil {
			return nil, nil, nil, 0, err
		}
		wrapNonce, err = meta.wrapNonceBytes()
		if err != nil {
			return nil, nil, nil, 0, err
		}
		dek, err = r.provider.UnwrapDEK(opCtx, meta.KeyVersion, wrapNonce, wrapped)
		if err != nil {
			return nil, nil, nil, 0, err
		}
		r.commitStreamDEK(stream, meta.KeyVersion, dek, wrapped, wrapNonce)
		return dek, wrapped, wrapNonce, meta.KeyVersion, nil
	}
	if !firstWrite {
		return nil, nil, nil, 0, errors.New("stream DEK unavailable for non-first write")
	}

	// Candidate DEK/wrap data lives only on the stack until successful first append.
	dek = make([]byte, dekSizeBytes)
	if _, err := io.ReadFull(rand.Reader, dek); err != nil {
		return nil, nil, nil, 0, err
	}
	wrapped, wrapNonce, err = r.provider.WrapDEK(ctx, dek)
	if err != nil {
		return nil, nil, nil, 0, err
	}
	return dek, wrapped, wrapNonce, r.provider.KeyVersion(), nil
}

func (r *KurrentStreamRepository) commitStreamDEK(stream string, keyVersion int, dek, wrapped, wrapNonce []byte) {
	r.mu.Lock()
	r.dekCache[stream] = cachedStreamDEK{
		dek: append([]byte(nil), dek...), keyVersion: keyVersion,
		wrapped: hexBytes(wrapped), wrapNonce: hexBytes(wrapNonce),
	}
	r.mu.Unlock()
}

func (r *KurrentStreamRepository) cachedOrUnwrap(ctx context.Context, stream string, meta envelopeMetadataV1) ([]byte, error) {
	r.mu.Lock()
	if cached, ok := r.dekCache[stream]; ok {
		if cached.keyVersion != meta.KeyVersion || cached.wrapped != meta.WrappedDEK || cached.wrapNonce != meta.WrapNonce {
			r.mu.Unlock()
			return nil, errors.New("stream key identity changed across events")
		}
		cp := append([]byte(nil), cached.dek...)
		r.mu.Unlock()
		return cp, nil
	}
	r.mu.Unlock()
	wrapped, err := meta.wrappedDEKBytes()
	if err != nil {
		return nil, err
	}
	wrapNonce, err := meta.wrapNonceBytes()
	if err != nil {
		return nil, err
	}
	dek, err := r.provider.UnwrapDEK(ctx, meta.KeyVersion, wrapNonce, wrapped)
	if err != nil {
		return nil, err
	}
	r.commitStreamDEK(stream, meta.KeyVersion, dek, wrapped, wrapNonce)
	return dek, nil
}

// cachedDEKCount is a test introspection helper.
func (r *KurrentStreamRepository) cachedDEKCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.dekCache)
}

func (r *KurrentStreamRepository) readAllEvents(ctx context.Context, stream string) ([]*kurrentdb.ResolvedEvent, error) {
	var out []*kurrentdb.ResolvedEvent
	var from kurrentdb.StreamPosition = kurrentdb.Start{}
	for {
		rs, err := r.client.ReadStream(ctx, stream, kurrentdb.ReadStreamOptions{
			Direction: kurrentdb.Forwards,
			From:      from,
		}, r.pageSize)
		if err != nil {
			if isStreamNotFound(err) {
				return out, nil
			}
			return nil, err
		}
		pageCount := 0
		var lastNum uint64
		for {
			ev, err := rs.Recv()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				rs.Close()
				if isStreamNotFound(err) {
					return out, nil
				}
				return nil, err
			}
			out = append(out, ev)
			pageCount++
			lastNum = ev.Event.EventNumber
		}
		rs.Close()
		if pageCount == 0 {
			break
		}
		if uint64(pageCount) < r.pageSize {
			break
		}
		from = kurrentdb.Revision(lastNum + 1)
	}
	return out, nil
}

func (r *KurrentStreamRepository) readFirstEvent(ctx context.Context, stream string) (*kurrentdb.ResolvedEvent, error) {
	return r.readOneEvent(ctx, stream, kurrentdb.Forwards, kurrentdb.Start{})
}

func (r *KurrentStreamRepository) readLatestEvent(ctx context.Context, stream string) (*kurrentdb.ResolvedEvent, error) {
	return r.readOneEvent(ctx, stream, kurrentdb.Backwards, kurrentdb.End{})
}

func (r *KurrentStreamRepository) readOneEvent(ctx context.Context, stream string, direction kurrentdb.Direction, from kurrentdb.StreamPosition) (*kurrentdb.ResolvedEvent, error) {
	rs, err := r.client.ReadStream(ctx, stream, kurrentdb.ReadStreamOptions{
		Direction: direction,
		From:      from,
	}, 1)
	if err != nil {
		if isStreamNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	defer rs.Close()
	ev, err := rs.Recv()
	if errors.Is(err, io.EOF) {
		return nil, nil
	}
	if err != nil {
		if isStreamNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	return ev, nil
}

func appendExpectation(events []*kurrentdb.ResolvedEvent) (kurrentdb.StreamState, uint64) {
	if len(events) == 0 {
		return kurrentdb.NoStream{}, 1
	}
	lastNum := events[len(events)-1].Event.EventNumber
	return kurrentdb.StreamRevision{Value: lastNum}, lastNum + 2
}

func expectedStreamState(existingCount int) kurrentdb.StreamState {
	if existingCount == 0 {
		return kurrentdb.NoStream{}
	}
	return kurrentdb.StreamRevision{Value: uint64(existingCount - 1)}
}

func isKurrentCode(err error, code kurrentdb.ErrorCode) bool {
	var esErr *kurrentdb.Error
	if errors.As(err, &esErr) {
		return esErr.Code() == code
	}
	// FromError wraps non-typed errors as Unknown; prefer direct type first.
	esErr2, isNil := kurrentdb.FromError(err)
	if isNil || esErr2 == nil {
		return false
	}
	return esErr2.Code() == code
}

func isStreamNotFound(err error) bool {
	return isKurrentCode(err, kurrentdb.ErrorCodeResourceNotFound) || isKurrentCode(err, kurrentdb.ErrorCodeStreamDeleted)
}

func isRevisionConflict(err error) bool {
	return isKurrentCode(err, kurrentdb.ErrorCodeWrongExpectedVersion) || isKurrentCode(err, kurrentdb.ErrorCodeStreamRevisionConflict)
}

func deterministicEventUUID(stream, originalEventID string) uuid.UUID {
	return uuid.NewSHA1(eventUUIDNamespace, []byte("gi.event.v1:"+stream+"\x00"+originalEventID))
}
