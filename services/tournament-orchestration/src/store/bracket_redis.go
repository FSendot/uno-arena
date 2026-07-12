package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"unoarena/services/tournament-orchestration/domain"
)

// ErrBracketProjectionUnavailable is returned when Redis projection ops fail
// or the live projection has not been rebuilt/refreshed yet (callers fall back to Postgres).
var ErrBracketProjectionUnavailable = errors.New("bracket projection unavailable")

// ErrBracketProjectionConflict is returned when an equal projectionVersion targets
// different summary/chunk JSON (fail-closed).
var ErrBracketProjectionConflict = errors.New("bracket projection conflict")

// RedisBracketStore is the non-authoritative chunked Redis bracket projection.
type RedisBracketStore struct {
	rdb  redis.UniversalClient
	keys BracketKeySpace
}

// NewRedisBracketStore wraps a Redis client for bracket projection I/O.
func NewRedisBracketStore(rdb redis.UniversalClient, keyPrefix string) *RedisBracketStore {
	return &RedisBracketStore{
		rdb:  rdb,
		keys: NewBracketKeySpace(keyPrefix),
	}
}

// Client exposes the underlying Redis client (tests/ops).
func (s *RedisBracketStore) Client() redis.UniversalClient { return s.rdb }

// KeyPrefix returns the configured key prefix.
func (s *RedisBracketStore) KeyPrefix() string { return s.keys.Prefix() }

// LoadScripts preloads Lua scripts (fail-closed wiring). go-redis Script.Run
// remains NOSCRIPT-safe and reloads on demand.
func (s *RedisBracketStore) LoadScripts(ctx context.Context) error {
	if s == nil || s.rdb == nil {
		return fmt.Errorf("%w: nil store", ErrBracketProjectionUnavailable)
	}
	for _, script := range []*redis.Script{
		upsertBracketSummaryScript,
		upsertBracketChunkScript,
		beginBracketRebuildScript,
		rebuildBracketSummaryScript,
		rebuildBracketChunkScript,
		cutoverBracketRebuildScript,
		abortBracketRebuildScript,
		pageBracketScript,
	} {
		if err := script.Load(ctx, s.rdb).Err(); err != nil {
			return wrapBracketUnavailable(err)
		}
	}
	return nil
}

// Ping checks Redis connectivity.
func (s *RedisBracketStore) Ping(ctx context.Context) error {
	if s == nil || s.rdb == nil {
		return fmt.Errorf("%w: nil store", ErrBracketProjectionUnavailable)
	}
	return wrapBracketUnavailable(PingRedis(ctx, s.rdb))
}

// FlushPrefixedKeys deletes only keys under this store's prefix (integration cleanup).
func (s *RedisBracketStore) FlushPrefixedKeys(ctx context.Context) error {
	if s == nil || s.rdb == nil {
		return fmt.Errorf("%w: nil store", ErrBracketProjectionUnavailable)
	}
	var cursor uint64
	pattern := s.keys.ScanPattern()
	for {
		keys, next, err := s.rdb.Scan(ctx, cursor, pattern, 200).Result()
		if err != nil {
			return wrapBracketUnavailable(err)
		}
		if len(keys) > 0 {
			if err := s.rdb.Del(ctx, keys...).Err(); err != nil {
				return wrapBracketUnavailable(err)
			}
		}
		cursor = next
		if cursor == 0 {
			return nil
		}
	}
}

// UpsertSummary writes compact summary JSON fenced by projectionVersion.
func (s *RedisBracketStore) UpsertSummary(
	ctx context.Context,
	tournamentID string,
	summary BracketSummary,
	projectionVersion int64,
	generatedAt time.Time,
) error {
	if strings.TrimSpace(tournamentID) == "" {
		return fmt.Errorf("tournamentId required")
	}
	if projectionVersion <= 0 {
		return fmt.Errorf("projectionVersion required")
	}
	if generatedAt.IsZero() {
		return fmt.Errorf("generatedAt required")
	}
	raw, err := json.Marshal(summary)
	if err != nil {
		return err
	}
	code, err := upsertBracketSummaryScript.Run(ctx, s.rdb,
		[]string{s.keys.Meta(tournamentID)},
		s.keys.TournamentRoot(tournamentID),
		string(raw),
		strconv.FormatInt(projectionVersion, 10),
		generatedAt.UTC().Format(time.RFC3339),
	).Int64()
	if err != nil {
		return wrapBracketUnavailable(err)
	}
	if code == 3 {
		return fmt.Errorf("%w: summary tournamentId=%s version=%d", ErrBracketProjectionConflict, tournamentID, projectionVersion)
	}
	return nil
}

// UpsertChunk writes one provisioning-batch chunk + index members fenced by projectionVersion.
func (s *RedisBracketStore) UpsertChunk(
	ctx context.Context,
	tournamentID string,
	roundNumber int,
	batchID string,
	slots []BracketSlotView,
	projectionVersion int64,
	generatedAt time.Time,
) error {
	if strings.TrimSpace(tournamentID) == "" || roundNumber < 1 || strings.TrimSpace(batchID) == "" {
		return fmt.Errorf("tournamentId, roundNumber, and batchId required")
	}
	if projectionVersion <= 0 {
		return fmt.Errorf("projectionVersion required")
	}
	if generatedAt.IsZero() {
		return fmt.Errorf("generatedAt required")
	}
	if len(slots) > domain.MaxProvisioningBatchSize {
		return fmt.Errorf("chunk exceeds max size %d", domain.MaxProvisioningBatchSize)
	}
	if slots == nil {
		slots = []BracketSlotView{}
	}
	raw, err := json.Marshal(slots)
	if err != nil {
		return err
	}
	args := make([]any, 0, 7+2*len(slots))
	args = append(args,
		s.keys.TournamentRoot(tournamentID),
		strconv.Itoa(roundNumber),
		batchID,
		string(raw),
		strconv.FormatInt(projectionVersion, 10),
		generatedAt.UTC().Format(time.RFC3339),
		strconv.Itoa(len(slots)),
	)
	for _, sl := range slots {
		args = append(args, BracketIndexScore(sl.RoundNumber, sl.SlotIndex), BracketIndexMember(sl.RoundNumber, sl.SlotIndex))
	}
	code, err := upsertBracketChunkScript.Run(ctx, s.rdb,
		[]string{s.keys.Meta(tournamentID)},
		args...,
	).Int64()
	if err != nil {
		return wrapBracketUnavailable(err)
	}
	if code == 3 {
		return fmt.Errorf("%w: chunk tournamentId=%s round=%d batch=%s version=%d",
			ErrBracketProjectionConflict, tournamentID, roundNumber, batchID, projectionVersion)
	}
	return nil
}

// Page returns a bounded live-keyset BracketPage from the Redis projection.
// Retries once on generation cutover races.
func (s *RedisBracketStore) Page(ctx context.Context, q BracketPageQuery) (BracketPage, error) {
	limit := q.Limit
	if limit <= 0 {
		limit = DefaultBracketPageLimit
	}
	if limit > MaxBracketPageLimit {
		return BracketPage{}, fmt.Errorf("%w: limit max %d", ErrInvalidBracketPageQuery, MaxBracketPageLimit)
	}
	if q.RoundNumber != nil && *q.RoundNumber < 1 {
		return BracketPage{}, fmt.Errorf("%w: roundNumber", ErrInvalidBracketPageQuery)
	}
	var after *BracketCursor
	if strings.TrimSpace(q.Cursor) != "" {
		c, err := DecodeBracketCursor(q.Cursor)
		if err != nil {
			return BracketPage{}, err
		}
		if q.RoundNumber != nil && c.RoundNumber != *q.RoundNumber {
			return BracketPage{}, fmt.Errorf("%w: cursor round mismatch", ErrInvalidBracketCursor)
		}
		after = &c
	}

	var lastErr error
	for attempt := 0; attempt < 2; attempt++ {
		page, retry, err := s.pageAtomic(ctx, q.TournamentID, q.RoundNumber, after, limit)
		if err != nil {
			return BracketPage{}, err
		}
		if retry {
			lastErr = fmt.Errorf("%w: generation cutover race", ErrBracketProjectionUnavailable)
			continue
		}
		return page, nil
	}
	return BracketPage{}, lastErr
}

func (s *RedisBracketStore) pageAtomic(
	ctx context.Context,
	tournamentID string,
	roundNumber *int,
	after *BracketCursor,
	limit int,
) (BracketPage, bool, error) {
	fetch := int64(limit + 1)
	hasCursor := "0"
	afterRound := "0"
	afterSlot := "0"
	if after != nil {
		hasCursor = "1"
		afterRound = strconv.Itoa(after.RoundNumber)
		afterSlot = strconv.Itoa(after.SlotIndex)
	}
	filterRound := "0"
	roundFilter := "0"
	if roundNumber != nil {
		filterRound = "1"
		roundFilter = strconv.Itoa(*roundNumber)
	}
	raw, err := pageBracketScript.Run(ctx, s.rdb,
		[]string{s.keys.Meta(tournamentID)},
		s.keys.TournamentRoot(tournamentID),
		strconv.FormatInt(fetch, 10),
		hasCursor,
		afterRound,
		afterSlot,
		filterRound,
		roundFilter,
	).Slice()
	if err != nil {
		return BracketPage{}, false, wrapBracketUnavailable(err)
	}
	if len(raw) < 1 {
		return BracketPage{}, false, wrapBracketUnavailable(fmt.Errorf("empty page script result"))
	}
	status := toInt64(raw[0])
	switch status {
	case 0:
		return BracketPage{}, false, ErrBracketProjectionUnavailable
	case 2:
		return BracketPage{}, true, nil
	}
	if len(raw) < 5 {
		return BracketPage{}, false, wrapBracketUnavailable(fmt.Errorf("short page script result"))
	}
	ver := toInt64(raw[2])
	generatedAt, err := parseBracketGeneratedAt(fmt.Sprint(raw[3]))
	if err != nil {
		return BracketPage{}, false, ErrBracketProjectionUnavailable
	}
	var summary BracketSummary
	if err := json.Unmarshal([]byte(fmt.Sprint(raw[4])), &summary); err != nil {
		return BracketPage{}, false, wrapBracketUnavailable(fmt.Errorf("summary json: %w", err))
	}
	slotRaw := raw[5:]
	slots := make([]BracketSlotView, 0, minInt(len(slotRaw), limit))
	for i, item := range slotRaw {
		if i >= limit {
			break
		}
		var sl BracketSlotView
		if err := json.Unmarshal([]byte(fmt.Sprint(item)), &sl); err != nil {
			return BracketPage{}, false, wrapBracketUnavailable(fmt.Errorf("slot json: %w", err))
		}
		slots = append(slots, sl)
	}
	page := BracketPage{
		TournamentID:      tournamentID,
		ProjectionVersion: ver,
		GeneratedAt:       generatedAt,
		Summary:           summary,
		Slots:             slots,
	}
	if len(slotRaw) > limit && len(slots) > 0 {
		last := slots[len(slots)-1]
		enc, err := EncodeBracketCursor(BracketCursor{RoundNumber: last.RoundNumber, SlotIndex: last.SlotIndex})
		if err != nil {
			return BracketPage{}, false, err
		}
		page.NextCursor = enc
	}
	return page, false, nil
}

// BracketMeta is Redis tournament projection metadata.
type BracketMeta struct {
	Generation        int64
	RebuildingGen     int64
	RebuildToken      string
	ProjectionVersion int64
	Ready             bool
	GeneratedAt       time.Time
}

func (s *RedisBracketStore) readMeta(ctx context.Context, tournamentID string) (BracketMeta, int64, int64, error) {
	vals, err := s.rdb.HGetAll(ctx, s.keys.Meta(tournamentID)).Result()
	if err != nil {
		return BracketMeta{}, 0, 0, wrapBracketUnavailable(err)
	}
	var m BracketMeta
	if v := vals["generation"]; v != "" {
		m.Generation, _ = strconv.ParseInt(v, 10, 64)
	}
	if v := vals["rebuilding_gen"]; v != "" {
		m.RebuildingGen, _ = strconv.ParseInt(v, 10, 64)
	}
	m.RebuildToken = vals["rebuild_token"]
	if v := vals["projectionVersion"]; v != "" {
		m.ProjectionVersion, _ = strconv.ParseInt(v, 10, 64)
	}
	m.Ready = vals["ready"] == "1"
	if v := vals["generatedAt"]; v != "" {
		if t, err := time.Parse(time.RFC3339, v); err == nil {
			m.GeneratedAt = t.UTC()
		}
	}
	if m.GeneratedAt.IsZero() {
		m.GeneratedAt = time.Now().UTC()
	}
	return m, m.Generation, m.RebuildingGen, nil
}

// Meta returns current tournament projection metadata.
func (s *RedisBracketStore) Meta(ctx context.Context, tournamentID string) (BracketMeta, error) {
	m, _, _, err := s.readMeta(ctx, tournamentID)
	return m, err
}

func wrapBracketUnavailable(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, ErrBracketProjectionUnavailable) {
		return err
	}
	return errors.Join(ErrBracketProjectionUnavailable, err)
}

func parseBracketGeneratedAt(v string) (time.Time, error) {
	v = strings.TrimSpace(v)
	if v == "" || v == "<nil>" {
		return time.Time{}, fmt.Errorf("generatedAt missing")
	}
	t, err := time.Parse(time.RFC3339, v)
	if err != nil {
		return time.Time{}, fmt.Errorf("generatedAt invalid: %w", err)
	}
	return t.UTC(), nil
}

func toInt64(v any) int64 {
	switch x := v.(type) {
	case int64:
		return x
	case int:
		return int64(x)
	case string:
		n, _ := strconv.ParseInt(x, 10, 64)
		return n
	default:
		n, _ := strconv.ParseInt(fmt.Sprint(v), 10, 64)
		return n
	}
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
