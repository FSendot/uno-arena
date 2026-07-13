package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"unoarena/services/tournament-orchestration/domain"
)

// ErrInvalidBracketPageQuery is returned for bad limit/round filters (HTTP 400).
var ErrInvalidBracketPageQuery = errors.New("invalid bracket page query")

const (
	DefaultBracketPageLimit = 100
	MaxBracketPageLimit     = 1000
)

// BracketPageQuery is the bounded public bracket read input.
type BracketPageQuery struct {
	TournamentID string
	RoundNumber  *int // optional filter; nil = all rounds in keyset order
	Cursor       string
	Limit        int
}

// BracketSummary is compact tournament/round metadata (never embeds every slot).
type BracketSummary struct {
	Phase           string                `json:"phase"`
	Capacity        int                   `json:"capacity"`
	RegisteredCount int                   `json:"registeredCount"`
	CurrentRound    int                   `json:"currentRound"`
	Rounds          []BracketRoundSummary `json:"rounds"`
}

// BracketRoundSummary is one round's compact status + batch count (never every batch).
type BracketRoundSummary struct {
	RoundNumber int    `json:"roundNumber"`
	Status      string `json:"status"`
	IsFinal     bool   `json:"isFinal"`
	SlotCount   int    `json:"slotCount"`
	Completed   bool   `json:"completed"`
	BatchCount  int    `json:"batchCount"`
}

// BracketBatchSummary is a compact provisioning batch descriptor for internal loaders.
type BracketBatchSummary struct {
	BatchID  string `json:"batchId"`
	SlotFrom string `json:"slotFrom"`
	SlotTo   string `json:"slotTo"`
	SlotSize int    `json:"slotSize"`
	Status   string `json:"status"`
}

// BracketSlotView is one public slot row for a page or projection chunk.
type BracketSlotView struct {
	RoundNumber        int      `json:"roundNumber"`
	SlotIndex          int      `json:"slotIndex"`
	SlotID             string   `json:"slotId"`
	Status             string   `json:"status"`
	SeededPlayerIDs    []string `json:"seededPlayerIds"`
	RoomID             string   `json:"roomId,omitempty"`
	BatchID            string   `json:"batchId,omitempty"`
	AdvancingPlayerIDs []string `json:"advancingPlayerIds,omitempty"`
	CompletionVersion  uint64   `json:"completionVersion,omitempty"`
	QuarantineReason   string   `json:"quarantineReason,omitempty"`
}

// BracketPage is the durable Postgres-backed BracketPage contract body.
type BracketPage struct {
	TournamentID      string            `json:"tournamentId"`
	ProjectionVersion int64             `json:"projectionVersion"`
	GeneratedAt       time.Time         `json:"generatedAt"`
	Summary           BracketSummary    `json:"summary"`
	Slots             []BracketSlotView `json:"slots"`
	NextCursor        string            `json:"nextCursor,omitempty"`
}

// LoadBracketSummary loads compact tournament/round metadata without hydrating slots.
func (s *TournamentStore) LoadBracketSummary(ctx context.Context, tournamentID string) (BracketSummary, bool, error) {
	if s == nil || s.pool == nil {
		return BracketSummary{}, false, fmt.Errorf("nil store")
	}
	return loadBracketSummaryQ(ctx, s.pool, tournamentID)
}

func loadBracketSummaryQ(ctx context.Context, q dbQuerier, tournamentID string) (BracketSummary, bool, error) {
	var (
		phase    string
		capacity int
		regCount int
		rulesRaw []byte
	)
	err := q.QueryRow(ctx, `
		SELECT phase, capacity, rules
		FROM tournaments WHERE tournament_id = $1
	`, tournamentID).Scan(&phase, &capacity, &rulesRaw)
	if errors.Is(err, pgx.ErrNoRows) {
		return BracketSummary{}, false, nil
	}
	if err != nil {
		return BracketSummary{}, false, err
	}
	// Authoritative registeredCount is SUM of registration shard counts (not denormalized column).
	err = q.QueryRow(ctx, `
		SELECT COALESCE(SUM(count), 0)::int
		FROM tournament_registration_shards
		WHERE tournament_id = $1
	`, tournamentID).Scan(&regCount)
	if err != nil {
		return BracketSummary{}, false, err
	}
	var rules tournamentRules
	jsonUnmarshalRules(rulesRaw, &rules)

	rows, err := q.Query(ctx, `
		SELECT r.round_number, r.status, r.is_final, r.completed_at IS NOT NULL,
			(SELECT count(*)::int FROM bracket_slots bs
			 WHERE bs.tournament_id = r.tournament_id AND bs.round_number = r.round_number),
			(SELECT count(*)::int FROM provisioning_batches pb
			 WHERE pb.tournament_id = r.tournament_id AND pb.round_number = r.round_number)
		FROM tournament_rounds r
		WHERE r.tournament_id = $1
		  AND r.status <> 'pending'
		ORDER BY r.round_number ASC
	`, tournamentID)
	if err != nil {
		return BracketSummary{}, false, err
	}
	defer rows.Close()

	rounds := make([]BracketRoundSummary, 0)
	for rows.Next() {
		var rr BracketRoundSummary
		if err := rows.Scan(&rr.RoundNumber, &rr.Status, &rr.IsFinal, &rr.Completed, &rr.SlotCount, &rr.BatchCount); err != nil {
			return BracketSummary{}, false, err
		}
		rounds = append(rounds, rr)
	}
	if err := rows.Err(); err != nil {
		return BracketSummary{}, false, err
	}
	return BracketSummary{
		Phase:           phase,
		Capacity:        capacity,
		RegisteredCount: regCount,
		CurrentRound:    rules.CurrentRound,
		Rounds:          rounds,
	}, true, nil
}

// LoadBracketBatchSummaries loads provisioning batch descriptors for one round
// (internal chunk rebuild / workers). Not part of the public BracketPage summary.
func (s *TournamentStore) LoadBracketBatchSummaries(ctx context.Context, tournamentID string, roundNumber int) ([]BracketBatchSummary, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("nil store")
	}
	return loadBracketBatchSummaries(ctx, s.pool, tournamentID, roundNumber)
}

func loadBracketBatchSummaries(ctx context.Context, q dbQuerier, tid string, roundNumber int) ([]BracketBatchSummary, error) {
	// Join bracket_slots once for numeric slot_index ordering (slot_10 must not
	// precede slot_2). Bounded to this tournament/round; no N+1.
	rows, err := q.Query(ctx, `
		SELECT b.batch_id, b.status, b.slot_id_from, b.slot_id_to
		FROM provisioning_batches b
		LEFT JOIN bracket_slots fs
		  ON fs.tournament_id = b.tournament_id
		 AND fs.round_number = b.round_number
		 AND fs.slot_id = b.slot_id_from
		WHERE b.tournament_id = $1 AND b.round_number = $2
		ORDER BY fs.slot_index ASC NULLS LAST, b.batch_id ASC
	`, tid, roundNumber)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]BracketBatchSummary, 0)
	for rows.Next() {
		var (
			batchID, status string
			from, to        *string
		)
		if err := rows.Scan(&batchID, &status, &from, &to); err != nil {
			return nil, err
		}
		slotFrom, slotTo := "", ""
		if from != nil {
			slotFrom = *from
		}
		if to != nil {
			slotTo = *to
		}
		size := 0
		if slotFrom != "" && slotTo != "" {
			fromIdx, errFrom := parseSlotIndex(slotFrom)
			toIdx, errTo := parseSlotIndex(slotTo)
			if errFrom == nil && errTo == nil && toIdx >= fromIdx {
				size = toIdx - fromIdx + 1
			}
		}
		out = append(out, BracketBatchSummary{
			BatchID:  batchID,
			SlotFrom: slotFrom,
			SlotTo:   slotTo,
			SlotSize: size,
			Status:   status,
		})
	}
	return out, rows.Err()
}

// LoadBracketSlotPage returns one keyset page of slots (limit+1 internally for nextCursor).
// It never hydrates the whole bracket aggregate.
func (s *TournamentStore) LoadBracketSlotPage(ctx context.Context, tournamentID string, roundNumber *int, after *BracketCursor, limit int) ([]BracketSlotView, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("nil store")
	}
	return loadBracketSlotPageQ(ctx, s.pool, tournamentID, roundNumber, after, limit)
}

func loadBracketSlotPageQ(ctx context.Context, q dbQuerier, tournamentID string, roundNumber *int, after *BracketCursor, limit int) ([]BracketSlotView, error) {
	if limit < 1 {
		limit = DefaultBracketPageLimit
	}
	if limit > MaxBracketPageLimit {
		limit = MaxBracketPageLimit
	}
	fetch := limit + 1

	var (
		rows pgx.Rows
		err  error
	)
	switch {
	case after != nil && roundNumber != nil:
		rows, err = q.Query(ctx, bracketSlotPageSQL(true, true), tournamentID, *roundNumber, after.RoundNumber, after.SlotIndex, fetch)
	case after != nil:
		rows, err = q.Query(ctx, bracketSlotPageSQL(false, true), tournamentID, after.RoundNumber, after.SlotIndex, fetch)
	case roundNumber != nil:
		rows, err = q.Query(ctx, bracketSlotPageSQL(true, false), tournamentID, *roundNumber, fetch)
	default:
		rows, err = q.Query(ctx, bracketSlotPageSQL(false, false), tournamentID, fetch)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanBracketSlotViews(rows)
}

func bracketSlotPageSQL(filterRound, hasCursor bool) string {
	// Public pages exclude pending seeding rounds (partial slots invisible until finalized).
	const base = `
		SELECT bs.round_number, bs.slot_index, bs.slot_id,
			CASE WHEN am.room_id IS NOT NULL AND am.runtime_ready_at IS NULL THEN 'provisioning' ELSE bs.status END,
			bs.seeded_player_ids,
			COALESCE(CASE WHEN am.runtime_ready_at IS NOT NULL THEN am.room_id END, ''),
			COALESCE(am.provisioning_batch_id, ''),
			COALESCE(ar.advancing_player_ids, '{}'),
			COALESCE(mr.completion_version, 0),
			COALESCE(mq.quarantine_reason, mr.quarantine_reason, '')
		FROM bracket_slots bs
		INNER JOIN tournament_rounds tr
			ON tr.tournament_id = bs.tournament_id
			AND tr.round_number = bs.round_number
			AND tr.status <> 'pending'
		LEFT JOIN assigned_matches am
			ON am.tournament_id = bs.tournament_id
			AND am.round_number = bs.round_number
			AND am.slot_id = bs.slot_id
		LEFT JOIN advancement_records ar
			ON ar.tournament_id = bs.tournament_id
			AND ar.round_number = bs.round_number
			AND ar.slot_id = bs.slot_id
		LEFT JOIN LATERAL (
			SELECT completion_version, quarantine_reason
			FROM match_results m
			WHERE m.tournament_id = bs.tournament_id
				AND m.round_number = bs.round_number
				AND m.slot_id = bs.slot_id
			ORDER BY m.completion_version DESC
			LIMIT 1
		) mr ON true
		LEFT JOIN LATERAL (
			SELECT reason AS quarantine_reason
			FROM match_result_quarantines q
			WHERE q.tournament_id = bs.tournament_id
				AND q.resolved_round_number = bs.round_number
				AND q.resolved_slot_id = bs.slot_id
				AND q.affects_slot = true
			ORDER BY q.created_at DESC
			LIMIT 1
		) mq ON true
		WHERE bs.tournament_id = $1`
	switch {
	case filterRound && hasCursor:
		return base + `
			AND bs.round_number = $2
			AND (bs.round_number > $3 OR (bs.round_number = $3 AND bs.slot_index > $4))
			ORDER BY bs.round_number ASC, bs.slot_index ASC
			LIMIT $5`
	case !filterRound && hasCursor:
		return base + `
			AND (bs.round_number > $2 OR (bs.round_number = $2 AND bs.slot_index > $3))
			ORDER BY bs.round_number ASC, bs.slot_index ASC
			LIMIT $4`
	case filterRound && !hasCursor:
		return base + `
			AND bs.round_number = $2
			ORDER BY bs.round_number ASC, bs.slot_index ASC
			LIMIT $3`
	default:
		return base + `
			ORDER BY bs.round_number ASC, bs.slot_index ASC
			LIMIT $2`
	}
}

func scanBracketSlotViews(rows pgx.Rows) ([]BracketSlotView, error) {
	out := make([]BracketSlotView, 0)
	for rows.Next() {
		var (
			v       BracketSlotView
			seeded  []string
			adv     []string
			compVer int64
		)
		if err := rows.Scan(
			&v.RoundNumber, &v.SlotIndex, &v.SlotID, &v.Status, &seeded,
			&v.RoomID, &v.BatchID, &adv, &compVer, &v.QuarantineReason,
		); err != nil {
			return nil, err
		}
		v.SeededPlayerIDs = seeded
		if v.SeededPlayerIDs == nil {
			v.SeededPlayerIDs = []string{}
		}
		if len(adv) > 0 {
			v.AdvancingPlayerIDs = adv
		}
		if compVer > 0 {
			v.CompletionVersion = uint64(compVer)
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

// LoadBatchChunkForProjection loads one provisioning-batch-bounded slot chunk
// for Redis rebuild (≤ MaxProvisioningBatchSize). Never loads the whole bracket.
func (s *TournamentStore) LoadBatchChunkForProjection(ctx context.Context, tournamentID string, roundNumber int, batchID string) ([]BracketSlotView, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("nil store")
	}
	if roundNumber < 1 || batchID == "" {
		return nil, fmt.Errorf("roundNumber and batchId required")
	}
	var fromID, toID string
	err := s.pool.QueryRow(ctx, `
		SELECT slot_id_from, slot_id_to
		FROM provisioning_batches
		WHERE tournament_id = $1 AND round_number = $2 AND batch_id = $3
	`, tournamentID, roundNumber, batchID).Scan(&fromID, &toID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	fromIdx, err := parseSlotIndex(fromID)
	if err != nil {
		return nil, err
	}
	toIdx, err := parseSlotIndex(toID)
	if err != nil {
		return nil, err
	}
	if toIdx < fromIdx {
		return nil, fmt.Errorf("invalid batch slot range")
	}
	if toIdx-fromIdx+1 > domain.MaxProvisioningBatchSize {
		return nil, fmt.Errorf("batch chunk exceeds max size")
	}
	rows, err := s.pool.Query(ctx, `
		SELECT bs.round_number, bs.slot_index, bs.slot_id,
			CASE WHEN am.room_id IS NOT NULL AND am.runtime_ready_at IS NULL THEN 'provisioning' ELSE bs.status END,
			bs.seeded_player_ids,
			COALESCE(CASE WHEN am.runtime_ready_at IS NOT NULL THEN am.room_id END, ''),
			COALESCE(am.provisioning_batch_id, ''),
			COALESCE(ar.advancing_player_ids, '{}'),
			COALESCE(mr.completion_version, 0),
			COALESCE(mq.quarantine_reason, mr.quarantine_reason, '')
		FROM bracket_slots bs
		LEFT JOIN assigned_matches am
			ON am.tournament_id = bs.tournament_id
			AND am.round_number = bs.round_number
			AND am.slot_id = bs.slot_id
		LEFT JOIN advancement_records ar
			ON ar.tournament_id = bs.tournament_id
			AND ar.round_number = bs.round_number
			AND ar.slot_id = bs.slot_id
		LEFT JOIN LATERAL (
			SELECT completion_version, quarantine_reason
			FROM match_results m
			WHERE m.tournament_id = bs.tournament_id
				AND m.round_number = bs.round_number
				AND m.slot_id = bs.slot_id
			ORDER BY m.completion_version DESC
			LIMIT 1
		) mr ON true
		LEFT JOIN LATERAL (
			SELECT reason AS quarantine_reason
			FROM match_result_quarantines q
			WHERE q.tournament_id = bs.tournament_id
				AND q.resolved_round_number = bs.round_number
				AND q.resolved_slot_id = bs.slot_id
				AND q.affects_slot = true
			ORDER BY q.created_at DESC
			LIMIT 1
		) mq ON true
		WHERE bs.tournament_id = $1
			AND bs.round_number = $2
			AND bs.slot_index >= $3
			AND bs.slot_index <= $4
		ORDER BY bs.slot_index ASC
	`, tournamentID, roundNumber, fromIdx, toIdx)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanBracketSlotViews(rows)
}

// LoadBracketPage assembles a contract BracketPage from Postgres without whole-bracket hydrate.
// Summary, projection checkpoint, and slot page are read inside one REPEATABLE READ
// read-only transaction so the page is internally consistent.
func (s *TournamentStore) LoadBracketPage(ctx context.Context, q BracketPageQuery) (BracketPage, error) {
	if s == nil || s.pool == nil {
		return BracketPage{}, fmt.Errorf("nil store")
	}
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

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{
		IsoLevel:   pgx.RepeatableRead,
		AccessMode: pgx.ReadOnly,
	})
	if err != nil {
		return BracketPage{}, wrapUnavailable(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	summary, ok, err := loadBracketSummaryQ(ctx, tx, q.TournamentID)
	if err != nil {
		return BracketPage{}, wrapUnavailable(err)
	}
	if !ok {
		return BracketPage{}, ErrTournamentNotFound
	}

	version, generatedAt, err := loadProjectionCheckpointQ(ctx, tx, q.TournamentID)
	if err != nil {
		return BracketPage{}, wrapUnavailable(err)
	}

	slots, err := loadBracketSlotPageQ(ctx, tx, q.TournamentID, q.RoundNumber, after, limit)
	if err != nil {
		return BracketPage{}, wrapUnavailable(err)
	}
	var next string
	if len(slots) > limit {
		last := slots[limit-1]
		enc, err := EncodeBracketCursor(BracketCursor{RoundNumber: last.RoundNumber, SlotIndex: last.SlotIndex})
		if err != nil {
			return BracketPage{}, err
		}
		next = enc
		slots = slots[:limit]
	}
	if err := tx.Commit(ctx); err != nil {
		return BracketPage{}, wrapUnavailable(err)
	}
	return BracketPage{
		TournamentID:      q.TournamentID,
		ProjectionVersion: version,
		GeneratedAt:       generatedAt,
		Summary:           summary,
		Slots:             slots,
		NextCursor:        next,
	}, nil
}

var ErrTournamentNotFound = errors.New("tournament not found")

func (s *TournamentStore) loadProjectionCheckpoint(ctx context.Context, tournamentID string) (int64, time.Time, error) {
	if s == nil || s.pool == nil {
		return 0, time.Time{}, fmt.Errorf("nil store")
	}
	return loadProjectionCheckpointQ(ctx, s.pool, tournamentID)
}

// LoadProjectionCheckpoint returns the public bracket projection fence
// (base.version + SUM(shards)) and max generatedAt.
func (s *TournamentStore) LoadProjectionCheckpoint(ctx context.Context, tournamentID string) (int64, time.Time, error) {
	return s.loadProjectionCheckpoint(ctx, tournamentID)
}

// ListVisibleBracketChunks lists provisioning batches for non-pending rounds only
// (pending seeding rounds stay invisible until finalize). Bounded per round.
func (s *TournamentStore) ListVisibleBracketChunks(ctx context.Context, tournamentID string) ([]BracketChunkRef, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("nil store")
	}
	summary, ok, err := s.LoadBracketSummary(ctx, tournamentID)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, ErrTournamentNotFound
	}
	out := make([]BracketChunkRef, 0)
	for _, rr := range summary.Rounds {
		batches, err := s.LoadBracketBatchSummaries(ctx, tournamentID, rr.RoundNumber)
		if err != nil {
			return nil, err
		}
		for _, b := range batches {
			if b.BatchID == "" {
				continue
			}
			out = append(out, BracketChunkRef{RoundNumber: rr.RoundNumber, BatchID: b.BatchID})
		}
	}
	return out, nil
}

// ListRoundBracketChunks lists provisioning batches for one round (bounded; no whole-bracket hydrate).
func (s *TournamentStore) ListRoundBracketChunks(ctx context.Context, tournamentID string, roundNumber int) ([]BracketChunkRef, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("nil store")
	}
	if roundNumber < 1 {
		return nil, fmt.Errorf("roundNumber required")
	}
	batches, err := s.LoadBracketBatchSummaries(ctx, tournamentID, roundNumber)
	if err != nil {
		return nil, err
	}
	out := make([]BracketChunkRef, 0, len(batches))
	for _, b := range batches {
		if b.BatchID == "" {
			continue
		}
		out = append(out, BracketChunkRef{RoundNumber: roundNumber, BatchID: b.BatchID})
	}
	return out, nil
}

// LookupBatchIDForSlot resolves the provisioning batch for one slot via assigned_matches
// or provisioning_batches slot range — never hydrates the whole bracket.
func (s *TournamentStore) LookupBatchIDForSlot(ctx context.Context, tournamentID string, roundNumber int, slotID string) (string, error) {
	if s == nil || s.pool == nil {
		return "", fmt.Errorf("nil store")
	}
	if roundNumber < 1 || strings.TrimSpace(slotID) == "" {
		return "", fmt.Errorf("roundNumber and slotId required")
	}
	var batchID string
	err := s.pool.QueryRow(ctx, `
		SELECT COALESCE(am.provisioning_batch_id, '')
		FROM assigned_matches am
		WHERE am.tournament_id = $1 AND am.round_number = $2 AND am.slot_id = $3
	`, tournamentID, roundNumber, slotID).Scan(&batchID)
	if err == nil && batchID != "" {
		return batchID, nil
	}
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return "", wrapUnavailable(err)
	}
	// Fall back to provisioning batch covering this slot_index (pre-assignment / seeded).
	var slotIndex int
	err = s.pool.QueryRow(ctx, `
		SELECT slot_index FROM bracket_slots
		WHERE tournament_id = $1 AND round_number = $2 AND slot_id = $3
	`, tournamentID, roundNumber, slotID).Scan(&slotIndex)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", wrapUnavailable(err)
	}
	err = s.pool.QueryRow(ctx, `
		SELECT b.batch_id
		FROM provisioning_batches b
		LEFT JOIN bracket_slots fs
		  ON fs.tournament_id = b.tournament_id AND fs.round_number = b.round_number AND fs.slot_id = b.slot_id_from
		LEFT JOIN bracket_slots ts
		  ON ts.tournament_id = b.tournament_id AND ts.round_number = b.round_number AND ts.slot_id = b.slot_id_to
		WHERE b.tournament_id = $1 AND b.round_number = $2
		  AND fs.slot_index IS NOT NULL AND ts.slot_index IS NOT NULL
		  AND $3 BETWEEN fs.slot_index AND ts.slot_index
		ORDER BY b.batch_id ASC
		LIMIT 1
	`, tournamentID, roundNumber, slotIndex).Scan(&batchID)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", wrapUnavailable(err)
	}
	return batchID, nil
}

func loadProjectionCheckpointQ(ctx context.Context, q dbQuerier, tournamentID string) (int64, time.Time, error) {
	var (
		ver int64
		at  *time.Time
	)
	// Public projectionVersion = base.version + SUM(all projection shard versions).
	// generatedAt = max(base, shards). Missing shards count as zero.
	err := q.QueryRow(ctx, `
		SELECT
			COALESCE((SELECT projection_version FROM bracket_projection_versions WHERE tournament_id = $1), 0)
			+ COALESCE((SELECT SUM(version) FROM bracket_projection_shards WHERE tournament_id = $1), 0),
			GREATEST(
				(SELECT generated_at FROM bracket_projection_versions WHERE tournament_id = $1),
				(SELECT MAX(generated_at) FROM bracket_projection_shards WHERE tournament_id = $1)
			)
	`, tournamentID).Scan(&ver, &at)
	if err != nil {
		return 0, time.Time{}, err
	}
	if at == nil {
		return ver, time.Time{}, nil
	}
	return ver, at.UTC(), nil
}

func bumpProjectionVersionTx(ctx context.Context, tx pgx.Tx, tid string, now time.Time) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO bracket_projection_versions (tournament_id, projection_version, generated_at)
		VALUES ($1, 1, $2)
		ON CONFLICT (tournament_id) DO UPDATE SET
			projection_version = bracket_projection_versions.projection_version + 1,
			generated_at = EXCLUDED.generated_at
	`, tid, now)
	return err
}

// bumpProjectionShardTx increments one public projection shard (slot_index % 64).
// Never deletes/resets shards. Legacy rewrite may still bump the base row instead.
func bumpProjectionShardTx(ctx context.Context, tx pgx.Tx, tid string, shardID int, now time.Time) error {
	if shardID < 0 {
		shardID = -shardID
	}
	shardID = shardID % domain.ProgressShardCount
	if _, err := tx.Exec(ctx, `
		INSERT INTO bracket_projection_shards (tournament_id, shard_id, version, generated_at)
		VALUES ($1, $2, 0, $3)
		ON CONFLICT DO NOTHING
	`, tid, shardID, now); err != nil {
		return err
	}
	var cur int64
	if err := tx.QueryRow(ctx, `
		SELECT version FROM bracket_projection_shards
		WHERE tournament_id = $1 AND shard_id = $2
		FOR UPDATE
	`, tid, shardID).Scan(&cur); err != nil {
		return err
	}
	_, err := tx.Exec(ctx, `
		UPDATE bracket_projection_shards
		SET version = version + 1, generated_at = $3
		WHERE tournament_id = $1 AND shard_id = $2
	`, tid, shardID, now)
	return err
}

func jsonUnmarshalRules(raw []byte, rules *tournamentRules) {
	if len(raw) == 0 {
		return
	}
	_ = json.Unmarshal(raw, rules)
}
