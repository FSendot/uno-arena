package store

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"unoarena/services/ranking/domain"
)

const (
	topicPlayerRatingUpdated          = "ranking.player_rating_updated"
	topicLeaderboardSnapshotPublished = "ranking.leaderboard_snapshot_published"
	topicCasualIngest                 = "room.game.completed" // AsyncAPI upstream topic for processed_upstream_events
	topicTournamentIngest             = "tournament.players.advanced"
	casualKeyDispositionApplied       = "applied"
	casualKeyDispositionIgnored       = "ignored"
)

// RankingStore is the durable Postgres adapter for Ranking.
type RankingStore struct {
	pool *pgxpool.Pool

	// FailNextCommits injects N commit failures after writes (integration tests only).
	FailNextCommits int
}

// NewRankingStore wraps a writer pool.
func NewRankingStore(pool *pgxpool.Pool) *RankingStore {
	return &RankingStore{pool: pool}
}

// OutboxEvent is an append-only CDC outbox row.
type OutboxEvent struct {
	EventID       string
	EventType     string
	PlayerID      string
	Topic         string
	PartitionKey  string
	SchemaVersion int
	Payload       map[string]any
	CreatedAt     time.Time
}

// GameCompletedRequest mirrors the application casual ingest contract.
type GameCompletedRequest struct {
	CommandID     domain.CommandID
	EventID       domain.EventID
	GameID        domain.GameID
	RoomID        domain.RoomID
	RoomType      domain.RoomType
	IsAbandoned   bool
	Authoritative bool
	Completed     bool
	Participants  []domain.RatedPlacement
	CorrelationID string
	CausationID   string
}

// TournamentPlacementRequest wraps the domain placement command with outbound event metadata.
type TournamentPlacementRequest struct {
	Command       domain.ApplyTournamentPlacementUpdateCommand
	CorrelationID string
	CausationID   string
}

// GameCompletedResult mirrors the application casual outcome contract.
type GameCompletedResult struct {
	Kind      domain.OutcomeKind
	CommandID domain.CommandID
	EventID   domain.EventID
	Facts     []domain.Fact
	Rejection *domain.Rejection
	PerPlayer []domain.CommandOutcome
}

// ListPendingOutbox is capability-only; durable CDC must never poll.
func (s *RankingStore) ListPendingOutbox(context.Context, int) ([]OutboxEvent, error) {
	return nil, ErrCapabilityOnly
}

// MarkOutboxPublished is capability-only; durable CDC must never mark published_at.
func (s *RankingStore) MarkOutboxPublished(context.Context, string, time.Time) error {
	return ErrCapabilityOnly
}

// Ping exposes pool health for reconnect tests.
func (s *RankingStore) Ping(ctx context.Context) error {
	if s == nil || s.pool == nil {
		return fmt.Errorf("nil pool")
	}
	return s.pool.Ping(ctx)
}

// CountOutbox is a test helper.
func (s *RankingStore) CountOutbox(ctx context.Context) (int, error) {
	var n int
	err := s.pool.QueryRow(ctx, `SELECT count(*) FROM outbox_events`).Scan(&n)
	return n, err
}

// CountPlayers is a test helper.
func (s *RankingStore) CountPlayers(ctx context.Context) (int, error) {
	var n int
	err := s.pool.QueryRow(ctx, `SELECT count(*) FROM player_ratings`).Scan(&n)
	return n, err
}

// CountHistory is a test helper.
func (s *RankingStore) CountHistory(ctx context.Context) (int, error) {
	var n int
	err := s.pool.QueryRow(ctx, `SELECT count(*) FROM rating_history`).Scan(&n)
	return n, err
}

// GetPlayerRating loads authoritative ratings for one player.
func (s *RankingStore) GetPlayerRating(ctx context.Context, id domain.PlayerID) (casual, tournament int, ok bool, err error) {
	err = s.pool.QueryRow(ctx, `
		SELECT casual_elo, tournament_placement_rating FROM player_ratings WHERE player_id = $1
	`, string(id)).Scan(&casual, &tournament)
	if err == pgx.ErrNoRows {
		return 0, 0, false, nil
	}
	if err != nil {
		return 0, 0, false, wrapUnavailable(err)
	}
	return casual, tournament, true, nil
}

// LeaderboardKeysetPage returns a bounded authoritative page ordered by rating DESC, player_id ASC.
// after is an exclusive keyset boundary; nil starts at the top. Used for Redis rebuild and
// Postgres HTTP fallback — never an unbounded full-table scan.
func (s *RankingStore) LeaderboardKeysetPage(
	ctx context.Context,
	boardType domain.RatingSourceType,
	after *LeaderboardCursor,
	limit int,
) ([]domain.LeaderboardEntry, error) {
	if boardType != domain.SourceCasualElo && boardType != domain.SourceTournamentPlacement {
		return nil, fmt.Errorf("boardType must be casual_elo or tournament_placement")
	}
	if limit < 1 {
		limit = DefaultLeaderboardPageLimit
	}
	col := "casual_elo"
	if boardType == domain.SourceTournamentPlacement {
		col = "tournament_placement_rating"
	}
	var (
		rows pgx.Rows
		err  error
	)
	if after == nil {
		//nolint:gosec // col is fixed enum branch only
		rows, err = s.pool.Query(ctx, fmt.Sprintf(`
			SELECT player_id, %s FROM player_ratings
			ORDER BY %s DESC, player_id ASC
			LIMIT $1
		`, col, col), limit)
	} else {
		//nolint:gosec // col is fixed enum branch only
		rows, err = s.pool.Query(ctx, fmt.Sprintf(`
			SELECT player_id, %s FROM player_ratings
			WHERE (%s < $1) OR (%s = $1 AND player_id > $2)
			ORDER BY %s DESC, player_id ASC
			LIMIT $3
		`, col, col, col, col), after.Rating, after.PlayerID, limit)
	}
	if err != nil {
		return nil, wrapUnavailable(err)
	}
	defer rows.Close()
	entries := make([]domain.LeaderboardEntry, 0, limit)
	for rows.Next() {
		var id string
		var rating int
		if err := rows.Scan(&id, &rating); err != nil {
			return nil, wrapUnavailable(err)
		}
		entries = append(entries, domain.LeaderboardEntry{
			PlayerID: domain.PlayerID(id),
			Rating:   rating,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, wrapUnavailable(err)
	}
	return entries, nil
}

// LeaderboardPage returns a bounded authoritative page with ranks and optional nextCursor.
// Prefer Redis for live public reads; this is the fail-open Postgres fallback path.
func (s *RankingStore) LeaderboardPage(ctx context.Context, q LeaderboardPageQuery) (LeaderboardPage, error) {
	limit := ClampLeaderboardLimit(q.Limit)
	var after *LeaderboardCursor
	if strings.TrimSpace(q.Cursor) != "" {
		c, err := DecodeLeaderboardCursor(q.Cursor)
		if err != nil {
			return LeaderboardPage{}, err
		}
		after = &c
	}
	batch, err := s.LeaderboardKeysetPage(ctx, q.BoardType, after, limit+1)
	if err != nil {
		return LeaderboardPage{}, err
	}
	rankBase := 1
	if after != nil {
		rankBase, err = s.leaderboardRankAfter(ctx, q.BoardType, *after)
		if err != nil {
			return LeaderboardPage{}, err
		}
	}
	entries := make([]RankedLeaderboardEntry, 0, min(len(batch), limit))
	for i, e := range batch {
		if i >= limit {
			break
		}
		entries = append(entries, RankedLeaderboardEntry{
			PlayerID: e.PlayerID,
			Rating:   e.Rating,
			Rank:     rankBase + i,
		})
	}
	pub, err := s.GetPublicationState(ctx, q.BoardType)
	if err != nil {
		return LeaderboardPage{}, err
	}
	generatedAt := time.Now().UTC()
	if pub.LastDirtyAt != nil {
		generatedAt = pub.LastDirtyAt.UTC()
	}
	page := LeaderboardPage{
		BoardType:         q.BoardType,
		ProjectionVersion: pub.DirtyVersion,
		GeneratedAt:       generatedAt,
		Entries:           entries,
	}
	if len(batch) > limit {
		last := entries[len(entries)-1]
		enc, err := EncodeLeaderboardCursor(LeaderboardCursor{
			Rating: last.Rating, PlayerID: string(last.PlayerID),
		})
		if err != nil {
			return LeaderboardPage{}, err
		}
		page.NextCursor = enc
	}
	return page, nil
}

func (s *RankingStore) leaderboardRankAfter(ctx context.Context, boardType domain.RatingSourceType, after LeaderboardCursor) (int, error) {
	col := "casual_elo"
	if boardType == domain.SourceTournamentPlacement {
		col = "tournament_placement_rating"
	}
	// Authoritative HTTP fallback rank primitive: indexed COUNT over (col DESC, player_id ASC)
	// keyset prefix — not an unbounded row hydrate / API full-table scan.
	var n int
	//nolint:gosec // col is fixed enum branch only
	err := s.pool.QueryRow(ctx, fmt.Sprintf(`
		SELECT count(*)::int FROM player_ratings
		WHERE (%s > $1) OR (%s = $1 AND player_id <= $2)
	`, col, col), after.Rating, after.PlayerID).Scan(&n)
	if err != nil {
		return 0, wrapUnavailable(err)
	}
	return n + 1, nil
}

// History returns rating history for a known player from Postgres.
func (s *RankingStore) History(ctx context.Context, playerID domain.PlayerID) ([]domain.RatingHistoryEntry, bool, error) {
	var exists bool
	if err := s.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM player_ratings WHERE player_id = $1)`, string(playerID)).Scan(&exists); err != nil {
		return nil, false, wrapUnavailable(err)
	}
	if !exists {
		return nil, false, nil
	}
	rows, err := s.pool.Query(ctx, `
		SELECT source_type, previous_rating, new_rating, delta, COALESCE(reason, ''),
		       game_id, room_id, tournament_id, placement_event_id, COALESCE(placement, 0),
		       COALESCE(advancement_depth, 0), upstream_event_id
		FROM rating_history
		WHERE player_id = $1
		ORDER BY history_id ASC
	`, string(playerID))
	if err != nil {
		return nil, false, wrapUnavailable(err)
	}
	defer rows.Close()
	var out []domain.RatingHistoryEntry
	for rows.Next() {
		var (
			source, reason                      string
			prev, next, delta, placement, depth int
			gameID, roomID, tid, peid, upstream *string
		)
		if err := rows.Scan(&source, &prev, &next, &delta, &reason, &gameID, &roomID, &tid, &peid, &placement, &depth, &upstream); err != nil {
			return nil, false, wrapUnavailable(err)
		}
		entry := domain.RatingHistoryEntry{
			SourceType:       domain.RatingSourceType(source),
			PreviousRating:   prev,
			NewRating:        next,
			Delta:            delta,
			Reason:           domain.RatingHistoryReason(reason),
			Placement:        placement,
			AdvancementDepth: depth,
		}
		if gameID != nil {
			entry.GameID = domain.GameID(*gameID)
		}
		if roomID != nil {
			entry.RoomID = domain.RoomID(*roomID)
		}
		if tid != nil {
			entry.TournamentID = domain.TournamentID(*tid)
		}
		if peid != nil {
			entry.PlacementEventID = domain.PlacementEventID(*peid)
		}
		if upstream != nil {
			entry.EventID = domain.EventID(*upstream)
		}
		out = append(out, entry)
	}
	if err := rows.Err(); err != nil {
		return nil, false, wrapUnavailable(err)
	}
	return out, true, nil
}

// RebuildStatus returns durable rebuild diagnostics from Postgres without loading the full board.
func (s *RankingStore) RebuildStatus(ctx context.Context) (playerCount int, top *domain.LeaderboardEntry, err error) {
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM player_ratings`).Scan(&playerCount); err != nil {
		return 0, nil, wrapUnavailable(err)
	}
	entries, err := s.LeaderboardKeysetPage(ctx, domain.SourceCasualElo, nil, 1)
	if err != nil {
		return 0, nil, err
	}
	if len(entries) > 0 {
		e := entries[0]
		top = &e
	}
	return playerCount, top, nil
}

// ApplyCasualGameCompleted applies all participants in one transaction.
func (s *RankingStore) ApplyCasualGameCompleted(ctx context.Context, req GameCompletedRequest) (GameCompletedResult, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return GameCompletedResult{}, wrapUnavailable(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	eventKey, gameKey := "", ""
	if req.EventID.Valid() {
		eventKey = string(req.EventID)
	}
	if req.GameID.Valid() {
		gameKey = string(req.GameID)
	}
	if err := acquireXactLocks(ctx, tx, casualIngestLockKeys(eventKey, gameKey)...); err != nil {
		return GameCompletedResult{}, err
	}

	if prior, ok, err := loadCasualDuplicate(ctx, tx, req); err != nil {
		return GameCompletedResult{}, err
	} else if ok {
		return prior, nil
	}

	if rej := validateCasualEligibility(req); rej != nil {
		// Eligibility-filtered but structurally valid: durable ignored disposition +
		// processed event + (playerId, gameId) keys; no ratings/history/outbox.
		return s.commitCasualIgnored(ctx, tx, req, rej)
	}

	ids := make([]string, 0, len(req.Participants))
	seen := map[string]struct{}{}
	placements := make(map[string]int, len(req.Participants))
	for _, p := range req.Participants {
		if !p.PlayerID.Valid() || p.Placement < 1 {
			rej := domain.Rejection{Code: domain.RejectInvalidOpponents, Message: "each participant requires playerId and placement >= 1"}
			return GameCompletedResult{Kind: domain.OutcomeRejected, CommandID: req.CommandID, EventID: req.EventID, Rejection: &rej}, nil
		}
		pid := string(p.PlayerID)
		if _, dup := seen[pid]; dup {
			rej := domain.Rejection{Code: domain.RejectInvalidOpponents, Message: "duplicate participant playerId"}
			return GameCompletedResult{Kind: domain.OutcomeRejected, CommandID: req.CommandID, EventID: req.EventID, Rejection: &rej}, nil
		}
		seen[pid] = struct{}{}
		ids = append(ids, pid)
		placements[pid] = p.Placement
	}
	sort.Strings(ids)

	cfg := domain.DefaultRatingConfig()
	for _, id := range ids {
		if _, err := tx.Exec(ctx, `
			INSERT INTO player_ratings (
				player_id, casual_elo, casual_games_played,
				tournament_placement_rating, tournament_events_applied, rating_floor, updated_at
			) VALUES ($1, $2, 0, $3, 0, $4, now())
			ON CONFLICT (player_id) DO NOTHING
		`, id, cfg.InitialCasualElo, cfg.InitialTournamentRating, cfg.Floor); err != nil {
			return GameCompletedResult{}, wrapUnavailable(err)
		}
	}

	// Lock every participant row in stable player-id order (overlapping distinct games).
	ratings := make(map[string]int, len(ids))
	tournaments := make(map[string]int, len(ids))
	for _, id := range ids {
		var casual, tournament, floor int
		if err := tx.QueryRow(ctx, `
			SELECT casual_elo, tournament_placement_rating, rating_floor
			FROM player_ratings WHERE player_id = $1 FOR UPDATE
		`, id).Scan(&casual, &tournament, &floor); err != nil {
			return GameCompletedResult{}, wrapUnavailable(err)
		}
		ratings[id] = casual
		tournaments[id] = tournament
		_ = floor
	}

	standings := make([]domain.RatedPlacement, 0, len(ids))
	for _, p := range req.Participants {
		standings = append(standings, domain.RatedPlacement{
			PlayerID:  p.PlayerID,
			Rating:    ratings[string(p.PlayerID)],
			Placement: p.Placement,
		})
	}

	aggregates := make(map[string]*domain.PlayerRating, len(ids))
	for _, id := range ids {
		aggregates[id] = domain.RestorePlayerRating(domain.PlayerID(id), cfg, ratings[id], tournaments[id])
	}

	var allFacts []domain.Fact
	var perPlayer []domain.CommandOutcome
	now := time.Now().UTC()
	var outbox []OutboxEvent
	scoreChanged := false

	for _, id := range ids {
		pid := domain.PlayerID(id)
		cmd := domain.ApplyCasualEloUpdateCommand{
			CommandID:     domain.CommandID(string(req.CommandID) + ":" + id),
			EventID:       domain.EventID(string(req.EventID) + ":" + id),
			PlayerID:      pid,
			GameID:        req.GameID,
			RoomID:        req.RoomID,
			RoomType:      req.RoomType,
			IsAbandoned:   req.IsAbandoned,
			Authoritative: req.Authoritative,
			Completed:     req.Completed,
			Participants:  standings,
		}
		out := aggregates[id].ApplyCasualEloUpdate(cmd)
		perPlayer = append(perPlayer, out)
		if out.Kind != domain.OutcomeAccepted && out.Kind != domain.OutcomeDuplicate {
			rej := out.Rejection
			if rej == nil {
				r := domain.Rejection{Code: domain.RejectInvalidCommand, Message: "participant apply failed"}
				rej = &r
			}
			// Roll back entire transaction — no participant commits alone.
			return GameCompletedResult{
				Kind: domain.OutcomeRejected, CommandID: req.CommandID, EventID: req.EventID,
				Rejection: rej, PerPlayer: perPlayer,
			}, nil
		}
		if out.Kind != domain.OutcomeAccepted {
			continue
		}
		allFacts = append(allFacts, out.Facts...)
		snap := aggregates[id].PublicSnapshot()
		if snap.CasualElo != ratings[id] {
			scoreChanged = true
		}
		if _, err := tx.Exec(ctx, `
			UPDATE player_ratings
			SET casual_elo = $2, casual_games_played = casual_games_played + 1, updated_at = now()
			WHERE player_id = $1
		`, id, snap.CasualElo); err != nil {
			return GameCompletedResult{}, wrapUnavailable(err)
		}
		hist := aggregates[id].History()
		last := hist[len(hist)-1]
		var room any
		if req.RoomID.Valid() {
			room = string(req.RoomID)
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO rating_history (
				player_id, source_type, previous_rating, new_rating, delta, reason,
				game_id, room_id, placement, upstream_event_id, applied_at
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
		`, id, string(domain.SourceCasualElo), last.PreviousRating, last.NewRating, last.Delta,
			string(domain.ReasonCasualGameCompleted), string(req.GameID), room, placements[id],
			string(req.EventID), now); err != nil {
			return s.casualConflictOrUnavailable(ctx, tx, req, err)
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO processed_casual_elo_keys (player_id, game_id, upstream_event_id, disposition, processed_at)
			VALUES ($1, $2, $3, $4, $5)
		`, id, string(req.GameID), string(req.EventID), casualKeyDispositionApplied, now); err != nil {
			return s.casualConflictOrUnavailable(ctx, tx, req, err)
		}
		causation := strings.TrimSpace(req.CausationID)
		if causation == "" {
			causation = string(req.CommandID)
		}
		for _, f := range out.Facts {
			ev, err := outboxFromFact(f, outboxMeta{
				UpstreamEventID: string(req.EventID),
				CorrelationID:   req.CorrelationID,
				CausationID:     causation,
				Now:             now,
			})
			if err != nil {
				return GameCompletedResult{}, err
			}
			outbox = append(outbox, ev)
		}
	}

	if scoreChanged {
		ver, err := markBoardDirty(ctx, tx, domain.SourceCasualElo)
		if err != nil {
			return GameCompletedResult{}, err
		}
		stampPlayerRatingProjectionVersion(outbox, ver)
	}

	if req.EventID.Valid() {
		if _, err := tx.Exec(ctx, `
			INSERT INTO processed_upstream_events (event_id, topic, consumer_group, processed_at)
			VALUES ($1, $2, 'ranking', $3)
		`, string(req.EventID), topicCasualIngest, now); err != nil {
			return s.casualConflictOrUnavailable(ctx, tx, req, err)
		}
	}

	result := GameCompletedResult{
		Kind:      domain.OutcomeAccepted,
		CommandID: req.CommandID,
		EventID:   req.EventID,
		Facts:     allFacts,
		PerPlayer: perPlayer,
	}
	if err := persistResponses(ctx, tx, req, result); err != nil {
		if isUniqueViolation(err) {
			return s.casualConflictOrUnavailable(ctx, tx, req, err)
		}
		return GameCompletedResult{}, err
	}
	if err := insertOutboxEvents(ctx, tx, outbox); err != nil {
		if isUniqueViolation(err) {
			return s.casualConflictOrUnavailable(ctx, tx, req, err)
		}
		return GameCompletedResult{}, err
	}
	if s.FailNextCommits > 0 {
		s.FailNextCommits--
		return GameCompletedResult{}, wrapUnavailable(fmt.Errorf("injected commit failure"))
	}
	if err := tx.Commit(ctx); err != nil {
		return GameCompletedResult{}, wrapUnavailable(err)
	}
	return result, nil
}

func loadCasualDuplicate(ctx context.Context, tx pgx.Tx, req GameCompletedRequest) (GameCompletedResult, bool, error) {
	if req.EventID.Valid() {
		if prior, ok, err := loadResponse(ctx, tx, "event_id", string(req.EventID)); err != nil {
			return GameCompletedResult{}, false, err
		} else if ok {
			prior.Kind = domain.OutcomeDuplicate
			prior.CommandID = req.CommandID
			prior.EventID = req.EventID
			return prior, true, nil
		}
	}
	if req.GameID.Valid() {
		if prior, ok, err := loadResponse(ctx, tx, "game_id", string(req.GameID)); err != nil {
			return GameCompletedResult{}, false, err
		} else if ok {
			prior.Kind = domain.OutcomeDuplicate
			prior.CommandID = req.CommandID
			prior.EventID = req.EventID
			return prior, true, nil
		}
	}
	return GameCompletedResult{}, false, nil
}

// casualConflictOrUnavailable maps exact-retry unique violations to the stored
// duplicate response after serialization. PostgreSQL aborts the failed tx, so
// the response is loaded in a fresh locked transaction.
func (s *RankingStore) casualConflictOrUnavailable(ctx context.Context, failed pgx.Tx, req GameCompletedRequest, cause error) (GameCompletedResult, error) {
	if !isUniqueViolation(cause) {
		return GameCompletedResult{}, wrapUnavailable(cause)
	}
	_ = failed.Rollback(ctx)
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return GameCompletedResult{}, wrapUnavailable(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	eventKey, gameKey := "", ""
	if req.EventID.Valid() {
		eventKey = string(req.EventID)
	}
	if req.GameID.Valid() {
		gameKey = string(req.GameID)
	}
	if err := acquireXactLocks(ctx, tx, casualIngestLockKeys(eventKey, gameKey)...); err != nil {
		return GameCompletedResult{}, err
	}
	if prior, ok, err := loadCasualDuplicate(ctx, tx, req); err != nil {
		return GameCompletedResult{}, err
	} else if ok {
		return prior, nil
	}
	return GameCompletedResult{}, wrapUnavailable(cause)
}

// ApplyTournamentPlacement applies one tournament placement update atomically.
func (s *RankingStore) ApplyTournamentPlacement(ctx context.Context, req TournamentPlacementRequest) (domain.CommandOutcome, error) {
	cmd := req.Command
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return domain.CommandOutcome{}, wrapUnavailable(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	placementKey := ""
	if cmd.PlayerID.Valid() && cmd.TournamentID.Valid() && cmd.PlacementEventID.Valid() {
		placementKey, err = placementDedupeKey(string(cmd.PlayerID), string(cmd.TournamentID), string(cmd.PlacementEventID))
		if err != nil {
			return domain.CommandOutcome{}, err
		}
	}
	eventKey := ""
	if cmd.EventID.Valid() {
		eventKey = string(cmd.EventID)
	}
	if err := acquireXactLocks(ctx, tx, placementIngestLockKeys(placementKey, eventKey)...); err != nil {
		return domain.CommandOutcome{}, err
	}

	if prior, ok, err := loadPlacementDuplicate(ctx, tx, cmd, placementKey); err != nil {
		return domain.CommandOutcome{}, err
	} else if ok {
		return prior, nil
	}

	cfg := domain.DefaultRatingConfig()
	pid := string(cmd.PlayerID)
	if pid != "" {
		if _, err := tx.Exec(ctx, `
			INSERT INTO player_ratings (
				player_id, casual_elo, casual_games_played,
				tournament_placement_rating, tournament_events_applied, rating_floor, updated_at
			) VALUES ($1, $2, 0, $3, 0, $4, now())
			ON CONFLICT (player_id) DO NOTHING
		`, pid, cfg.InitialCasualElo, cfg.InitialTournamentRating, cfg.Floor); err != nil {
			return domain.CommandOutcome{}, wrapUnavailable(err)
		}
	}

	var casual, tournament int
	if pid != "" {
		if err := tx.QueryRow(ctx, `
			SELECT casual_elo, tournament_placement_rating
			FROM player_ratings WHERE player_id = $1 FOR UPDATE
		`, pid).Scan(&casual, &tournament); err != nil {
			return domain.CommandOutcome{}, wrapUnavailable(err)
		}
	}

	agg := domain.RestorePlayerRating(cmd.PlayerID, cfg, casual, tournament)
	out := agg.ApplyTournamentPlacementUpdate(cmd)
	if out.Kind != domain.OutcomeAccepted {
		// Rejections write no state/dedupe/outbox.
		return out, nil
	}

	now := time.Now().UTC()
	snap := agg.PublicSnapshot()
	scoreChanged := snap.TournamentPlacementRating > tournament
	if _, err := tx.Exec(ctx, `
		UPDATE player_ratings
		SET tournament_placement_rating = $2,
		    tournament_events_applied = tournament_events_applied + 1,
		    updated_at = now()
		WHERE player_id = $1
	`, pid, snap.TournamentPlacementRating); err != nil {
		return domain.CommandOutcome{}, wrapUnavailable(err)
	}
	hist := agg.History()
	last := hist[len(hist)-1]
	var placement any
	var depth any
	if last.Reason == domain.ReasonTournamentAdvancement {
		depth = last.AdvancementDepth
	} else if last.Placement > 0 {
		placement = last.Placement
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO rating_history (
			player_id, source_type, previous_rating, new_rating, delta, reason,
			tournament_id, placement_event_id, placement, advancement_depth, upstream_event_id, applied_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
	`, pid, string(domain.SourceTournamentPlacement), last.PreviousRating, last.NewRating, last.Delta,
		string(last.Reason), string(cmd.TournamentID), string(cmd.PlacementEventID), placement, depth,
		string(cmd.EventID), now); err != nil {
		return s.placementConflictOrUnavailable(ctx, tx, cmd, placementKey, err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO processed_tournament_placement_keys (
			player_id, tournament_id, placement_event_id, upstream_event_id, processed_at
		) VALUES ($1, $2, $3, $4, $5)
	`, pid, string(cmd.TournamentID), string(cmd.PlacementEventID), string(cmd.EventID), now); err != nil {
		return s.placementConflictOrUnavailable(ctx, tx, cmd, placementKey, err)
	}
	if cmd.EventID.Valid() {
		if _, err := tx.Exec(ctx, `
			INSERT INTO processed_upstream_events (event_id, topic, consumer_group, processed_at)
			VALUES ($1, $2, 'ranking', $3)
		`, string(cmd.EventID), topicTournamentIngest, now); err != nil {
			return s.placementConflictOrUnavailable(ctx, tx, cmd, placementKey, err)
		}
	}

	causation := strings.TrimSpace(req.CausationID)
	if causation == "" {
		causation = string(cmd.CommandID)
	}
	var outbox []OutboxEvent
	for _, f := range out.Facts {
		ev, err := outboxFromFact(f, outboxMeta{
			UpstreamEventID: string(cmd.EventID),
			CorrelationID:   req.CorrelationID,
			CausationID:     causation,
			Now:             now,
		})
		if err != nil {
			return domain.CommandOutcome{}, err
		}
		outbox = append(outbox, ev)
	}
	if scoreChanged {
		ver, err := markBoardDirty(ctx, tx, domain.SourceTournamentPlacement)
		if err != nil {
			return domain.CommandOutcome{}, err
		}
		stampPlayerRatingProjectionVersion(outbox, ver)
	}
	if err := insertOutboxEvents(ctx, tx, outbox); err != nil {
		if isUniqueViolation(err) {
			return s.placementConflictOrUnavailable(ctx, tx, cmd, placementKey, err)
		}
		return domain.CommandOutcome{}, err
	}

	body, err := json.Marshal(outcomeDTO(out))
	if err != nil {
		return domain.CommandOutcome{}, err
	}
	if placementKey != "" {
		if _, err := tx.Exec(ctx, `
			INSERT INTO ranking_command_responses (dedupe_kind, dedupe_key, response_json, created_at)
			VALUES ('tournament_placement', $1, $2, $3)
			ON CONFLICT DO NOTHING
		`, placementKey, body, now); err != nil {
			return s.placementConflictOrUnavailable(ctx, tx, cmd, placementKey, err)
		}
	}
	if cmd.EventID.Valid() {
		if _, err := tx.Exec(ctx, `
			INSERT INTO ranking_command_responses (dedupe_kind, dedupe_key, response_json, created_at)
			VALUES ('event_id', $1, $2, $3)
			ON CONFLICT DO NOTHING
		`, "placement:"+string(cmd.EventID), body, now); err != nil {
			return s.placementConflictOrUnavailable(ctx, tx, cmd, placementKey, err)
		}
	}

	if s.FailNextCommits > 0 {
		s.FailNextCommits--
		return domain.CommandOutcome{}, wrapUnavailable(fmt.Errorf("injected commit failure"))
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.CommandOutcome{}, wrapUnavailable(err)
	}
	return out, nil
}

func loadPlacementDuplicate(ctx context.Context, tx pgx.Tx, cmd domain.ApplyTournamentPlacementUpdateCommand, placementKey string) (domain.CommandOutcome, bool, error) {
	if placementKey != "" {
		if prior, ok, err := loadOutcomeResponse(ctx, tx, "tournament_placement", placementKey); err != nil {
			return domain.CommandOutcome{}, false, err
		} else if ok {
			prior.Kind = domain.OutcomeDuplicate
			return prior, true, nil
		}
	}
	if cmd.EventID.Valid() {
		if prior, ok, err := loadOutcomeResponse(ctx, tx, "event_id", "placement:"+string(cmd.EventID)); err != nil {
			return domain.CommandOutcome{}, false, err
		} else if ok {
			prior.Kind = domain.OutcomeDuplicate
			return prior, true, nil
		}
	}
	return domain.CommandOutcome{}, false, nil
}

func (s *RankingStore) placementConflictOrUnavailable(ctx context.Context, failed pgx.Tx, cmd domain.ApplyTournamentPlacementUpdateCommand, placementKey string, cause error) (domain.CommandOutcome, error) {
	if !isUniqueViolation(cause) {
		return domain.CommandOutcome{}, wrapUnavailable(cause)
	}
	_ = failed.Rollback(ctx)
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return domain.CommandOutcome{}, wrapUnavailable(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	eventKey := ""
	if cmd.EventID.Valid() {
		eventKey = string(cmd.EventID)
	}
	if err := acquireXactLocks(ctx, tx, placementIngestLockKeys(placementKey, eventKey)...); err != nil {
		return domain.CommandOutcome{}, err
	}
	if prior, ok, err := loadPlacementDuplicate(ctx, tx, cmd, placementKey); err != nil {
		return domain.CommandOutcome{}, err
	} else if ok {
		return prior, nil
	}
	return domain.CommandOutcome{}, wrapUnavailable(cause)
}

// commitCasualIgnored persists a stable eligibility-rejected disposition plus
// processed event and (playerId, gameId) contract keys without mutating ratings,
// history, or outbox. Exact redelivery resolves via loadCasualDuplicate.
func (s *RankingStore) commitCasualIgnored(ctx context.Context, tx pgx.Tx, req GameCompletedRequest, rej *domain.Rejection) (GameCompletedResult, error) {
	result := GameCompletedResult{
		Kind: domain.OutcomeRejected, CommandID: req.CommandID, EventID: req.EventID, Rejection: rej,
	}
	now := time.Now().UTC()
	if err := persistResponses(ctx, tx, req, result); err != nil {
		return GameCompletedResult{}, err
	}
	if req.EventID.Valid() {
		if _, err := tx.Exec(ctx, `
			INSERT INTO processed_upstream_events (event_id, topic, consumer_group, processed_at)
			VALUES ($1, $2, 'ranking', $3)
		`, string(req.EventID), topicCasualIngest, now); err != nil {
			return s.casualConflictOrUnavailable(ctx, tx, req, err)
		}
	}
	if req.GameID.Valid() {
		seen := map[string]struct{}{}
		for _, p := range req.Participants {
			if !p.PlayerID.Valid() {
				continue
			}
			pid := string(p.PlayerID)
			if _, dup := seen[pid]; dup {
				continue
			}
			seen[pid] = struct{}{}
			if _, err := tx.Exec(ctx, `
				INSERT INTO processed_casual_elo_keys (player_id, game_id, upstream_event_id, disposition, processed_at)
				VALUES ($1, $2, $3, $4, $5)
			`, pid, string(req.GameID), string(req.EventID), casualKeyDispositionIgnored, now); err != nil {
				return s.casualConflictOrUnavailable(ctx, tx, req, err)
			}
		}
	}
	if s.FailNextCommits > 0 {
		s.FailNextCommits--
		return GameCompletedResult{}, wrapUnavailable(fmt.Errorf("injected commit failure"))
	}
	if err := tx.Commit(ctx); err != nil {
		return GameCompletedResult{}, wrapUnavailable(err)
	}
	return result, nil
}

func validateCasualEligibility(req GameCompletedRequest) *domain.Rejection {
	if !req.Authoritative {
		return &domain.Rejection{Code: domain.RejectNotAuthoritative, Message: "casual elo requires an authoritative GameCompleted"}
	}
	if !req.Completed {
		return &domain.Rejection{Code: domain.RejectIneligibleGame, Message: "casual elo requires a completed game"}
	}
	if req.IsAbandoned {
		return &domain.Rejection{Code: domain.RejectAbandonedGame, Message: "abandoned games do not update casual elo"}
	}
	if req.RoomType == domain.RoomTypeTournament {
		return &domain.Rejection{Code: domain.RejectTournamentGame, Message: "tournament games do not update casual elo"}
	}
	if req.RoomType != domain.RoomTypeAdHoc {
		return &domain.Rejection{Code: domain.RejectIneligibleGame, Message: "casual elo requires roomType=ad_hoc"}
	}
	if !req.CommandID.Valid() || !req.GameID.Valid() {
		return &domain.Rejection{Code: domain.RejectInvalidIdentity, Message: "casual elo requires commandId and gameId"}
	}
	if len(req.Participants) < 2 {
		return &domain.Rejection{Code: domain.RejectInvalidOpponents, Message: "pairwise elo requires at least two participants"}
	}
	return nil
}
