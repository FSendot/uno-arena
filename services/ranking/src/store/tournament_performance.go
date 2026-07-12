package store

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"unoarena/services/ranking/domain"
)

const (
	topicPlayersAdvanced            = "tournament.players.advanced"
	topicTournamentCompleted        = "tournament.completed"
	maxAdvancementFanout            = 3
	maxFinalStandingFanout          = 10
	dedupeKindTournamentPerformance = "tournament_performance"
)

// TournamentPlayerPerformance is one affected player inside an event-wide apply.
type TournamentPlayerPerformance struct {
	PlayerID    domain.PlayerID
	Placement   int // final standing 1..10; 0 for advancement
	RoundNumber int // advancement depth; 0 for final standing
	Reason      domain.RatingHistoryReason
}

// TournamentPerformanceRequest is the event-wide tournament performance apply contract.
type TournamentPerformanceRequest struct {
	SourceTopic        string
	UpstreamEventID    domain.EventID
	BusinessKey        string
	PayloadFingerprint string
	TournamentID       domain.TournamentID
	CorrelationID      string
	CausationID        string
	Players            []TournamentPlayerPerformance
}

// TournamentPerformanceResult is the atomic multi-player tournament outcome.
type TournamentPerformanceResult struct {
	Kind            domain.OutcomeKind
	UpstreamEventID domain.EventID
	BusinessKey     string
	Facts           []domain.Fact
	Rejection       *domain.Rejection
	PerPlayer       []domain.CommandOutcome
	ScoreChanged    bool
}

// PlayersAdvancedBusinessKey encodes ADR-0036 (tournamentId, roundNumber, sourceSlotId).
func PlayersAdvancedBusinessKey(tournamentID string, roundNumber int, sourceSlotID string) (string, error) {
	b, err := json.Marshal([]any{tournamentID, roundNumber, sourceSlotID})
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// TournamentCompletedBusinessKey encodes ADR-0036 eventId business key.
func TournamentCompletedBusinessKey(eventID string) string {
	return strings.TrimSpace(eventID)
}

// ApplyTournamentPerformance applies one upstream tournament fact atomically across all players.
func (s *RankingStore) ApplyTournamentPerformance(ctx context.Context, req TournamentPerformanceRequest) (TournamentPerformanceResult, error) {
	if rej := validateTournamentPerformanceRequest(req); rej != nil {
		return TournamentPerformanceResult{
			Kind: domain.OutcomeRejected, UpstreamEventID: req.UpstreamEventID,
			BusinessKey: req.BusinessKey, Rejection: rej,
		}, nil
	}

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return TournamentPerformanceResult{}, wrapUnavailable(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	eventKey := string(req.UpstreamEventID)
	if err := acquireXactLocks(ctx, tx, performanceIngestLockKeys(req.SourceTopic, req.BusinessKey, eventKey)...); err != nil {
		return TournamentPerformanceResult{}, err
	}

	if prior, ok, err := loadTournamentPerformanceDuplicate(ctx, tx, req); err != nil {
		return TournamentPerformanceResult{}, err
	} else if ok {
		return prior, nil
	}

	ids := make([]string, 0, len(req.Players))
	seen := map[string]struct{}{}
	byID := map[string]TournamentPlayerPerformance{}
	for _, p := range req.Players {
		if !p.PlayerID.Valid() {
			rej := domain.Rejection{Code: domain.RejectInvalidIdentity, Message: "each participant requires playerId"}
			return TournamentPerformanceResult{
				Kind: domain.OutcomeRejected, UpstreamEventID: req.UpstreamEventID,
				BusinessKey: req.BusinessKey, Rejection: &rej,
			}, nil
		}
		pid := string(p.PlayerID)
		if _, dup := seen[pid]; dup {
			rej := domain.Rejection{Code: domain.RejectInvalidOpponents, Message: "duplicate participant playerId"}
			return TournamentPerformanceResult{
				Kind: domain.OutcomeRejected, UpstreamEventID: req.UpstreamEventID,
				BusinessKey: req.BusinessKey, Rejection: &rej,
			}, nil
		}
		if _, err := domain.ComputeTournamentAward(p.Reason, p.Placement, p.RoundNumber); err != nil {
			rej := domain.Rejection{Code: domain.RejectInvalidCommand, Message: err.Error()}
			return TournamentPerformanceResult{
				Kind: domain.OutcomeRejected, UpstreamEventID: req.UpstreamEventID,
				BusinessKey: req.BusinessKey, Rejection: &rej,
			}, nil
		}
		seen[pid] = struct{}{}
		ids = append(ids, pid)
		byID[pid] = p
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
			return TournamentPerformanceResult{}, wrapUnavailable(err)
		}
	}

	ratings := make(map[string]int, len(ids))
	casuals := make(map[string]int, len(ids))
	for _, id := range ids {
		var casual, tournament int
		if err := tx.QueryRow(ctx, `
			SELECT casual_elo, tournament_placement_rating
			FROM player_ratings WHERE player_id = $1 FOR UPDATE
		`, id).Scan(&casual, &tournament); err != nil {
			return TournamentPerformanceResult{}, wrapUnavailable(err)
		}
		casuals[id] = casual
		ratings[id] = tournament
	}

	rawEventID := string(req.UpstreamEventID)
	placementEventID := domain.PlacementEventID(rawEventID)
	var allFacts []domain.Fact
	var perPlayer []domain.CommandOutcome
	var outbox []OutboxEvent
	scoreChanged := false
	now := time.Now().UTC()
	aggregates := make(map[string]*domain.PlayerRating, len(ids))

	for _, id := range ids {
		part := byID[id]
		agg := domain.RestorePlayerRating(domain.PlayerID(id), cfg, casuals[id], ratings[id])
		aggregates[id] = agg
		cmd := domain.ApplyTournamentPlacementUpdateCommand{
			CommandID:        domain.CommandID(rawEventID + ":" + id),
			EventID:          domain.EventID(rawEventID + ":" + id),
			PlayerID:         domain.PlayerID(id),
			TournamentID:     req.TournamentID,
			PlacementEventID: placementEventID,
			Placement:        part.Placement,
			RoundNumber:      part.RoundNumber,
			Reason:           part.Reason,
		}
		out := agg.ApplyTournamentPlacementUpdate(cmd)
		perPlayer = append(perPlayer, out)
		if out.Kind != domain.OutcomeAccepted && out.Kind != domain.OutcomeDuplicate {
			rej := out.Rejection
			if rej == nil {
				r := domain.Rejection{Code: domain.RejectInvalidCommand, Message: "participant apply failed"}
				rej = &r
			}
			return TournamentPerformanceResult{
				Kind: domain.OutcomeRejected, UpstreamEventID: req.UpstreamEventID,
				BusinessKey: req.BusinessKey, Rejection: rej, PerPlayer: perPlayer,
			}, nil
		}
		if out.Kind != domain.OutcomeAccepted {
			continue
		}
		allFacts = append(allFacts, out.Facts...)
		snap := agg.PublicSnapshot()
		if snap.TournamentPlacementRating > ratings[id] {
			scoreChanged = true
		}
		if _, err := tx.Exec(ctx, `
			UPDATE player_ratings
			SET tournament_placement_rating = $2,
			    tournament_events_applied = tournament_events_applied + 1,
			    updated_at = now()
			WHERE player_id = $1
		`, id, snap.TournamentPlacementRating); err != nil {
			return TournamentPerformanceResult{}, wrapUnavailable(err)
		}
		hist := agg.History()
		last := hist[len(hist)-1]
		var placement any
		var depth any
		if last.Reason == domain.ReasonTournamentAdvancement {
			depth = last.AdvancementDepth
		} else {
			placement = last.Placement
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO rating_history (
				player_id, source_type, previous_rating, new_rating, delta, reason,
				tournament_id, placement_event_id, placement, advancement_depth, upstream_event_id, applied_at
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
		`, id, string(domain.SourceTournamentPlacement), last.PreviousRating, last.NewRating, last.Delta,
			string(last.Reason), string(req.TournamentID), string(placementEventID), placement, depth,
			rawEventID, now); err != nil {
			return TournamentPerformanceResult{}, wrapUnavailable(err)
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO processed_tournament_placement_keys (
				player_id, tournament_id, placement_event_id, upstream_event_id, processed_at
			) VALUES ($1, $2, $3, $4, $5)
		`, id, string(req.TournamentID), string(placementEventID), rawEventID, now); err != nil {
			if isUniqueViolation(err) {
				return s.tournamentPerformanceConflictOrUnavailable(ctx, tx, req, err)
			}
			return TournamentPerformanceResult{}, wrapUnavailable(err)
		}
		causation := strings.TrimSpace(req.CausationID)
		if causation == "" {
			causation = rawEventID
		}
		for _, f := range out.Facts {
			ev, err := outboxFromFact(f, outboxMeta{
				UpstreamEventID: rawEventID,
				CorrelationID:   req.CorrelationID,
				CausationID:     causation,
				Now:             now,
			})
			if err != nil {
				return TournamentPerformanceResult{}, err
			}
			outbox = append(outbox, ev)
		}
	}

	// Score-changing TX dirties the board once; snapshotter publishes coalesced top-100 (ADR-0038).
	// Zero-delta facts still emit PlayerRatingUpdated but must not bump dirty_version.
	if scoreChanged {
		ver, err := markBoardDirty(ctx, tx, domain.SourceTournamentPlacement)
		if err != nil {
			return TournamentPerformanceResult{}, err
		}
		stampPlayerRatingProjectionVersion(outbox, ver)
	}

	if req.UpstreamEventID.Valid() {
		if _, err := tx.Exec(ctx, `
			INSERT INTO processed_upstream_events (event_id, topic, consumer_group, processed_at)
			VALUES ($1, $2, 'ranking', $3)
		`, rawEventID, req.SourceTopic, now); err != nil {
			if isUniqueViolation(err) {
				return s.tournamentPerformanceConflictOrUnavailable(ctx, tx, req, err)
			}
			return TournamentPerformanceResult{}, wrapUnavailable(err)
		}
	}

	result := TournamentPerformanceResult{
		Kind: domain.OutcomeAccepted, UpstreamEventID: req.UpstreamEventID,
		BusinessKey: req.BusinessKey, Facts: allFacts, PerPlayer: perPlayer, ScoreChanged: scoreChanged,
	}
	if err := insertOutboxEvents(ctx, tx, outbox); err != nil {
		if isUniqueViolation(err) {
			return s.tournamentPerformanceConflictOrUnavailable(ctx, tx, req, err)
		}
		return TournamentPerformanceResult{}, err
	}
	if err := persistTournamentPerformanceOutcome(ctx, tx, req, result, now); err != nil {
		if isUniqueViolation(err) {
			return s.tournamentPerformanceConflictOrUnavailable(ctx, tx, req, err)
		}
		return TournamentPerformanceResult{}, err
	}

	if s.FailNextCommits > 0 {
		s.FailNextCommits--
		return TournamentPerformanceResult{}, wrapUnavailable(fmt.Errorf("injected commit failure"))
	}
	if err := tx.Commit(ctx); err != nil {
		return TournamentPerformanceResult{}, wrapUnavailable(err)
	}
	return result, nil
}

func validateTournamentPerformanceRequest(req TournamentPerformanceRequest) *domain.Rejection {
	if strings.TrimSpace(req.SourceTopic) == "" || strings.TrimSpace(req.BusinessKey) == "" ||
		strings.TrimSpace(req.PayloadFingerprint) == "" || !req.UpstreamEventID.Valid() || !req.TournamentID.Valid() {
		return &domain.Rejection{Code: domain.RejectInvalidIdentity, Message: "tournament performance requires topic, business key, fingerprint, eventId, tournamentId"}
	}
	n := len(req.Players)
	switch req.SourceTopic {
	case topicPlayersAdvanced:
		if n < 1 || n > maxAdvancementFanout {
			return &domain.Rejection{Code: domain.RejectInvalidOpponents, Message: "players advanced fanout must be 1..3"}
		}
	case topicTournamentCompleted:
		if n < 1 || n > maxFinalStandingFanout {
			return &domain.Rejection{Code: domain.RejectInvalidOpponents, Message: "tournament completed fanout must be 1..10"}
		}
	default:
		if n < 1 {
			return &domain.Rejection{Code: domain.RejectInvalidOpponents, Message: "tournament performance requires at least one player"}
		}
	}
	return nil
}

func loadTournamentPerformanceDuplicate(ctx context.Context, tx pgx.Tx, req TournamentPerformanceRequest) (TournamentPerformanceResult, bool, error) {
	var fp string
	var raw []byte
	err := tx.QueryRow(ctx, `
		SELECT payload_fingerprint, outcome_json
		FROM processed_tournament_performance_events
		WHERE consumer_group = 'ranking' AND source_topic = $1 AND business_key = $2
	`, req.SourceTopic, req.BusinessKey).Scan(&fp, &raw)
	if err == pgx.ErrNoRows {
		return TournamentPerformanceResult{}, false, nil
	}
	if err != nil {
		return TournamentPerformanceResult{}, false, wrapUnavailable(err)
	}
	if fp != req.PayloadFingerprint {
		return TournamentPerformanceResult{}, false, &TournamentPerformanceConflictError{
			SourceTopic: req.SourceTopic, BusinessKey: req.BusinessKey,
			ExistingFingerprint: fp, IncomingFingerprint: req.PayloadFingerprint,
		}
	}
	var dto tournamentPerformanceDTO
	if err := json.Unmarshal(raw, &dto); err != nil {
		return TournamentPerformanceResult{}, false, err
	}
	out := dto.toResult()
	out.Kind = domain.OutcomeDuplicate
	return out, true, nil
}

func persistTournamentPerformanceOutcome(ctx context.Context, tx pgx.Tx, req TournamentPerformanceRequest, result TournamentPerformanceResult, now time.Time) error {
	body, err := json.Marshal(tournamentPerformanceResultDTO(result))
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO processed_tournament_performance_events (
			consumer_group, source_topic, business_key, upstream_event_id,
			payload_fingerprint, outcome_json, processed_at
		) VALUES ('ranking', $1, $2, $3, $4, $5, $6)
	`, req.SourceTopic, req.BusinessKey, string(req.UpstreamEventID), req.PayloadFingerprint, body, now); err != nil {
		return wrapUnavailable(err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO ranking_command_responses (dedupe_kind, dedupe_key, response_json, created_at)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT DO NOTHING
	`, dedupeKindTournamentPerformance, req.SourceTopic+":"+req.BusinessKey, body, now); err != nil {
		return wrapUnavailable(err)
	}
	return nil
}

func (s *RankingStore) tournamentPerformanceConflictOrUnavailable(ctx context.Context, failed pgx.Tx, req TournamentPerformanceRequest, cause error) (TournamentPerformanceResult, error) {
	if !isUniqueViolation(cause) {
		return TournamentPerformanceResult{}, wrapUnavailable(cause)
	}
	_ = failed.Rollback(ctx)
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return TournamentPerformanceResult{}, wrapUnavailable(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := acquireXactLocks(ctx, tx, performanceIngestLockKeys(req.SourceTopic, req.BusinessKey, string(req.UpstreamEventID))...); err != nil {
		return TournamentPerformanceResult{}, err
	}
	if prior, ok, err := loadTournamentPerformanceDuplicate(ctx, tx, req); err != nil {
		return TournamentPerformanceResult{}, err
	} else if ok {
		return prior, nil
	}
	return TournamentPerformanceResult{}, wrapUnavailable(cause)
}

type tournamentPerformanceDTO struct {
	Kind            string              `json:"kind"`
	UpstreamEventID string              `json:"upstreamEventId"`
	BusinessKey     string              `json:"businessKey"`
	Facts           []factDTO           `json:"facts"`
	Rejection       *rejectionDTO       `json:"rejection,omitempty"`
	PerPlayer       []commandOutcomeDTO `json:"participants,omitempty"`
	ScoreChanged    bool                `json:"scoreChanged"`
}

func tournamentPerformanceResultDTO(r TournamentPerformanceResult) tournamentPerformanceDTO {
	dto := tournamentPerformanceDTO{
		Kind: string(r.Kind), UpstreamEventID: string(r.UpstreamEventID),
		BusinessKey: r.BusinessKey, Facts: factsDTO(r.Facts), ScoreChanged: r.ScoreChanged,
	}
	if r.Rejection != nil {
		dto.Rejection = &rejectionDTO{Code: string(r.Rejection.Code), Message: r.Rejection.Message}
	}
	for _, p := range r.PerPlayer {
		dto.PerPlayer = append(dto.PerPlayer, outcomeDTO(p))
	}
	return dto
}

func (d tournamentPerformanceDTO) toResult() TournamentPerformanceResult {
	r := TournamentPerformanceResult{
		Kind: domain.OutcomeKind(d.Kind), UpstreamEventID: domain.EventID(d.UpstreamEventID),
		BusinessKey: d.BusinessKey, Facts: toFacts(d.Facts), ScoreChanged: d.ScoreChanged,
	}
	if d.Rejection != nil {
		r.Rejection = &domain.Rejection{Code: domain.RejectionCode(d.Rejection.Code), Message: d.Rejection.Message}
	}
	for _, p := range d.PerPlayer {
		r.PerPlayer = append(r.PerPlayer, p.toOutcome())
	}
	return r
}
