package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"unoarena/services/tournament-orchestration/domain"
)

// ErrMalformedStandings is returned when a recorded final ranked_result cannot be
// decoded into a valid ordered finalStandings list (HTTP 503 fail-closed).
var ErrMalformedStandings = errors.New("malformed final standings")

// StandingsProjection is the bounded public tournament standings read model.
// finalStandings is empty until a disposition=recorded final-round result exists;
// when present it is ordered player IDs from ranked_result.standings (max 10).
type StandingsProjection struct {
	TournamentID      string    `json:"tournamentId"`
	ProjectionVersion int64     `json:"projectionVersion"`
	GeneratedAt       time.Time `json:"generatedAt"`
	Phase             string    `json:"phase"`
	RegisteredCount   int       `json:"registeredCount"`
	CurrentRound      int       `json:"currentRound"`
	FinalStandings    []string  `json:"finalStandings"`
}

// LoadStandingsProjection loads a consistent standings snapshot in one QueryRow.
// It never hydrates the whole tournament aggregate or registration/round graphs.
func (s *TournamentStore) LoadStandingsProjection(ctx context.Context, tournamentID string) (StandingsProjection, error) {
	if s == nil || s.pool == nil {
		return StandingsProjection{}, fmt.Errorf("nil store")
	}
	return loadStandingsProjectionQ(ctx, s.pool, tournamentID)
}

func loadStandingsProjectionQ(ctx context.Context, q dbQuerier, tournamentID string) (StandingsProjection, error) {
	var (
		found             bool
		phase             string
		rulesRaw          []byte
		registeredCount   int
		projectionVersion int64
		generatedAt       *time.Time
		finalCount        int
		rankedRaw         []byte
	)
	// Single statement: existence, phase/rules, shard SUM, projection checkpoint,
	// and final-round recorded ranked_result. Fields cannot mix committed snapshots.
	err := q.QueryRow(ctx, `
		WITH t AS (
			SELECT tournament_id, phase, rules
			FROM tournaments
			WHERE tournament_id = $1
		),
		reg AS (
			SELECT COALESCE(SUM(count), 0)::int AS registered_count
			FROM tournament_registration_shards
			WHERE tournament_id = $1
		),
		proj AS (
			SELECT
				COALESCE((SELECT projection_version FROM bracket_projection_versions WHERE tournament_id = $1), 0)
				+ COALESCE((SELECT SUM(version) FROM bracket_projection_shards WHERE tournament_id = $1), 0)
					AS projection_version,
				GREATEST(
					(SELECT generated_at FROM bracket_projection_versions WHERE tournament_id = $1),
					(SELECT MAX(generated_at) FROM bracket_projection_shards WHERE tournament_id = $1)
				) AS generated_at
		),
		-- Bound to two candidates: a valid tournament has at most one recorded
		-- final result. count>1 is inconsistent persisted state (fail closed).
		final_candidates AS (
			SELECT mr.ranked_result
			FROM match_results mr
			INNER JOIN tournament_rounds tr
				ON tr.tournament_id = mr.tournament_id
				AND tr.round_number = mr.round_number
				AND tr.is_final = true
			WHERE mr.tournament_id = $1
				AND mr.disposition = 'recorded'
			LIMIT 2
		),
		final_result AS (
			SELECT
				COUNT(*)::int AS final_count,
				(ARRAY_AGG(ranked_result))[1] AS ranked_result
			FROM final_candidates
		)
		SELECT
			EXISTS(SELECT 1 FROM t),
			COALESCE((SELECT phase FROM t), ''),
			COALESCE((SELECT rules FROM t), '{}'::jsonb),
			(SELECT registered_count FROM reg),
			(SELECT projection_version FROM proj),
			(SELECT generated_at FROM proj),
			(SELECT final_count FROM final_result),
			(SELECT ranked_result FROM final_result)
	`, tournamentID).Scan(
		&found, &phase, &rulesRaw, &registeredCount, &projectionVersion, &generatedAt, &finalCount, &rankedRaw,
	)
	if err != nil {
		return StandingsProjection{}, wrapUnavailable(err)
	}
	if !found {
		return StandingsProjection{}, ErrTournamentNotFound
	}
	if finalCount > 1 {
		return StandingsProjection{}, fmt.Errorf("%w: multiple recorded final results", ErrMalformedStandings)
	}

	var rules tournamentRules
	jsonUnmarshalRules(rulesRaw, &rules)

	finalStandings := []string{}
	if finalCount == 1 && len(rankedRaw) > 0 {
		ids, parseErr := finalStandingsFromRankedResult(rankedRaw)
		if parseErr != nil {
			return StandingsProjection{}, parseErr
		}
		finalStandings = ids
	}

	genAt := time.Time{}
	if generatedAt != nil {
		genAt = generatedAt.UTC()
	}
	return StandingsProjection{
		TournamentID:      tournamentID,
		ProjectionVersion: projectionVersion,
		GeneratedAt:       genAt,
		Phase:             phase,
		RegisteredCount:   registeredCount,
		CurrentRound:      rules.CurrentRound,
		FinalStandings:    finalStandings,
	}, nil
}

// finalStandingsFromRankedResult extracts ordered player IDs from match_results.ranked_result,
// preserving standings array order. Fail-closed on malformed / oversized payloads.
func finalStandingsFromRankedResult(raw []byte) ([]string, error) {
	var payload struct {
		Standings []struct {
			PlayerID string `json:"playerId"`
		} `json:"standings"`
	}
	if err := json.Unmarshal(raw, &payload); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrMalformedStandings, err)
	}
	if payload.Standings == nil {
		return nil, fmt.Errorf("%w: missing standings", ErrMalformedStandings)
	}
	ids := make([]domain.PlayerID, 0, len(payload.Standings))
	for _, s := range payload.Standings {
		ids = append(ids, domain.PlayerID(s.PlayerID))
	}
	if err := domain.ValidateFinalStandings(ids); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrMalformedStandings, err)
	}
	out := make([]string, len(ids))
	for i, id := range ids {
		out[i] = string(id)
	}
	return out, nil
}
