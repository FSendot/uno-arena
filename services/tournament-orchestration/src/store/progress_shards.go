package store

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"

	"unoarena/services/tournament-orchestration/domain"
)

// rebuildRoundProgressShardsTx deletes and reinserts all 64 shards per round from
// authoritative assigned_matches / bracket_slots / match_results / advancement_records state.
// SUM(assigned_count) must equal assigned matches after every legacy persist commit.
// advancing_count is derived from advancement_records so compatibility rewrites preserve exact counts.
func rebuildRoundProgressShardsTx(ctx context.Context, tx pgx.Tx, tid string) error {
	if _, err := tx.Exec(ctx, `DELETE FROM round_progress_shards WHERE tournament_id = $1`, tid); err != nil {
		return err
	}
	rows, err := tx.Query(ctx, `
		SELECT round_number FROM tournament_rounds WHERE tournament_id = $1 ORDER BY round_number
	`, tid)
	if err != nil {
		return err
	}
	var rounds []int
	for rows.Next() {
		var rn int
		if err := rows.Scan(&rn); err != nil {
			rows.Close()
			return err
		}
		rounds = append(rounds, rn)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}

	for _, rn := range rounds {
		counts := make([]shardCounts, domain.ProgressShardCount)
		srows, err := tx.Query(ctx, `
			SELECT s.slot_index, s.status, (a.room_id IS NOT NULL)
			FROM bracket_slots s
			LEFT JOIN assigned_matches a
			  ON a.tournament_id = s.tournament_id
			 AND a.round_number = s.round_number
			 AND a.slot_id = s.slot_id
			WHERE s.tournament_id = $1 AND s.round_number = $2
		`, tid, rn)
		if err != nil {
			return err
		}
		for srows.Next() {
			var idx int
			var status string
			var hasRoom bool
			if err := srows.Scan(&idx, &status, &hasRoom); err != nil {
				srows.Close()
				return err
			}
			shard := domain.ProgressShardID(idx)
			if hasRoom {
				counts[shard].assigned++
			}
			switch domain.SlotStatus(status) {
			case domain.SlotResultRecorded, domain.SlotAdvanced:
				counts[shard].resolved++
			case domain.SlotQuarantined:
				counts[shard].quarantined++
			}
		}
		srows.Close()
		if err := srows.Err(); err != nil {
			return err
		}

		// Derive advancing_count from advancement_records (exact parity with differential increments).
		arows, err := tx.Query(ctx, `
			SELECT s.slot_index, COALESCE(cardinality(ar.advancing_player_ids), 0)
			FROM advancement_records ar
			JOIN bracket_slots s
			  ON s.tournament_id = ar.tournament_id
			 AND s.round_number = ar.round_number
			 AND s.slot_id = ar.slot_id
			WHERE ar.tournament_id = $1 AND ar.round_number = $2
		`, tid, rn)
		if err != nil {
			return err
		}
		for arows.Next() {
			var idx, n int
			if err := arows.Scan(&idx, &n); err != nil {
				arows.Close()
				return err
			}
			counts[domain.ProgressShardID(idx)].advancing += n
		}
		arows.Close()
		if err := arows.Err(); err != nil {
			return err
		}

		for shardID := 0; shardID < domain.ProgressShardCount; shardID++ {
			c := counts[shardID]
			if _, err := tx.Exec(ctx, `
				INSERT INTO round_progress_shards (
					tournament_id, round_number, shard_id,
					assigned_count, resolved_count, quarantined_count, advancing_count
				) VALUES ($1, $2, $3, $4, $5, $6, $7)
			`, tid, rn, shardID, c.assigned, c.resolved, c.quarantined, c.advancing); err != nil {
				return fmt.Errorf("insert progress shard %d round %d: %w", shardID, rn, err)
			}
		}
	}
	return nil
}

type shardCounts struct {
	assigned, resolved, quarantined, advancing int
}

// RoundProgressReadiness is the O(64)+bounded-index view for CompleteRound gating (no auto-emit).
type RoundProgressReadiness struct {
	TournamentID       string
	RoundNumber        int
	AssignedCount      int
	ResolvedCount      int
	QuarantinedCount   int
	AdvancingCount     int
	RoundStatus        string
	QuarantinedBatches int
	Ready              bool
}

// LoadRoundProgressReadiness sums the 64 shards and checks round/batch gates (never emits RoundCompleted).
// Ready only when status=in_progress, assigned>0, resolved==assigned, quarantined==0,
// quarantined batches==0, and advancing_count>0.
// One QueryRow snapshot: concurrent round/batch mutations cannot mix across separate reads.
func (s *TournamentStore) LoadRoundProgressReadiness(ctx context.Context, tournamentID string, roundNumber int) (RoundProgressReadiness, error) {
	out := RoundProgressReadiness{TournamentID: tournamentID, RoundNumber: roundNumber}
	if s == nil || s.pool == nil {
		return out, fmt.Errorf("nil store")
	}
	var roundStatus *string
	err := s.pool.QueryRow(ctx, `
		WITH shard_sums AS (
			SELECT
				COALESCE(SUM(assigned_count), 0)::int AS assigned_count,
				COALESCE(SUM(resolved_count), 0)::int AS resolved_count,
				COALESCE(SUM(quarantined_count), 0)::int AS quarantined_count,
				COALESCE(SUM(advancing_count), 0)::int AS advancing_count
			FROM round_progress_shards
			WHERE tournament_id = $1 AND round_number = $2
		)
		SELECT
			s.assigned_count,
			s.resolved_count,
			s.quarantined_count,
			s.advancing_count,
			(SELECT status FROM tournament_rounds
			 WHERE tournament_id = $1 AND round_number = $2),
			(SELECT COUNT(*)::int FROM provisioning_batches
			 WHERE tournament_id = $1 AND round_number = $2 AND status = 'quarantined')
		FROM shard_sums s
	`, tournamentID, roundNumber).Scan(
		&out.AssignedCount,
		&out.ResolvedCount,
		&out.QuarantinedCount,
		&out.AdvancingCount,
		&roundStatus,
		&out.QuarantinedBatches,
	)
	if err != nil {
		return out, err
	}
	if roundStatus != nil {
		out.RoundStatus = *roundStatus
	}
	out.Ready = out.RoundStatus == string(domain.RoundInProgress) &&
		out.AssignedCount > 0 &&
		out.ResolvedCount == out.AssignedCount &&
		out.QuarantinedCount == 0 &&
		out.QuarantinedBatches == 0 &&
		out.AdvancingCount > 0
	return out, nil
}

// ReadyRoundCandidate is a FindReadyRoundCandidate hint (CompleteRound TX revalidates).
type ReadyRoundCandidate struct {
	TournamentID string
	RoundNumber  int
}

// FindReadyRoundCandidate returns at most one in_progress round whose O(64) gates look ready.
// Hint only — never completes a round. Uses partial index on status=in_progress.
// Excludes terminal tournaments and shard/advancement_records advancing_count drift so a
// cancelled or corrupt first row cannot starve later healthy candidates (LIMIT 1 poll).
func (s *TournamentStore) FindReadyRoundCandidate(ctx context.Context) (*ReadyRoundCandidate, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("nil store")
	}
	var tid string
	var rn int
	err := s.pool.QueryRow(ctx, `
		SELECT r.tournament_id, r.round_number
		FROM tournament_rounds r
		INNER JOIN tournaments t ON t.tournament_id = r.tournament_id
		WHERE t.phase NOT IN ('completed', 'cancelled')
		  AND r.status = 'in_progress'
		  AND EXISTS (
			SELECT 1
			FROM round_progress_shards s
			WHERE s.tournament_id = r.tournament_id AND s.round_number = r.round_number
			HAVING COALESCE(SUM(s.assigned_count), 0) > 0
			   AND COALESCE(SUM(s.resolved_count), 0) = COALESCE(SUM(s.assigned_count), 0)
			   AND COALESCE(SUM(s.quarantined_count), 0) = 0
			   AND COALESCE(SUM(s.advancing_count), 0) > 0
			   AND COALESCE(SUM(s.advancing_count), 0) = (
				SELECT COALESCE(SUM(cardinality(ar.advancing_player_ids)), 0)
				FROM advancement_records ar
				WHERE ar.tournament_id = r.tournament_id
				  AND ar.round_number = r.round_number
			   )
			   AND COALESCE(SUM(s.advancing_count), 0) = (
				SELECT COUNT(*)::bigint
				FROM round_advancing_players ap
				WHERE ap.tournament_id = r.tournament_id
				  AND ap.source_round_number = r.round_number
			   )
		  )
		  AND NOT EXISTS (
			SELECT 1 FROM provisioning_batches b
			WHERE b.tournament_id = r.tournament_id
			  AND b.round_number = r.round_number
			  AND b.status = 'quarantined'
		  )
		ORDER BY r.tournament_id, r.round_number
		LIMIT 1
	`).Scan(&tid, &rn)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, wrapUnavailable(err)
	}
	return &ReadyRoundCandidate{TournamentID: tid, RoundNumber: rn}, nil
}
