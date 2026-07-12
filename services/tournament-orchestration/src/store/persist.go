package store

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"unoarena/services/tournament-orchestration/domain"
)

func persistTournamentTx(ctx context.Context, tx pgx.Tx, t *domain.Tournament, matchSource *MatchResultSource) error {
	tid := string(t.ID())
	now := time.Now().UTC()
	rules, err := json.Marshal(tournamentRules{
		RetryBudget:  t.RetryBudget(),
		BatchSize:    t.BatchSize(),
		CurrentRound: t.CurrentRound(),
		ChampionID:   string(t.Champion()),
	})
	if err != nil {
		return err
	}
	var completedAt any
	if t.Phase().IsTerminal() {
		completedAt = now
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO tournaments (
			tournament_id, phase, capacity, registered_count, visibility, rules, created_at, updated_at, completed_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $7, $8)
		ON CONFLICT (tournament_id) DO UPDATE SET
			phase = EXCLUDED.phase,
			capacity = EXCLUDED.capacity,
			registered_count = EXCLUDED.registered_count,
			visibility = EXCLUDED.visibility,
			rules = EXCLUDED.rules,
			updated_at = EXCLUDED.updated_at,
			completed_at = COALESCE(EXCLUDED.completed_at, tournaments.completed_at)
	`, tid, string(t.Phase()), t.Capacity(), t.RegisteredCount(), string(t.Visibility()), rules, now, completedAt)
	if err != nil {
		return err
	}

	if _, err := tx.Exec(ctx, `DELETE FROM round_advancing_players WHERE tournament_id = $1`, tid); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `DELETE FROM advancement_records WHERE tournament_id = $1`, tid); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `DELETE FROM match_results WHERE tournament_id = $1`, tid); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `DELETE FROM assigned_matches WHERE tournament_id = $1`, tid); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `DELETE FROM tournament_round_slot_players WHERE tournament_id = $1`, tid); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `DELETE FROM bracket_slots WHERE tournament_id = $1`, tid); err != nil {
		return err
	}
	// Capture active in_progress leases before delete/reinsert so concurrent workers on
	// other batches of this tournament keep reclaim-safe lease_owner/lease_expires_at.
	preservedLeases, err := loadActiveProvisioningLeases(ctx, tx, tid)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `DELETE FROM provisioning_batches WHERE tournament_id = $1`, tid); err != nil {
		return err
	}
	// Seeding jobs are tournament-owned (survive round rewrite). Never DELETE jobs/batches
	// here — ON DELETE CASCADE would erase audit state. Cancel active work when terminal.
	if t.Phase().IsTerminal() {
		if _, err := tx.Exec(ctx, `
			UPDATE round_seeding_jobs
			SET status = 'cancelled',
			    lease_owner = NULL,
			    lease_expires_at = NULL,
			    quarantine_reason = NULL,
			    completed_at = NULL,
			    updated_at = $2
			WHERE tournament_id = $1
			  AND status IN ('pending', 'in_progress')
		`, tid, now); err != nil {
			return err
		}
	}
	if _, err := tx.Exec(ctx, `DELETE FROM round_progress_shards WHERE tournament_id = $1`, tid); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `DELETE FROM tournament_rounds WHERE tournament_id = $1`, tid); err != nil {
		return err
	}
	// Preserve player→shard allocations across delete/reinsert (like provisioning leases).
	preservedShards, err := loadRegistrationShardAllocationsTx(ctx, tx, tid)
	if err != nil {
		return err
	}
	if err := ensureRegistrationShardsTx(ctx, tx, tid, t.Capacity()); err != nil {
		return err
	}
	quotas := domain.AllocateRegistrationQuotas(t.Capacity())
	usedCounts := make([]int, domain.RegistrationShardCount)
	type regAssign struct {
		pid   domain.PlayerID
		shard int
	}
	assigns := make([]regAssign, 0, len(t.RegisteredPlayers()))
	for _, pid := range t.RegisteredPlayers() {
		ps := string(pid)
		shard, ok := preservedShards[ps]
		if ok && shard >= 0 && shard < domain.RegistrationShardCount && usedCounts[shard] < quotas[shard] {
			// Keep preserved allocation.
		} else {
			var found bool
			shard, found = domain.AssignRegistrationShard(tid, ps, quotas, usedCounts)
			if !found {
				return fmt.Errorf("cannot allocate registration shard for player %s within capacity", ps)
			}
		}
		usedCounts[shard]++
		assigns = append(assigns, regAssign{pid: pid, shard: shard})
	}
	totalAssigned := 0
	for _, c := range usedCounts {
		totalAssigned += c
	}
	if totalAssigned > t.Capacity() {
		return fmt.Errorf("registration rewrite overcapacity: %d > %d", totalAssigned, t.Capacity())
	}

	if _, err := tx.Exec(ctx, `DELETE FROM tournament_registrations WHERE tournament_id = $1`, tid); err != nil {
		return err
	}

	for i, a := range assigns {
		regAt := now.Add(time.Duration(i) * time.Microsecond)
		if _, err := tx.Exec(ctx, `
			INSERT INTO tournament_registrations (tournament_id, player_id, shard_id, registered_at, status)
			VALUES ($1, $2, $3, $4, 'registered')
		`, tid, string(a.pid), a.shard, regAt); err != nil {
			return err
		}
	}
	if err := rebuildRegistrationShardCountsTx(ctx, tx, tid, t.Capacity()); err != nil {
		return err
	}

	for _, round := range t.RoundsSnapshot() {
		status := string(round.Status)
		var seededAt, roundCompleted any
		if round.Status != domain.RoundPending {
			seededAt = now
		}
		if round.Completed || round.Status == domain.RoundCompleted {
			roundCompleted = now
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO tournament_rounds (
				tournament_id, round_number, status, is_final, seeded_at, completed_at
			) VALUES ($1, $2, $3, $4, $5, $6)
		`, tid, round.Number, status, round.IsFinal, seededAt, roundCompleted); err != nil {
			return err
		}

		for _, batch := range round.Batches {
			var qReason, lastErr any
			if batch.QuarantineReason != "" {
				qReason = batch.QuarantineReason
			}
			if batch.LastError != "" {
				lastErr = batch.LastError
			}
			leaseOwner, leaseExpires, leaseVersion := provisioningLeaseForRewrite(batch, preservedLeases[leaseKey(round.Number, string(batch.BatchID))])
			if _, err := tx.Exec(ctx, `
				INSERT INTO provisioning_batches (
					tournament_id, round_number, batch_id, shard_key, status, retry_attempt,
					slot_id_from, slot_id_to, last_error, quarantine_reason,
					lease_owner, lease_expires_at, lease_version, created_at, updated_at
				) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $14)
			`, tid, round.Number, string(batch.BatchID), string(batch.BatchID), string(batch.Status),
				batch.RetryAttempt, string(batch.SlotFrom), string(batch.SlotTo), lastErr, qReason,
				leaseOwner, leaseExpires, leaseVersion, now); err != nil {
				return err
			}
		}

		for _, slot := range round.Slots {
			seeded := make([]string, len(slot.SeededPlayers))
			for i, p := range slot.SeededPlayers {
				seeded[i] = string(p)
			}
			if _, err := tx.Exec(ctx, `
				INSERT INTO bracket_slots (
					tournament_id, round_number, slot_id, slot_index, status, seeded_player_ids, created_at, updated_at
				) VALUES ($1, $2, $3, $4, $5, $6, $7, $7)
			`, tid, round.Number, string(slot.SlotID), slot.Index, string(slot.Status), seeded, now); err != nil {
				return err
			}
			for seat, pid := range slot.SeededPlayers {
				if _, err := tx.Exec(ctx, `
					INSERT INTO tournament_round_slot_players (
						tournament_id, round_number, player_id, slot_id, seat_index
					) VALUES ($1, $2, $3, $4, $5)
				`, tid, round.Number, string(pid), string(slot.SlotID), seat); err != nil {
					return err
				}
			}
			if slot.RoomID.Valid() {
				var batchID any
				if slot.BatchID.Valid() {
					batchID = string(slot.BatchID)
				}
				if _, err := tx.Exec(ctx, `
					INSERT INTO assigned_matches (
						tournament_id, round_number, slot_id, room_id, assigned_at, provisioning_batch_id
					) VALUES ($1, $2, $3, $4, $5, $6)
				`, tid, round.Number, string(slot.SlotID), string(slot.RoomID), now, batchID); err != nil {
					return err
				}
			}
		}
	}

	resultKeys := t.ResultKeysSnapshot()

	type resultRow struct {
		RoomID            string
		CompletionVersion uint64
		RoundNumber       int
		SlotID            string
		Disposition       domain.ResultDisposition
		Fingerprint       string
		Standings         []domain.PlayerMatchStanding
		QuarantineReason  string
		SourceEventID     string
		Advancing         []domain.PlayerID
	}
	byKey := map[string]resultRow{}

	for _, round := range t.RoundsSnapshot() {
		for _, slot := range round.Slots {
			if !slot.RoomID.Valid() {
				continue
			}
			if !slot.HasResult && slot.Status != domain.SlotQuarantined {
				continue
			}
			ver := uint64(slot.CompletionVersion)
			if ver == 0 && slot.Status == domain.SlotQuarantined {
				// Prefer resultKeys for quarantined completions.
				continue
			}
			key := string(slot.RoomID) + ":" + strconv.FormatUint(ver, 10)
			disp := domain.DispositionRecorded
			fp := slot.ResultFingerprint
			src := ""
			if rr, ok := resultKeys[key]; ok {
				disp = rr.Disposition
				if rr.Fingerprint != "" {
					fp = rr.Fingerprint
				}
				src = rr.SourceEventID
			} else if slot.Status == domain.SlotQuarantined {
				disp = domain.DispositionQuarantined
			}
			byKey[key] = resultRow{
				RoomID:            string(slot.RoomID),
				CompletionVersion: ver,
				RoundNumber:       round.Number,
				SlotID:            string(slot.SlotID),
				Disposition:       disp,
				Fingerprint:       fp,
				Standings:         slot.Standings,
				QuarantineReason:  slot.QuarantineReason,
				SourceEventID:     src,
				Advancing:         slot.Advancing,
			}
		}
	}

	// Fill any resultKeys not yet covered (duplicate_ignored / quarantined without HasResult).
	for key, rr := range resultKeys {
		if existing, ok := byKey[key]; ok {
			if existing.SourceEventID == "" && rr.SourceEventID != "" {
				existing.SourceEventID = rr.SourceEventID
				byKey[key] = existing
			}
			continue
		}
		parts := strings.SplitN(key, ":", 2)
		if len(parts) != 2 {
			continue
		}
		ver, err := strconv.ParseUint(parts[1], 10, 64)
		if err != nil {
			continue
		}
		roomID := domain.RoomID(parts[0])
		rn, slotID, ok := t.AssignmentByRoomID(roomID)
		if !ok {
			continue
		}
		byKey[key] = resultRow{
			RoomID:            parts[0],
			CompletionVersion: ver,
			RoundNumber:       rn,
			SlotID:            string(slotID),
			Disposition:       rr.Disposition,
			Fingerprint:       rr.Fingerprint,
			SourceEventID:     rr.SourceEventID,
		}
	}

	if matchSource != nil && matchSource.EventID != "" && matchSource.RoomID != "" && matchSource.CompletionVersion > 0 {
		key := matchSource.RoomID + ":" + strconv.FormatUint(matchSource.CompletionVersion, 10)
		if row, ok := byKey[key]; ok {
			row.SourceEventID = matchSource.EventID
			byKey[key] = row
		}
	}

	for _, row := range byKey {
		if row.CompletionVersion == 0 {
			continue
		}
		ranked, err := json.Marshal(standingsPayload(row.Standings, row.Fingerprint))
		if err != nil {
			return err
		}
		var qReason any
		if row.QuarantineReason != "" {
			qReason = row.QuarantineReason
		} else if row.Disposition == domain.DispositionQuarantined {
			qReason = "quarantined"
		}
		var sourceEvent any
		if row.SourceEventID != "" {
			sourceEvent = row.SourceEventID
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO match_results (
				room_id, completion_version, tournament_id, round_number, slot_id,
				disposition, ranked_result, quarantine_reason, source_event_id, processed_at
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		`, row.RoomID, int64(row.CompletionVersion), tid, row.RoundNumber, row.SlotID,
			string(row.Disposition), ranked, qReason, sourceEvent, now); err != nil {
			return err
		}
		if row.Disposition == domain.DispositionRecorded && len(row.Advancing) > 0 {
			advID := fmt.Sprintf("adv:%s:r%d:%s", tid, row.RoundNumber, row.SlotID)
			tieBreak, _ := json.Marshal(map[string]string{"fingerprint": row.Fingerprint})
			adv := make([]string, len(row.Advancing))
			for i, p := range row.Advancing {
				adv[i] = string(p)
			}
			if _, err := tx.Exec(ctx, `
				INSERT INTO advancement_records (
					tournament_id, round_number, slot_id, advancement_id, advancing_player_ids,
					tie_break_inputs, source_room_id, source_completion_version, recorded_at
				) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
			`, tid, row.RoundNumber, row.SlotID, advID, adv,
				tieBreak, row.RoomID, int64(row.CompletionVersion), now); err != nil {
				return err
			}
		}
	}

	// Deterministic rebuild of normalized advancing players from ranked array ordinals.
	if _, err := tx.Exec(ctx, `
		INSERT INTO round_advancing_players (
			tournament_id, source_round_number, player_id, source_slot_id, advancement_rank
		)
		SELECT ar.tournament_id, ar.round_number, p.player_id, ar.slot_id, (p.ord - 1)::int
		FROM advancement_records ar
		CROSS JOIN LATERAL unnest(ar.advancing_player_ids) WITH ORDINALITY AS p(player_id, ord)
		WHERE ar.tournament_id = $1
	`, tid); err != nil {
		return err
	}

	if err := rebuildRoundProgressShardsTx(ctx, tx, tid); err != nil {
		return err
	}
	return nil
}

func standingsPayload(standings []domain.PlayerMatchStanding, fp string) map[string]any {
	rows := make([]map[string]any, 0, len(standings))
	for _, s := range standings {
		rows = append(rows, map[string]any{
			"playerId":             string(s.PlayerID),
			"matchWins":            s.MatchWins,
			"cumulativeCardPoints": s.CumulativeCardPoints,
			"finalGameCompletedAt": s.FinalGameCompletedAt.UTC().Format(time.RFC3339Nano),
			"forfeited":            s.Forfeited,
		})
	}
	return map[string]any{"standings": rows, "fingerprint": fp}
}

type provisioningLeaseSnapshot struct {
	Owner   string
	Expires time.Time
	Version int64
}

func leaseKey(roundNumber int, batchID string) string {
	return strconv.Itoa(roundNumber) + "\x00" + batchID
}

func loadActiveProvisioningLeases(ctx context.Context, tx pgx.Tx, tid string) (map[string]provisioningLeaseSnapshot, error) {
	rows, err := tx.Query(ctx, `
		SELECT round_number, batch_id, lease_owner, lease_expires_at, lease_version
		FROM provisioning_batches
		WHERE tournament_id = $1
		  AND status = 'in_progress'
		  AND lease_owner IS NOT NULL
		  AND lease_expires_at IS NOT NULL
	`, tid)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[string]provisioningLeaseSnapshot)
	for rows.Next() {
		var (
			rn      int
			batchID string
			owner   string
			expires time.Time
			version int64
		)
		if err := rows.Scan(&rn, &batchID, &owner, &expires, &version); err != nil {
			return nil, err
		}
		out[leaseKey(rn, batchID)] = provisioningLeaseSnapshot{
			Owner: owner, Expires: expires.UTC(), Version: version,
		}
	}
	return out, rows.Err()
}

// provisioningLeaseForRewrite keeps another active in_progress lease across full aggregate
// rewrite and clears leases for completed/retried/quarantined/cancelled/pending rows.
// Returns owner, expires_at, lease_version (version preserved even when lease cleared → 0 default).
func provisioningLeaseForRewrite(batch domain.ProvisioningBatch, prev provisioningLeaseSnapshot) (any, any, any) {
	if batch.Status != domain.BatchInProgress {
		return nil, nil, int64(0)
	}
	if prev.Owner == "" || prev.Expires.IsZero() {
		return nil, nil, int64(0)
	}
	return prev.Owner, prev.Expires, prev.Version
}
