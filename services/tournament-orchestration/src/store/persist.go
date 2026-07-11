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
			tournament_id, phase, capacity, registered_count, rules, created_at, updated_at, completed_at
		) VALUES ($1, $2, $3, $4, $5, $6, $6, $7)
		ON CONFLICT (tournament_id) DO UPDATE SET
			phase = EXCLUDED.phase,
			capacity = EXCLUDED.capacity,
			registered_count = EXCLUDED.registered_count,
			rules = EXCLUDED.rules,
			updated_at = EXCLUDED.updated_at,
			completed_at = COALESCE(EXCLUDED.completed_at, tournaments.completed_at)
	`, tid, string(t.Phase()), t.Capacity(), t.RegisteredCount(), rules, now, completedAt)
	if err != nil {
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
	if _, err := tx.Exec(ctx, `DELETE FROM bracket_slots WHERE tournament_id = $1`, tid); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `DELETE FROM provisioning_batches WHERE tournament_id = $1`, tid); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `DELETE FROM tournament_rounds WHERE tournament_id = $1`, tid); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `DELETE FROM tournament_registrations WHERE tournament_id = $1`, tid); err != nil {
		return err
	}

	for i, pid := range t.RegisteredPlayers() {
		regAt := now.Add(time.Duration(i) * time.Microsecond)
		if _, err := tx.Exec(ctx, `
			INSERT INTO tournament_registrations (tournament_id, player_id, registered_at, status)
			VALUES ($1, $2, $3, 'registered')
		`, tid, string(pid), regAt); err != nil {
			return err
		}
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
			if _, err := tx.Exec(ctx, `
				INSERT INTO provisioning_batches (
					tournament_id, round_number, batch_id, shard_key, status, retry_attempt,
					slot_id_from, slot_id_to, last_error, quarantine_reason, created_at, updated_at
				) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $11)
			`, tid, round.Number, string(batch.BatchID), string(batch.BatchID), string(batch.Status),
				batch.RetryAttempt, string(batch.SlotFrom), string(batch.SlotTo), lastErr, qReason, now); err != nil {
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
					tournament_id, round_number, slot_id, status, seeded_player_ids, created_at, updated_at
				) VALUES ($1, $2, $3, $4, $5, $6, $6)
			`, tid, round.Number, string(slot.SlotID), string(slot.Status), seeded, now); err != nil {
				return err
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
