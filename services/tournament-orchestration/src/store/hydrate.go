package store

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"

	"unoarena/services/tournament-orchestration/domain"
)

type dbQuerier interface {
	QueryRow(context.Context, string, ...any) pgx.Row
	Query(context.Context, string, ...any) (pgx.Rows, error)
}

func (s *TournamentStore) loadTournament(ctx context.Context, tid string) (*domain.Tournament, error) {
	if s == nil || s.pool == nil {
		return nil, fmtNilPool()
	}
	return s.loadTournamentQ(ctx, s.pool, tid)
}

func fmtNilPool() error { return errors.New("nil pool") }

func (s *TournamentStore) loadTournamentQ(ctx context.Context, q dbQuerier, tid string) (*domain.Tournament, error) {
	var (
		phase    string
		capacity int
		rulesRaw []byte
	)
	err := q.QueryRow(ctx, `
		SELECT phase, capacity, rules FROM tournaments WHERE tournament_id = $1
	`, tid).Scan(&phase, &capacity, &rulesRaw)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var rules tournamentRules
	_ = json.Unmarshal(rulesRaw, &rules)

	regs := map[domain.PlayerID]struct{}{}
	order := make([]domain.PlayerID, 0)
	rows, err := q.Query(ctx, `
		SELECT player_id FROM tournament_registrations
		WHERE tournament_id = $1 AND status = 'registered'
		ORDER BY registered_at ASC, player_id ASC
	`, tid)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var pid string
		if err := rows.Scan(&pid); err != nil {
			rows.Close()
			return nil, err
		}
		id := domain.PlayerID(pid)
		regs[id] = struct{}{}
		order = append(order, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	rounds := map[int]*domain.Round{}
	rrows, err := q.Query(ctx, `
		SELECT round_number, status, is_final, completed_at IS NOT NULL
		FROM tournament_rounds WHERE tournament_id = $1 ORDER BY round_number
	`, tid)
	if err != nil {
		return nil, err
	}
	for rrows.Next() {
		var (
			n         int
			status    string
			isFinal   bool
			completed bool
		)
		if err := rrows.Scan(&n, &status, &isFinal, &completed); err != nil {
			rrows.Close()
			return nil, err
		}
		rounds[n] = &domain.Round{
			Number:    n,
			Status:    domain.RoundStatus(status),
			IsFinal:   isFinal,
			Completed: completed,
		}
	}
	rrows.Close()
	if err := rrows.Err(); err != nil {
		return nil, err
	}

	for n, round := range rounds {
		slots, err := loadSlotsQ(ctx, q, tid, n)
		if err != nil {
			return nil, err
		}
		round.Slots = slots
		batches, err := loadBatchesQ(ctx, q, tid, n, slots)
		if err != nil {
			return nil, err
		}
		round.Batches = batches
	}

	resultKeys := map[string]domain.ResultRecord{}
	processed := map[domain.EventID]domain.CommandOutcome{}
	roomOwners := map[domain.RoomID]string{}

	mrows, err := q.Query(ctx, `
		SELECT room_id, completion_version, round_number, slot_id, disposition,
			ranked_result, quarantine_reason, source_event_id
		FROM match_results WHERE tournament_id = $1
	`, tid)
	if err != nil {
		return nil, err
	}
	for mrows.Next() {
		var (
			roomID, slotID, disposition string
			roundNumber                 int
			completionVersion           int64
			rankedRaw                   []byte
			qReason, sourceEvent        *string
		)
		if err := mrows.Scan(&roomID, &completionVersion, &roundNumber, &slotID, &disposition, &rankedRaw, &qReason, &sourceEvent); err != nil {
			mrows.Close()
			return nil, err
		}
		fp, standings := parseRankedResult(rankedRaw)
		key := roomID + ":" + strconv.FormatUint(uint64(completionVersion), 10)
		src := ""
		if sourceEvent != nil {
			src = *sourceEvent
		}
		resultKeys[key] = domain.ResultRecord{
			Disposition:   domain.ResultDisposition(disposition),
			Fingerprint:   fp,
			SourceEventID: src,
		}
		roomOwners[domain.RoomID(roomID)] = strconv.Itoa(roundNumber) + ":" + slotID
		if round, ok := rounds[roundNumber]; ok {
			for i := range round.Slots {
				if string(round.Slots[i].SlotID) != slotID {
					continue
				}
				round.Slots[i].RoomID = domain.RoomID(roomID)
				round.Slots[i].CompletionVersion = domain.CompletionVersion(completionVersion)
				round.Slots[i].ResultFingerprint = fp
				round.Slots[i].Standings = standings
				if disposition == string(domain.DispositionRecorded) || disposition == string(domain.DispositionDuplicateIgnored) {
					if disposition == string(domain.DispositionRecorded) {
						round.Slots[i].HasResult = true
					}
				}
				if disposition == string(domain.DispositionQuarantined) {
					if round.Slots[i].Status != domain.SlotAdvanced && round.Slots[i].Status != domain.SlotResultRecorded {
						round.Slots[i].Status = domain.SlotQuarantined
					}
					if qReason != nil {
						round.Slots[i].QuarantineReason = *qReason
					}
				}
			}
		}
		if src != "" {
			facts := []domain.Fact{}
			data := map[string]string{
				"tournamentId":      tid,
				"roundNumber":       strconv.Itoa(roundNumber),
				"slotId":            slotID,
				"roomId":            roomID,
				"completionVersion": strconv.FormatUint(uint64(completionVersion), 10),
			}
			switch disposition {
			case string(domain.DispositionRecorded):
				facts = append(facts, domain.Fact{Name: domain.FactTournamentMatchResultRecorded, Data: data})
			case string(domain.DispositionQuarantined):
				if qReason != nil {
					data["reason"] = *qReason
				}
				facts = append(facts, domain.Fact{Name: domain.FactTournamentResultQuarantined, Data: data})
			}
			processed[domain.EventID(src)] = domain.CommandOutcome{
				Kind:      domain.OutcomeAccepted,
				CommandID: domain.CommandID("ingest:" + src),
				Facts:     facts,
			}
		}
	}
	mrows.Close()
	if err := mrows.Err(); err != nil {
		return nil, err
	}

	arows, err := q.Query(ctx, `
		SELECT round_number, slot_id, advancing_player_ids, source_room_id, source_completion_version
		FROM advancement_records WHERE tournament_id = $1
	`, tid)
	if err != nil {
		return nil, err
	}
	for arows.Next() {
		var (
			slotID, sourceRoom string
			rn                 int
			completionVersion  int64
			adv                []string
		)
		if err := arows.Scan(&rn, &slotID, &adv, &sourceRoom, &completionVersion); err != nil {
			arows.Close()
			return nil, err
		}
		if round, ok := rounds[rn]; ok {
			for i := range round.Slots {
				if string(round.Slots[i].SlotID) != slotID {
					continue
				}
				round.Slots[i].Advancing = textToPlayerIDs(adv)
				round.Slots[i].HasResult = true
				round.Slots[i].RoomID = domain.RoomID(sourceRoom)
				round.Slots[i].CompletionVersion = domain.CompletionVersion(completionVersion)
				if round.Slots[i].Status != domain.SlotQuarantined {
					if round.IsFinal {
						round.Slots[i].Status = domain.SlotResultRecorded
					} else {
						round.Slots[i].Status = domain.SlotAdvanced
					}
				}
			}
		}
		roomOwners[domain.RoomID(sourceRoom)] = strconv.Itoa(rn) + ":" + slotID
	}
	arows.Close()
	if err := arows.Err(); err != nil {
		return nil, err
	}

	amrows, err := q.Query(ctx, `
		SELECT round_number, slot_id, room_id, provisioning_batch_id
		FROM assigned_matches WHERE tournament_id = $1
	`, tid)
	if err != nil {
		return nil, err
	}
	for amrows.Next() {
		var (
			rn             int
			slotID, roomID string
			batchID        *string
		)
		if err := amrows.Scan(&rn, &slotID, &roomID, &batchID); err != nil {
			amrows.Close()
			return nil, err
		}
		roomOwners[domain.RoomID(roomID)] = strconv.Itoa(rn) + ":" + slotID
		if round, ok := rounds[rn]; ok {
			for i := range round.Slots {
				if string(round.Slots[i].SlotID) != slotID {
					continue
				}
				round.Slots[i].RoomID = domain.RoomID(roomID)
				if batchID != nil {
					round.Slots[i].BatchID = domain.BatchID(*batchID)
				}
				if round.Slots[i].Status == domain.SlotPending {
					round.Slots[i].Status = domain.SlotAssigned
				}
			}
		}
	}
	amrows.Close()
	if err := amrows.Err(); err != nil {
		return nil, err
	}

	return domain.RestoreTournament(domain.RestoreTournamentInput{
		ID:                domain.TournamentID(tid),
		Phase:             domain.TournamentPhase(phase),
		Capacity:          capacity,
		RetryBudget:       rules.RetryBudget,
		BatchSize:         rules.BatchSize,
		Registrations:     regs,
		RegistrationOrder: order,
		Rounds:            rounds,
		CurrentRound:      rules.CurrentRound,
		Champion:          domain.PlayerID(rules.ChampionID),
		ProcessedEvents:   processed,
		ResultKeys:        resultKeys,
		RoomOwners:        roomOwners,
	}), nil
}

func loadSlotsQ(ctx context.Context, q dbQuerier, tid string, roundNumber int) ([]domain.BracketSlot, error) {
	rows, err := q.Query(ctx, `
		SELECT slot_id, status, seeded_player_ids
		FROM bracket_slots
		WHERE tournament_id = $1 AND round_number = $2
		ORDER BY slot_id
	`, tid, roundNumber)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]domain.BracketSlot, 0)
	idx := 0
	for rows.Next() {
		var (
			slotID, status string
			seeded         []string
		)
		if err := rows.Scan(&slotID, &status, &seeded); err != nil {
			return nil, err
		}
		out = append(out, domain.BracketSlot{
			SlotID:        domain.SlotID(slotID),
			Index:         idx,
			Status:        domain.SlotStatus(status),
			SeededPlayers: textToPlayerIDs(seeded),
		})
		idx++
	}
	return out, rows.Err()
}

func loadBatchesQ(ctx context.Context, q dbQuerier, tid string, roundNumber int, slots []domain.BracketSlot) ([]domain.ProvisioningBatch, error) {
	rows, err := q.Query(ctx, `
		SELECT batch_id, status, retry_attempt, slot_id_from, slot_id_to, last_error, quarantine_reason
		FROM provisioning_batches
		WHERE tournament_id = $1 AND round_number = $2
		ORDER BY batch_id
	`, tid, roundNumber)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]domain.ProvisioningBatch, 0)
	for rows.Next() {
		var (
			batchID, status, from, to string
			retry                     int
			lastErr, qReason          *string
		)
		if err := rows.Scan(&batchID, &status, &retry, &from, &to, &lastErr, &qReason); err != nil {
			return nil, err
		}
		indexes := make([]int, 0)
		inRange := false
		for i, s := range slots {
			if string(s.SlotID) == from {
				inRange = true
			}
			if inRange {
				indexes = append(indexes, i)
			}
			if string(s.SlotID) == to {
				break
			}
		}
		b := domain.ProvisioningBatch{
			BatchID:      domain.BatchID(batchID),
			SlotFrom:     domain.SlotID(from),
			SlotTo:       domain.SlotID(to),
			SlotIndexes:  indexes,
			Status:       domain.BatchStatus(status),
			RetryAttempt: retry,
		}
		if lastErr != nil {
			b.LastError = *lastErr
		}
		if qReason != nil {
			b.QuarantineReason = *qReason
		}
		out = append(out, b)
	}
	return out, rows.Err()
}

func parseRankedResult(raw []byte) (string, []domain.PlayerMatchStanding) {
	if len(raw) == 0 {
		return "", nil
	}
	var payload struct {
		Fingerprint string `json:"fingerprint"`
		Standings   []struct {
			PlayerID             string `json:"playerId"`
			MatchWins            int    `json:"matchWins"`
			CumulativeCardPoints int    `json:"cumulativeCardPoints"`
			FinalGameCompletedAt string `json:"finalGameCompletedAt"`
			Forfeited            bool   `json:"forfeited"`
		} `json:"standings"`
	}
	if err := json.Unmarshal(raw, &payload); err != nil {
		return "", nil
	}
	out := make([]domain.PlayerMatchStanding, 0, len(payload.Standings))
	for _, s := range payload.Standings {
		var at time.Time
		if s.FinalGameCompletedAt != "" {
			at, _ = time.Parse(time.RFC3339Nano, s.FinalGameCompletedAt)
			if at.IsZero() {
				at, _ = time.Parse(time.RFC3339, s.FinalGameCompletedAt)
			}
		}
		out = append(out, domain.PlayerMatchStanding{
			PlayerID:             domain.PlayerID(s.PlayerID),
			MatchWins:            s.MatchWins,
			CumulativeCardPoints: s.CumulativeCardPoints,
			FinalGameCompletedAt: at,
			Forfeited:            s.Forfeited,
		})
	}
	return payload.Fingerprint, out
}

func textToPlayerIDs(in []string) []domain.PlayerID {
	out := make([]domain.PlayerID, len(in))
	for i, s := range in {
		out[i] = domain.PlayerID(s)
	}
	return out
}
