package store

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"unoarena/services/room-gameplay/app"
	"unoarena/services/room-gameplay/domain"
	"unoarena/services/room-gameplay/game"
)

func persistSessionTx(ctx context.Context, tx pgx.Tx, sess *domain.Session, req app.CommitRequest, createPath bool) error {
	if sess == nil || sess.Room() == nil {
		return fmt.Errorf("session required")
	}
	room := sess.Room()
	roomID := string(room.ID())
	now := time.Now().UTC()

	outcomesJSON, err := domainOutcomesToJSON(room.OutcomesMap())
	if err != nil {
		return err
	}
	discJSON, err := encodeDisconnects(room.DisconnectsMap())
	if err != nil {
		return err
	}
	nextJSON, err := encodeNextDisconnect(room.NextDisconnectVersions())
	if err != nil {
		return err
	}
	uno, hasUno := room.UnoWindow()
	unoJSON, err := encodeDomainUnoExact(uno, hasUno)
	if err != nil {
		return err
	}
	usedJSON, err := encodeUsedGameIDs(sess.UsedGameIDs())
	if err != nil {
		return err
	}
	skippedJSON, err := encodeSkipped(sess.SkippedTurns())
	if err != nil {
		return err
	}
	matchJSON, err := encodeMatch(sess.Match())
	if err != nil {
		return err
	}
	matchScore := []byte("{}")
	if sess.Match() != nil {
		ms, err := json.Marshal(intMapToJSON(sess.Match().Score().Wins()))
		if err != nil {
			return err
		}
		matchScore = ms
	}

	var host any
	if room.HostID() != "" {
		host = string(room.HostID())
	}
	var tid, slot any
	var round any
	if room.Type() == domain.RoomTypeTournament {
		tid = string(room.TournamentID())
		round = room.RoundNumber()
		slot = room.SlotID()
	}
	var completedAt any
	if room.Status().IsTerminal() {
		completedAt = now
	}

	integrity := int64(0)
	if req.SetIntegrityRevision {
		integrity = req.IntegrityRevision
	}

	if createPath {
		_, err = tx.Exec(ctx, `
			INSERT INTO rooms (
				room_id, room_type, status, visibility, capacity, sequence_number, host_player_id,
				match_number, match_score, tournament_id, round_number, slot_id, integrity_log_offset,
				turn_version, game_completed_in_match, used_game_ids, skipped_turns, has_uno, uno_window,
				disconnects, next_disconnect_versions, match_snapshot, outcomes, created_at, updated_at, completed_at
			) VALUES (
				$1,$2,$3,$4,$5,$6,$7,
				$8,$9,$10,$11,$12,$13,
				$14,$15,$16,$17,$18,$19,
				$20,$21,$22,$23,$24,$25,$26
			)
		`, roomID, string(room.Type()), string(room.Status()), string(room.Visibility()), room.Roster().Capacity(),
			int64(room.Sequence()), host, 1, matchScore, tid, round, slot, integrity,
			int64(sess.TurnVersion()), room.GameCompletedInMatch(), usedJSON, skippedJSON, hasUno, unoJSON,
			discJSON, nextJSON, matchJSON, outcomesJSON, now, now, completedAt)
		if err != nil {
			return err
		}
	} else {
		tag, err := tx.Exec(ctx, `
			UPDATE rooms SET
				status = $2, visibility = $3, capacity = $4, sequence_number = $5, host_player_id = $6,
				match_score = $7, integrity_log_offset = CASE WHEN $8::boolean THEN $9 ELSE integrity_log_offset END,
				turn_version = $10, game_completed_in_match = $11, used_game_ids = $12, skipped_turns = $13,
				has_uno = $14, uno_window = $15, disconnects = $16, next_disconnect_versions = $17,
				match_snapshot = $18, outcomes = $19, updated_at = $20, completed_at = COALESCE($21, completed_at)
			WHERE room_id = $1
		`, roomID, string(room.Status()), string(room.Visibility()), room.Roster().Capacity(),
			int64(room.Sequence()), host, matchScore, req.SetIntegrityRevision, integrity,
			int64(sess.TurnVersion()), room.GameCompletedInMatch(), usedJSON, skippedJSON,
			hasUno, unoJSON, discJSON, nextJSON, matchJSON, outcomesJSON, now, completedAt)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			// First write after create-race resolution: insert.
			return persistSessionTx(ctx, tx, sess, req, true)
		}
	}

	if _, err := tx.Exec(ctx, `DELETE FROM room_roster WHERE room_id = $1`, roomID); err != nil {
		return err
	}
	wins := map[string]int{}
	points := map[string]int{}
	if sess.Match() != nil {
		for k, v := range sess.Match().Score().Wins() {
			wins[string(k)] = v
		}
		for k, v := range sess.Match().CardPoints() {
			points[string(k)] = v
		}
	}
	for _, seat := range room.Roster().Seats() {
		if !seat.Occupied {
			continue
		}
		conn := "connected"
		var discVer int64
		if ds, ok := room.DisconnectState(seat.PlayerID); ok && ds.Active {
			conn = "disconnected"
			discVer = int64(ds.DisconnectVersion)
		}
		_, err := tx.Exec(ctx, `
			INSERT INTO room_roster (room_id, seat_number, player_id, occupied, connection_status, disconnect_version, wins, points)
			VALUES ($1,$2,$3,true,$4,$5,$6,$7)
		`, roomID, int(seat.Index), string(seat.PlayerID), conn, discVer, wins[string(seat.PlayerID)], points[string(seat.PlayerID)])
		if err != nil {
			return err
		}
	}

	if _, err := tx.Exec(ctx, `DELETE FROM current_games WHERE room_id = $1`, roomID); err != nil {
		return err
	}
	if g := sess.Game(); g != nil {
		engine, err := encodeGame(g)
		if err != nil {
			return err
		}
		status := "active"
		if g.Completed() {
			status = "completed"
		}
		if g.Abandoned() {
			status = "abandoned"
		}
		hands, _ := json.Marshal(handsToJSON(g.HandsMap()))
		turnOrder, _ := json.Marshal(playerIDsToStrings(g.Seats()))
		top, _ := json.Marshal(g.DiscardTop())
		placement, _ := json.Marshal(playerIDsToStrings(g.PlacementOrder()))
		counts := map[string]int{}
		for pid, h := range g.HandsMap() {
			counts[string(pid)] = len(h)
		}
		cardCounts, _ := json.Marshal(counts)
		cur := g.CurrentSeatIndex()
		_, err = tx.Exec(ctx, `
			INSERT INTO current_games (
				room_id, game_id, game_number, status, snapshot_sequence, turn_order, current_seat,
				active_color, direction, penalty_stack, top_discard, hands, card_counts, placement_order, engine_state
			) VALUES ($1,$2,1,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
		`, roomID, string(g.ID()), status, int64(room.Sequence()), turnOrder, cur,
			string(g.ActiveColor()), int(g.Direction()), g.PenaltyAmount(), top, hands, cardCounts, placement, engine)
		if err != nil {
			return err
		}
	}

	// Deadlines from session disconnects + uno window.
	if err := syncDeadlines(ctx, tx, sess); err != nil {
		return err
	}
	if err := syncNextGameContinuation(ctx, tx, sess, now); err != nil {
		return err
	}

	if req.BindPlayerSession && req.PlayerID != "" && req.PlayerSessionID != "" {
		_, err := tx.Exec(ctx, `
			INSERT INTO player_session_bindings (room_id, player_id, session_id, updated_at)
			VALUES ($1,$2,$3,$4)
			ON CONFLICT (room_id, player_id) DO UPDATE SET session_id = EXCLUDED.session_id, updated_at = EXCLUDED.updated_at
		`, roomID, req.PlayerID, req.PlayerSessionID, now)
		if err != nil {
			return err
		}
	}
	if req.ProvisionKey != nil && req.ProvisionRoomID != "" {
		_, err := tx.Exec(ctx, `
			INSERT INTO tournament_provisions (tournament_id, round_number, slot_id, room_id)
			VALUES ($1,$2,$3,$4)
			ON CONFLICT DO NOTHING
		`, req.ProvisionKey.TournamentID, req.ProvisionKey.RoundNumber, req.ProvisionKey.SlotID, req.ProvisionRoomID)
		if err != nil {
			return err
		}
	}
	if req.SetStreamSeq {
		_, err := tx.Exec(ctx, `
			INSERT INTO player_stream_highwater (room_id, sequence_number, updated_at)
			VALUES ($1,$2,$3)
			ON CONFLICT (room_id) DO UPDATE SET
				sequence_number = GREATEST(player_stream_highwater.sequence_number, EXCLUDED.sequence_number),
				updated_at = EXCLUDED.updated_at
		`, roomID, req.StreamSeqHighWater, now)
		if err != nil {
			return err
		}
	}
	if len(req.Outbox.Events) > 0 {
		if err := insertDualOutboxes(ctx, tx, req.Outbox); err != nil {
			return err
		}
	}
	return nil
}

func syncNextGameContinuation(ctx context.Context, tx pgx.Tx, sess *domain.Session, now time.Time) error {
	room := sess.Room()
	roomID := string(room.ID())
	game := sess.Game()
	awaitingNext := room.GameCompletedInMatch() && !room.Status().IsTerminal() &&
		sess.Match() != nil && !sess.Match().Completed() && game != nil && game.Completed()
	if !awaitingNext {
		_, err := tx.Exec(ctx, `DELETE FROM next_game_continuations WHERE room_id = $1`, roomID)
		return err
	}

	completedGameID := string(sess.GameID())
	commandID := "auto-next-" + roomID + "-after-" + completedGameID
	nextGameID := "game-next-" + completedGameID
	_, err := tx.Exec(ctx, `
		INSERT INTO next_game_continuations (
			room_id, completed_game_id, command_id, next_game_id, available_at, lease_until, attempts, updated_at
		) VALUES ($1,$2,$3,$4,$5,NULL,0,$5)
		ON CONFLICT (room_id) DO UPDATE SET
			completed_game_id = EXCLUDED.completed_game_id,
			command_id = EXCLUDED.command_id,
			next_game_id = EXCLUDED.next_game_id,
			available_at = EXCLUDED.available_at,
			lease_until = NULL,
			attempts = 0,
			updated_at = EXCLUDED.updated_at
		WHERE next_game_continuations.completed_game_id IS DISTINCT FROM EXCLUDED.completed_game_id
	`, roomID, completedGameID, commandID, nextGameID, now.UTC())
	return err
}

func syncDeadlines(ctx context.Context, tx pgx.Tx, sess *domain.Session) error {
	room := sess.Room()
	roomID := string(room.ID())
	// Reconnect deadlines: upsert open ones from active disconnects; close missing.
	for pid, ds := range room.DisconnectsMap() {
		if !ds.Active {
			continue
		}
		_, err := tx.Exec(ctx, `
			INSERT INTO reconnect_deadlines (room_id, player_id, disconnect_version, expires_at, status)
			VALUES ($1,$2,$3,$4,'open')
			ON CONFLICT (room_id, player_id, disconnect_version) DO UPDATE SET
				expires_at = EXCLUDED.expires_at,
				status = 'open',
				resolved_at = NULL
		`, roomID, string(pid), int64(ds.DisconnectVersion), ds.DeadlineUTC.UTC())
		if err != nil {
			return err
		}
	}
	if uw, ok := room.UnoWindow(); ok && uw.IsOpen() {
		_, err := tx.Exec(ctx, `
			INSERT INTO uno_deadlines (
				room_id, game_id, player_id, triggering_game_event_id, expires_at, opening_room_sequence, status
			) VALUES ($1,$2,$3,$4,$5,$6,'open')
			ON CONFLICT (room_id, game_id, player_id, triggering_game_event_id) DO UPDATE SET
				expires_at = EXCLUDED.expires_at,
				opening_room_sequence = EXCLUDED.opening_room_sequence,
				status = 'open',
				resolved_at = NULL
		`, roomID, string(uw.GameID), string(uw.PlayerID), string(uw.TriggeringGameEventID),
			uw.ExpiresAt.UTC(), int64(uw.OpeningSequence))
		if err != nil {
			return err
		}
	}
	if g := sess.Game(); g != nil {
		if gu := g.UnoWindow(); gu != nil && gu.IsOpen() {
			_, err := tx.Exec(ctx, `
				INSERT INTO uno_deadlines (
					room_id, game_id, player_id, triggering_game_event_id, expires_at, opening_room_sequence, status
				) VALUES ($1,$2,$3,$4,$5,$6,'open')
				ON CONFLICT (room_id, game_id, player_id, triggering_game_event_id) DO UPDATE SET
					expires_at = EXCLUDED.expires_at,
					opening_room_sequence = EXCLUDED.opening_room_sequence,
					status = 'open',
					resolved_at = NULL
			`, roomID, string(g.ID()), string(gu.PlayerID), "engine-uno",
				gu.ExpiresAt.UTC(), int64(gu.OpeningSequence))
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func insertDualOutboxes(ctx context.Context, tx pgx.Tx, entry app.OutboxEntry) error {
	for _, ev := range entry.Events {
		kind, err := app.ClassifyOutboxEvent(ev)
		if err != nil {
			return err
		}
		if kind == app.OutboxSkip {
			continue
		}
		payload := ev.Payload
		if len(payload) == 0 {
			payload = []byte("{}")
		}
		var occurred any
		if ev.OccurredAt != "" {
			if t, err := time.Parse(time.RFC3339Nano, ev.OccurredAt); err == nil {
				occurred = t.UTC()
			} else if t, err := time.Parse(time.RFC3339, ev.OccurredAt); err == nil {
				occurred = t.UTC()
			}
		}
		switch kind {
		case app.OutboxRealtime:
			targetStream, err := app.PlayerFeedTargetStream(entry.RoomID)
			if err != nil {
				return err
			}
			_, err = tx.Exec(ctx, `
				INSERT INTO realtime_outbox_events (
					event_id, event_type, topic, target_stream, partition_key, schema_version,
					room_id, player_id, session_id, sequence_number, integrity_log_offset,
					payload, correlation_id, causation_id, occurred_at
				) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
				ON CONFLICT (event_id) DO NOTHING
			`, ev.EventID, ev.EventType, ev.Topic, targetStream, entry.RoomID, ev.SchemaVersion,
				entry.RoomID, nullIfEmpty(ev.PlayerID), nullIfEmpty(ev.SessionID), ev.SequenceNumber, entry.LogOffset,
				payload, nullIfEmpty(ev.CorrelationID), nullIfEmpty(ev.CausationID), occurred)
			if err != nil {
				return err
			}
		case app.OutboxIntegration:
			_, err := tx.Exec(ctx, `
				INSERT INTO integration_outbox_events (
					event_id, event_type, topic, partition_key, schema_version, room_id,
					integrity_log_offset, payload, correlation_id, causation_id, occurred_at
				) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
				ON CONFLICT (event_id) DO NOTHING
			`, ev.EventID, ev.EventType, ev.Topic, entry.RoomID, ev.SchemaVersion, entry.RoomID,
				entry.LogOffset, payload, nullIfEmpty(ev.CorrelationID), nullIfEmpty(ev.CausationID), occurred)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

type disconnectRow struct {
	PlayerID          string    `json:"playerId"`
	DisconnectVersion uint64    `json:"disconnectVersion"`
	DeadlineUTC       time.Time `json:"deadlineUtc"`
	Active            bool      `json:"active"`
}

func encodeDisconnects(in map[domain.PlayerID]domain.DisconnectState) ([]byte, error) {
	out := map[string]disconnectRow{}
	for k, v := range in {
		out[string(k)] = disconnectRow{
			PlayerID:          string(v.PlayerID),
			DisconnectVersion: uint64(v.DisconnectVersion),
			DeadlineUTC:       v.DeadlineUTC.UTC(),
			Active:            v.Active,
		}
	}
	return json.Marshal(out)
}

func decodeDisconnects(b []byte) (map[domain.PlayerID]domain.DisconnectState, error) {
	if len(b) == 0 || string(b) == "{}" || string(b) == "null" {
		return map[domain.PlayerID]domain.DisconnectState{}, nil
	}
	var raw map[string]disconnectRow
	if err := json.Unmarshal(b, &raw); err != nil {
		return nil, err
	}
	out := make(map[domain.PlayerID]domain.DisconnectState, len(raw))
	for k, v := range raw {
		out[domain.PlayerID(k)] = domain.DisconnectState{
			PlayerID:          domain.PlayerID(v.PlayerID),
			DisconnectVersion: domain.DisconnectVersion(v.DisconnectVersion),
			DeadlineUTC:       v.DeadlineUTC,
			Active:            v.Active,
		}
	}
	return out, nil
}

func encodeNextDisconnect(in map[domain.PlayerID]domain.DisconnectVersion) ([]byte, error) {
	out := map[string]uint64{}
	for k, v := range in {
		out[string(k)] = uint64(v)
	}
	return json.Marshal(out)
}

func decodeNextDisconnect(b []byte) (map[domain.PlayerID]domain.DisconnectVersion, error) {
	if len(b) == 0 || string(b) == "{}" || string(b) == "null" {
		return map[domain.PlayerID]domain.DisconnectVersion{}, nil
	}
	var raw map[string]uint64
	if err := json.Unmarshal(b, &raw); err != nil {
		return nil, err
	}
	out := make(map[domain.PlayerID]domain.DisconnectVersion, len(raw))
	for k, v := range raw {
		out[domain.PlayerID(k)] = domain.DisconnectVersion(v)
	}
	return out, nil
}

func encodeUsedGameIDs(in map[domain.GameID]struct{}) ([]byte, error) {
	out := make([]string, 0, len(in))
	for k := range in {
		out = append(out, string(k))
	}
	return json.Marshal(out)
}

func decodeUsedGameIDs(b []byte) (map[domain.GameID]struct{}, error) {
	if len(b) == 0 || string(b) == "[]" || string(b) == "null" {
		return map[domain.GameID]struct{}{}, nil
	}
	var raw []string
	if err := json.Unmarshal(b, &raw); err != nil {
		return nil, err
	}
	out := make(map[domain.GameID]struct{}, len(raw))
	for _, id := range raw {
		out[domain.GameID(id)] = struct{}{}
	}
	return out, nil
}

func encodeSkipped(in []domain.SkippedTurn) ([]byte, error) {
	type row struct {
		PlayerID    string `json:"playerId"`
		TurnVersion uint64 `json:"turnVersion"`
	}
	out := make([]row, 0, len(in))
	for _, s := range in {
		out = append(out, row{PlayerID: string(s.PlayerID), TurnVersion: uint64(s.TurnVersion)})
	}
	return json.Marshal(out)
}

func decodeSkipped(b []byte) ([]domain.SkippedTurn, error) {
	if len(b) == 0 || string(b) == "[]" || string(b) == "null" {
		return nil, nil
	}
	type row struct {
		PlayerID    string `json:"playerId"`
		TurnVersion uint64 `json:"turnVersion"`
	}
	var raw []row
	if err := json.Unmarshal(b, &raw); err != nil {
		return nil, err
	}
	out := make([]domain.SkippedTurn, 0, len(raw))
	for _, r := range raw {
		out = append(out, domain.SkippedTurn{
			PlayerID:    domain.PlayerID(r.PlayerID),
			TurnVersion: domain.SequenceNumber(r.TurnVersion),
		})
	}
	return out, nil
}

// Ensure game import used when building card helpers.
var _ = game.Card{}
