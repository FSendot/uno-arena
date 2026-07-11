package app

import (
	"encoding/json"
	"strconv"
	"strings"
	"time"

	"unoarena/services/room-gameplay/domain"
	"unoarena/services/room-gameplay/game"
)

const (
	TopicPlayerFeed      = "room.player-feed.events"
	TopicSpectatorSafe   = "room.spectator-safe.events"
	TopicGameplayMetrics = "room.gameplay.metrics"
	TopicGameCompleted   = "room.game.completed"
	TopicMatchCompleted  = "room.match.completed"

	StreamPlayer    = "player"
	StreamSpectator = "spectator"

	EventSnapshotSanitized = "SnapshotSanitized"
	EventRoomCompleted     = "RoomCompleted"
	EventRoomCancelled     = "RoomCancelled"
)

// FeedAudience is one player stream target.
type FeedAudience struct {
	PlayerID  string
	SessionID string
}

// BuildFeedEvents builds independently sequenced player-private fact events plus
// exactly one canonical spectator projection event per state-changing accepted command.
// Accepted no-ops (no facts and no room-sequence advance vs prevRoomSequence) emit nothing.
// Spectator SequenceNumber is the authoritative room sequence (first=1, contiguous).
// playerStartSeq is the first player-feed sequence to assign; returns player high-water.
func BuildFeedEvents(
	sess *domain.Session,
	roomSequence int64,
	playerStartSeq int64,
	correlationID, commandID string,
	facts []domain.Fact,
	audiences []FeedAudience,
	prevRoomSequence int64,
) ([]PublishedEvent, int64) {
	out := make([]PublishedEvent, 0, len(facts)*len(audiences)+1)
	seq := playerStartSeq
	if seq < 1 {
		seq = 1
	}
	high := int64(0)
	roomID := ""
	if sess != nil && sess.Room() != nil {
		roomID = string(sess.Room().ID())
	}
	stateAdvanced := roomSequence > prevRoomSequence
	if len(facts) == 0 && !stateAdvanced {
		return nil, 0
	}
	for i, f := range facts {
		data := copyStringMapAny(f.Data)
		raw, _ := json.Marshal(map[string]any{
			"roomId":   roomID,
			"fact":     string(f.Name),
			"sequence": roomSequence,
			"data":     data,
		})
		for j, aud := range audiences {
			if aud.PlayerID == "" || aud.SessionID == "" {
				continue
			}
			out = append(out, PublishedEvent{
				Topic:          TopicPlayerFeed,
				Stream:         StreamPlayer,
				RoomID:         roomID,
				EventID:        commandID + "-pf-" + strconv.Itoa(i) + "-" + strconv.Itoa(j),
				EventType:      string(f.Name),
				SequenceNumber: seq,
				SchemaVersion:  1,
				CorrelationID:  correlationID,
				CausationID:    commandID,
				PlayerID:       aud.PlayerID,
				SessionID:      aud.SessionID,
				Payload:        raw,
			})
			high = seq
			seq++
		}
	}

	if sess != nil && sess.Room() != nil && roomSequence >= 1 {
		eventType := spectatorEventType(sess.Room())
		specData := BuildPublicSpectatorSnapshot(sess)
		specRaw, _ := json.Marshal(specData)
		out = append(out, PublishedEvent{
			Topic:          TopicSpectatorSafe,
			Stream:         StreamSpectator,
			RoomID:         roomID,
			EventID:        commandID + "-ss",
			EventType:      eventType,
			SequenceNumber: roomSequence,
			SchemaVersion:  1,
			CorrelationID:  correlationID,
			CausationID:    commandID,
			Payload:        specRaw,
		})
	}
	return out, high
}

// BuildPublicSpectatorSnapshot builds the full public projection payload from
// staged Session/Game. Never includes card identities, hands, or session fields.
func BuildPublicSpectatorSnapshot(sess *domain.Session) map[string]any {
	if sess == nil || sess.Room() == nil {
		return map[string]any{}
	}
	room := sess.Room()
	data := map[string]any{
		"status":     string(room.Status()),
		"visibility": string(room.Visibility()),
	}

	handCounts := map[string]int{}
	if g := sess.Game(); g != nil {
		for _, seat := range room.Roster().Seats() {
			if !seat.Occupied {
				continue
			}
			pid := game.PlayerID(seat.PlayerID)
			handCounts[string(seat.PlayerID)] = g.HandCount(pid)
		}
	}

	seats := make([]map[string]any, 0)
	for _, seat := range room.Roster().Seats() {
		if !seat.Occupied {
			continue
		}
		pid := string(seat.PlayerID)
		seats = append(seats, map[string]any{
			"seatIndex":   int(seat.Index),
			"playerId":    pid,
			"displayName": pid,
			"cardCount":   handCounts[pid],
			"occupied":    true,
		})
	}
	data["roster"] = seats
	data["seats"] = seats

	if g := sess.Game(); g != nil {
		pub := g.PublicSnapshot()
		data["discardTop"] = pub.DiscardTop.PublicFace()
		data["activeColor"] = string(pub.ActiveColor)
		data["direction"] = game.DirectionLabel(pub.Direction)
		if pub.CurrentPlayer != "" {
			data["currentPlayerId"] = string(pub.CurrentPlayer)
		}
		data["penaltyAmount"] = pub.PenaltyAmount
		if pub.PenaltyTarget != "" {
			data["penaltyTarget"] = string(pub.PenaltyTarget)
		}
		if pub.Completed {
			data["gameCompleted"] = true
			if order := g.PlacementOrder(); len(order) > 0 {
				data["winnerPlayerId"] = string(order[0])
			}
		} else {
			data["gameCompleted"] = false
		}
	}

	// Prefer room Uno window (authoritative opening room sequence + absolute expiry).
	if uw, ok := room.UnoWindow(); ok && uw.IsOpen() {
		data["unoWindow"] = map[string]any{
			"playerId":        string(uw.PlayerID),
			"expiresAt":       uw.ExpiresAt.UTC().Format(time.RFC3339Nano),
			"openingSequence": uint64(uw.OpeningSequence),
		}
		data["expiresAt"] = uw.ExpiresAt.UTC().Format(time.RFC3339Nano)
		data["openingSequence"] = uint64(uw.OpeningSequence)
	} else if g := sess.Game(); g != nil {
		if pub := g.PublicSnapshot(); pub.Uno != nil {
			data["unoWindow"] = map[string]any{
				"playerId":        string(pub.Uno.PlayerID),
				"expiresAt":       pub.Uno.ExpiresAt.UTC().Format(time.RFC3339Nano),
				"openingSequence": uint64(pub.Uno.OpeningSequence),
				"called":          pub.Uno.Called,
			}
			data["expiresAt"] = pub.Uno.ExpiresAt.UTC().Format(time.RFC3339Nano)
			data["openingSequence"] = uint64(pub.Uno.OpeningSequence)
		}
	}

	if m := sess.Match(); m != nil {
		wins := m.Score().Wins()
		score := make(map[string]any, len(wins))
		for pid, n := range wins {
			score[string(pid)] = n
		}
		if len(score) > 0 {
			data["gameScore"] = score
			data["matchWins"] = score
		}
		if w, ok := m.Winner(); ok {
			data["matchWinner"] = string(w)
			data["winnerPlayerId"] = string(w)
		}
	}

	return data
}

func spectatorEventType(room *domain.Room) string {
	if room == nil {
		return EventSnapshotSanitized
	}
	switch room.Status() {
	case domain.RoomStatusCompleted:
		return EventRoomCompleted
	case domain.RoomStatusCancelled:
		return EventRoomCancelled
	default:
		return EventSnapshotSanitized
	}
}

// BuildCompletionEvents emits AsyncAPI-shaped room.game.completed / room.match.completed.
// When g is non-nil, GameCompleted includes all authoritative participants with
// placement, card points, and outcomes from the completed Game (not winner-only).
func BuildCompletionEvents(
	room *domain.Room,
	g *game.Game,
	gameID string,
	sequence int64,
	correlationID, commandID string,
	facts []domain.Fact,
	occurredAt time.Time,
) []PublishedEvent {
	if room == nil {
		return nil
	}
	out := make([]PublishedEvent, 0, 2)
	at := occurredAt.UTC().Format(time.RFC3339Nano)
	for _, f := range facts {
		switch f.Name {
		case domain.FactGameCompleted:
			placement, participants := gameCompletedParticipants(g, f.Data)
			isAbandoned := f.Data["isAbandoned"] == "true"
			if g != nil && g.Abandoned() {
				isAbandoned = true
			}
			payload := map[string]any{
				"eventId":          commandID + "-game-completed",
				"eventType":        "GameCompleted",
				"schemaVersion":    1,
				"correlationId":    correlationID,
				"causationId":      commandID,
				"commandId":        commandID,
				"occurredAt":       at,
				"roomId":           string(room.ID()),
				"gameId":           firstNonEmpty(f.Data["gameId"], gameID),
				"roomType":         string(room.Type()),
				"isAbandoned":      isAbandoned,
				"authoritative":    true,
				"completed":        true,
				"placementOrder":   placement,
				"participants":     participants,
				"completionReason": firstNonEmpty(f.Data["completionReason"], "normal"),
			}
			raw, _ := json.Marshal(payload)
			out = append(out, PublishedEvent{
				Topic:          TopicGameCompleted,
				RoomID:         string(room.ID()),
				EventID:        commandID + "-game-completed",
				EventType:      "GameCompleted",
				SequenceNumber: sequence,
				SchemaVersion:  1,
				CorrelationID:  correlationID,
				CausationID:    commandID,
				OccurredAt:     at,
				Payload:        raw,
			})
		case domain.FactMatchCompleted:
			players := matchPlayersFromFact(f.Data)
			payload := map[string]any{
				"eventId":           commandID + "-match-completed",
				"eventType":         "MatchCompleted",
				"schemaVersion":     1,
				"correlationId":     correlationID,
				"causationId":       commandID,
				"occurredAt":        at,
				"roomId":            string(room.ID()),
				"completionVersion": atoiDefault(f.Data["completionVersion"], 1),
				"players":           players,
				"forfeits":          splitCSV(f.Data["forfeits"]),
				"isAbandoned":       f.Data["isAbandoned"] == "true",
			}
			if room.TournamentID().Valid() {
				payload["tournamentId"] = string(room.TournamentID())
			}
			if room.RoundNumber() > 0 {
				payload["roundNumber"] = room.RoundNumber()
			}
			if room.SlotID() != "" {
				payload["slotId"] = room.SlotID()
			}
			if tid := f.Data["tournamentId"]; tid != "" {
				payload["tournamentId"] = tid
			}
			if rn := f.Data["roundNumber"]; rn != "" {
				payload["roundNumber"] = atoiDefault(rn, room.RoundNumber())
			}
			if sid := f.Data["slotId"]; sid != "" {
				payload["slotId"] = sid
			}
			raw, _ := json.Marshal(payload)
			out = append(out, PublishedEvent{
				Topic:          TopicMatchCompleted,
				RoomID:         string(room.ID()),
				EventID:        commandID + "-match-completed",
				EventType:      "MatchCompleted",
				SequenceNumber: sequence,
				SchemaVersion:  1,
				CorrelationID:  correlationID,
				CausationID:    commandID,
				OccurredAt:     at,
				Payload:        raw,
			})
		}
	}
	return out
}

func matchPlayersFromFact(data map[string]string) []map[string]any {
	ranked := splitCSV(data["rankedPlayerIds"])
	wins := parseKeyedInts(data["matchWins"])
	pts := parseKeyedInts(data["cardPoints"])
	forfeits := map[string]struct{}{}
	for _, p := range splitCSV(data["forfeits"]) {
		forfeits[p] = struct{}{}
	}
	completedAt := data["completedAt"]
	out := make([]map[string]any, 0, len(ranked))
	for _, pid := range ranked {
		_, ff := forfeits[pid]
		out = append(out, map[string]any{
			"playerId":             pid,
			"matchWins":            wins[pid],
			"cumulativeCardPoints": pts[pid],
			"finalGameCompletedAt": completedAt,
			"forfeited":            ff,
		})
	}
	return out
}

func gameCompletedParticipants(g *game.Game, data map[string]string) (placement []string, participants []map[string]any) {
	if g != nil && g.Completed() {
		order := g.PlacementOrder()
		pts := g.CardPoints()
		placement = make([]string, 0, len(order))
		participants = make([]map[string]any, 0, len(order))
		for i, pid := range order {
			placement = append(placement, string(pid))
			outcome := "placed"
			if i == 0 {
				outcome = "won"
			}
			participants = append(participants, map[string]any{
				"playerId":   string(pid),
				"placement":  i + 1,
				"cardPoints": pts[pid],
				"outcome":    outcome,
			})
		}
		return placement, participants
	}
	placement = splitCSV(data["placementOrder"])
	if len(placement) == 0 && data["winner"] != "" {
		placement = []string{data["winner"]}
	}
	pts := parseKeyedInts(data["cardPoints"])
	participants = make([]map[string]any, 0, len(placement))
	for i, pid := range placement {
		outcome := "placed"
		if i == 0 {
			outcome = "won"
		}
		participants = append(participants, map[string]any{
			"playerId":   pid,
			"placement":  i + 1,
			"cardPoints": pts[pid],
			"outcome":    outcome,
		})
	}
	return placement, participants
}

func parseKeyedInts(s string) map[string]int {
	out := map[string]int{}
	for _, part := range splitCSV(s) {
		kv := strings.SplitN(part, ":", 2)
		if len(kv) != 2 {
			continue
		}
		out[kv[0]] = atoiDefault(kv[1], 0)
	}
	return out
}

func splitCSV(s string) []string {
	s = strings.TrimSpace(s)
	if s == "" {
		return []string{}
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func atoiDefault(s string, def int) int {
	n, err := strconv.Atoi(s)
	if err != nil {
		return def
	}
	return n
}

func firstNonEmpty(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

func copyStringMapAny(in map[string]string) map[string]string {
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
