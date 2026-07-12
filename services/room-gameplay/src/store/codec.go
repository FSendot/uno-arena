package store

import (
	"encoding/json"
	"fmt"
	"time"

	"unoarena/services/room-gameplay/domain"
	"unoarena/services/room-gameplay/game"
)

type engineStateJSON struct {
	ID            string                 `json:"id"`
	Seats         []string               `json:"seats"`
	Hands         map[string][]game.Card `json:"hands"`
	Discard       game.Card              `json:"discard"`
	Active        game.Color             `json:"active"`
	Dir           game.Direction         `json:"dir"`
	Current       int                    `json:"current"`
	Sequence      uint64                 `json:"sequence"`
	PendingColor  bool                   `json:"pendingColor"`
	ColorChooser  string                 `json:"colorChooser"`
	PenaltyAmount int                    `json:"penaltyAmount"`
	PenaltyTarget string                 `json:"penaltyTarget"`
	Uno           *unoWindowJSON         `json:"uno,omitempty"`
	Completed     bool                   `json:"completed"`
	Abandoned     bool                   `json:"abandoned"`
	Placement     []string               `json:"placement"`
	CardPoints    map[string]int         `json:"cardPoints"`
	Outcomes      map[string]outcomeJSON `json:"outcomes"`
	DrawPileSize  int                    `json:"drawPileSize"`
}

type unoWindowJSON struct {
	PlayerID         string    `json:"playerId"`
	ExpiresAt        time.Time `json:"expiresAt"`
	OpeningSequence  uint64    `json:"openingSequence"`
	Called           bool      `json:"called"`
	Open             bool      `json:"open"`
	ClosedByNextTurn bool      `json:"closedByNextTurn"`
}

type domainUnoJSON struct {
	PlayerID              string    `json:"playerId"`
	GameID                string    `json:"gameId"`
	TriggeringGameEventID string    `json:"triggeringGameEventId"`
	ExpiresAt             time.Time `json:"expiresAt"`
	OpeningSequence       uint64    `json:"openingSequence"`
	Open                  bool      `json:"open"`
	ClosedByNextTurn      bool      `json:"closedByNextTurn"`
}

type matchSnapshotJSON struct {
	Players       []string       `json:"players"`
	Wins          map[string]int `json:"wins"`
	CardPoints    map[string]int `json:"cardPoints"`
	Forfeits      []string       `json:"forfeits"`
	CompletedAt   time.Time      `json:"completedAt"`
	Abandoned     bool           `json:"abandoned"`
	CompletionVer int            `json:"completionVer"`
}

type outcomeJSON struct {
	Kind      string         `json:"kind"`
	CommandID string         `json:"commandId"`
	Sequence  uint64         `json:"sequence"`
	Rejection *rejectionJSON `json:"rejection,omitempty"`
	Facts     []factJSON     `json:"facts,omitempty"`
}

type rejectionJSON struct {
	Code              string `json:"code"`
	Message           string `json:"message"`
	SubmittedSequence uint64 `json:"submittedSequence"`
	CurrentSequence   uint64 `json:"currentSequence"`
}

type factJSON struct {
	Name string            `json:"name"`
	Data map[string]string `json:"data"`
}

func encodeGame(g *game.Game) ([]byte, error) {
	if g == nil {
		return []byte("null"), nil
	}
	st := engineStateJSON{
		ID:            string(g.ID()),
		Seats:         playerIDsToStrings(g.Seats()),
		Hands:         handsToJSON(g.HandsMap()),
		Discard:       g.DiscardTop(),
		Active:        g.ActiveColor(),
		Dir:           g.Direction(),
		Current:       g.CurrentSeatIndex(),
		Sequence:      uint64(g.Sequence()),
		PendingColor:  g.PendingColorChoice(),
		ColorChooser:  string(g.ColorChooser()),
		PenaltyAmount: g.PenaltyAmount(),
		PenaltyTarget: string(g.PenaltyTarget()),
		Completed:     g.Completed(),
		Abandoned:     g.Abandoned(),
		Placement:     playerIDsToStrings(g.PlacementOrder()),
		CardPoints:    intMapToJSON(g.CardPoints()),
		Outcomes:      gameOutcomesToJSON(g.OutcomesMap()),
		DrawPileSize:  g.DrawPileSize(),
	}
	if uw := g.UnoWindow(); uw != nil {
		st.Uno = &unoWindowJSON{
			PlayerID:         string(uw.PlayerID),
			ExpiresAt:        uw.ExpiresAt.UTC(),
			OpeningSequence:  uint64(uw.OpeningSequence),
			Called:           uw.Called,
			Open:             uw.IsOpen(),
			ClosedByNextTurn: uw.ClosedByNextTurn(),
		}
	}
	return json.Marshal(st)
}

func decodeGame(b []byte) (*game.Game, error) {
	if len(b) == 0 || string(b) == "null" {
		return nil, nil
	}
	var st engineStateJSON
	if err := json.Unmarshal(b, &st); err != nil {
		return nil, err
	}
	var uno *game.UnoWindow
	if st.Uno != nil {
		uw := game.RestoreUnoWindow(
			game.PlayerID(st.Uno.PlayerID),
			st.Uno.ExpiresAt,
			game.SequenceNumber(st.Uno.OpeningSequence),
			st.Uno.Called,
			st.Uno.Open,
			st.Uno.ClosedByNextTurn,
		)
		uno = &uw
	}
	if st.DrawPileSize < 0 {
		return nil, fmt.Errorf("drawPileSize must be >= 0")
	}
	return game.RestoreGame(game.RestoreGameInput{
		ID:            game.GameID(st.ID),
		Seats:         stringsToPlayerIDs(st.Seats),
		Hands:         handsFromJSON(st.Hands),
		Discard:       st.Discard,
		Active:        st.Active,
		Dir:           st.Dir,
		Current:       st.Current,
		Sequence:      game.SequenceNumber(st.Sequence),
		PendingColor:  st.PendingColor,
		ColorChooser:  game.PlayerID(st.ColorChooser),
		PenaltyAmount: st.PenaltyAmount,
		PenaltyTarget: game.PlayerID(st.PenaltyTarget),
		Uno:           uno,
		Completed:     st.Completed,
		Abandoned:     st.Abandoned,
		Placement:     stringsToPlayerIDs(st.Placement),
		CardPoints:    intMapFromJSON(st.CardPoints),
		Outcomes:      gameOutcomesFromJSON(st.Outcomes),
		DrawPileSize:  st.DrawPileSize,
	}), nil
}

func encodeMatch(m *game.Match) ([]byte, error) {
	if m == nil {
		return []byte("null"), nil
	}
	return json.Marshal(matchSnapshotJSON{
		Players:       playerIDsToStrings(m.Players()),
		Wins:          intMapToJSON(m.Score().Wins()),
		CardPoints:    intMapToJSON(m.CardPoints()),
		Forfeits:      playerIDsToStrings(m.Forfeits()),
		CompletedAt:   m.CompletedAt().UTC(),
		Abandoned:     m.Abandoned(),
		CompletionVer: m.CompletionVer(),
	})
}

func decodeMatch(b []byte) (*game.Match, error) {
	if len(b) == 0 || string(b) == "null" {
		return nil, nil
	}
	var st matchSnapshotJSON
	if err := json.Unmarshal(b, &st); err != nil {
		return nil, err
	}
	return game.RestoreMatch(game.RestoreMatchInput{
		Players:       stringsToPlayerIDs(st.Players),
		Wins:          intMapFromJSON(st.Wins),
		CardPoints:    intMapFromJSON(st.CardPoints),
		Forfeits:      stringsToPlayerIDs(st.Forfeits),
		CompletedAt:   st.CompletedAt,
		Abandoned:     st.Abandoned,
		CompletionVer: st.CompletionVer,
	}), nil
}

func encodeDomainUnoExact(w domain.UnoWindow, has bool) ([]byte, error) {
	if !has {
		return []byte("null"), nil
	}
	return json.Marshal(domainUnoJSON{
		PlayerID:              string(w.PlayerID),
		GameID:                string(w.GameID),
		TriggeringGameEventID: string(w.TriggeringGameEventID),
		ExpiresAt:             w.ExpiresAt.UTC(),
		OpeningSequence:       uint64(w.OpeningSequence),
		Open:                  w.IsOpen(),
		ClosedByNextTurn:      w.ClosedByNextTurn(),
	})
}

func decodeDomainUno(b []byte) (domain.UnoWindow, bool, error) {
	if len(b) == 0 || string(b) == "null" {
		return domain.UnoWindow{}, false, nil
	}
	var st domainUnoJSON
	if err := json.Unmarshal(b, &st); err != nil {
		return domain.UnoWindow{}, false, err
	}
	return domain.RestoreUnoWindow(
		domain.PlayerID(st.PlayerID),
		domain.GameID(st.GameID),
		domain.TriggeringGameEventID(st.TriggeringGameEventID),
		st.ExpiresAt,
		domain.SequenceNumber(st.OpeningSequence),
		st.Open,
		st.ClosedByNextTurn,
	), true, nil
}

func playerIDsToStrings(in []game.PlayerID) []string {
	out := make([]string, len(in))
	for i, p := range in {
		out[i] = string(p)
	}
	return out
}

func stringsToPlayerIDs(in []string) []game.PlayerID {
	out := make([]game.PlayerID, len(in))
	for i, p := range in {
		out[i] = game.PlayerID(p)
	}
	return out
}

func handsToJSON(in map[game.PlayerID][]game.Card) map[string][]game.Card {
	if in == nil {
		return nil
	}
	out := make(map[string][]game.Card, len(in))
	for k, v := range in {
		out[string(k)] = v
	}
	return out
}

func handsFromJSON(in map[string][]game.Card) map[game.PlayerID][]game.Card {
	if in == nil {
		return nil
	}
	out := make(map[game.PlayerID][]game.Card, len(in))
	for k, v := range in {
		out[game.PlayerID(k)] = v
	}
	return out
}

func intMapToJSON(in map[game.PlayerID]int) map[string]int {
	if in == nil {
		return nil
	}
	out := make(map[string]int, len(in))
	for k, v := range in {
		out[string(k)] = v
	}
	return out
}

func intMapFromJSON(in map[string]int) map[game.PlayerID]int {
	if in == nil {
		return nil
	}
	out := make(map[game.PlayerID]int, len(in))
	for k, v := range in {
		out[game.PlayerID(k)] = v
	}
	return out
}

func gameOutcomesToJSON(in map[game.CommandID]game.CommandOutcome) map[string]outcomeJSON {
	if in == nil {
		return nil
	}
	out := make(map[string]outcomeJSON, len(in))
	for k, v := range in {
		out[string(k)] = outcomeFromGame(v)
	}
	return out
}

func gameOutcomesFromJSON(in map[string]outcomeJSON) map[game.CommandID]game.CommandOutcome {
	if in == nil {
		return nil
	}
	out := make(map[game.CommandID]game.CommandOutcome, len(in))
	for k, v := range in {
		out[game.CommandID(k)] = outcomeToGame(v)
	}
	return out
}

func outcomeFromGame(v game.CommandOutcome) outcomeJSON {
	o := outcomeJSON{
		Kind:      string(v.Kind),
		CommandID: string(v.CommandID),
		Sequence:  uint64(v.Sequence),
	}
	if v.Rejection != nil {
		o.Rejection = &rejectionJSON{
			Code:              string(v.Rejection.Code),
			Message:           v.Rejection.Message,
			SubmittedSequence: uint64(v.Rejection.SubmittedSequence),
			CurrentSequence:   uint64(v.Rejection.CurrentSequence),
		}
	}
	for _, f := range v.Facts {
		o.Facts = append(o.Facts, factJSON{Name: string(f.Name), Data: f.Data})
	}
	return o
}

func outcomeToGame(v outcomeJSON) game.CommandOutcome {
	o := game.CommandOutcome{
		Kind:      game.OutcomeKind(v.Kind),
		CommandID: game.CommandID(v.CommandID),
		Sequence:  game.SequenceNumber(v.Sequence),
	}
	if v.Rejection != nil {
		o.Rejection = &game.Rejection{
			Code:              game.RejectionCode(v.Rejection.Code),
			Message:           v.Rejection.Message,
			SubmittedSequence: game.SequenceNumber(v.Rejection.SubmittedSequence),
			CurrentSequence:   game.SequenceNumber(v.Rejection.CurrentSequence),
		}
	}
	for _, f := range v.Facts {
		o.Facts = append(o.Facts, game.Fact{Name: game.FactName(f.Name), Data: f.Data})
	}
	return o
}

func domainOutcomesToJSON(in map[domain.CommandID]domain.CommandOutcome) ([]byte, error) {
	out := make(map[string]outcomeJSON, len(in))
	for k, v := range in {
		o := outcomeJSON{
			Kind:      string(v.Kind),
			CommandID: string(v.CommandID),
			Sequence:  uint64(v.Sequence),
		}
		if v.Rejection != nil {
			o.Rejection = &rejectionJSON{
				Code:              string(v.Rejection.Code),
				Message:           v.Rejection.Message,
				SubmittedSequence: uint64(v.Rejection.SubmittedSequence),
				CurrentSequence:   uint64(v.Rejection.CurrentSequence),
			}
		}
		for _, f := range v.Facts {
			o.Facts = append(o.Facts, factJSON{Name: string(f.Name), Data: f.Data})
		}
		out[string(k)] = o
	}
	return json.Marshal(out)
}

func domainOutcomesFromJSON(b []byte) (map[domain.CommandID]domain.CommandOutcome, error) {
	if len(b) == 0 || string(b) == "{}" || string(b) == "null" {
		return map[domain.CommandID]domain.CommandOutcome{}, nil
	}
	var raw map[string]outcomeJSON
	if err := json.Unmarshal(b, &raw); err != nil {
		return nil, err
	}
	out := make(map[domain.CommandID]domain.CommandOutcome, len(raw))
	for k, v := range raw {
		o := domain.CommandOutcome{
			Kind:      domain.OutcomeKind(v.Kind),
			CommandID: domain.CommandID(v.CommandID),
			Sequence:  domain.SequenceNumber(v.Sequence),
		}
		if v.Rejection != nil {
			o.Rejection = &domain.Rejection{
				Code:              domain.RejectionCode(v.Rejection.Code),
				Message:           v.Rejection.Message,
				SubmittedSequence: domain.SequenceNumber(v.Rejection.SubmittedSequence),
				CurrentSequence:   domain.SequenceNumber(v.Rejection.CurrentSequence),
			}
		}
		for _, f := range v.Facts {
			o.Facts = append(o.Facts, domain.Fact{Name: domain.FactName(f.Name), Data: f.Data})
		}
		out[domain.CommandID(k)] = o
	}
	return out, nil
}

func encodeOutcomeBody(out domain.CommandOutcome) ([]byte, error) {
	o := outcomeJSON{
		Kind:      string(out.Kind),
		CommandID: string(out.CommandID),
		Sequence:  uint64(out.Sequence),
	}
	if out.Rejection != nil {
		o.Rejection = &rejectionJSON{
			Code:              string(out.Rejection.Code),
			Message:           out.Rejection.Message,
			SubmittedSequence: uint64(out.Rejection.SubmittedSequence),
			CurrentSequence:   uint64(out.Rejection.CurrentSequence),
		}
	}
	for _, f := range out.Facts {
		o.Facts = append(o.Facts, factJSON{Name: string(f.Name), Data: f.Data})
	}
	return json.Marshal(o)
}

func decodeOutcomeBody(b []byte) (domain.CommandOutcome, error) {
	var v outcomeJSON
	if err := json.Unmarshal(b, &v); err != nil {
		return domain.CommandOutcome{}, err
	}
	o := domain.CommandOutcome{
		Kind:      domain.OutcomeKind(v.Kind),
		CommandID: domain.CommandID(v.CommandID),
		Sequence:  domain.SequenceNumber(v.Sequence),
	}
	if v.Rejection != nil {
		o.Rejection = &domain.Rejection{
			Code:              domain.RejectionCode(v.Rejection.Code),
			Message:           v.Rejection.Message,
			SubmittedSequence: domain.SequenceNumber(v.Rejection.SubmittedSequence),
			CurrentSequence:   domain.SequenceNumber(v.Rejection.CurrentSequence),
		}
	}
	for _, f := range v.Facts {
		o.Facts = append(o.Facts, domain.Fact{Name: domain.FactName(f.Name), Data: f.Data})
	}
	return o, nil
}

func outcomeStatus(out domain.CommandOutcome) string {
	switch out.Kind {
	case domain.OutcomeAccepted, domain.OutcomeDuplicate:
		if out.Rejection != nil {
			return "rejected"
		}
		return "accepted"
	case domain.OutcomeRejected:
		return "rejected"
	default:
		return "failed"
	}
}

// sessionSnapshotJSON is the full session blob stored in reconciliation markers.
type sessionSnapshotJSON struct {
	RoomID               string          `json:"roomId"`
	RoomType             string          `json:"roomType"`
	TournamentID         string          `json:"tournamentId,omitempty"`
	RoundNumber          int             `json:"roundNumber,omitempty"`
	SlotID               string          `json:"slotId,omitempty"`
	Status               string          `json:"status"`
	Visibility           string          `json:"visibility"`
	HostID               string          `json:"hostId,omitempty"`
	Capacity             int             `json:"capacity"`
	Sequence             int64           `json:"sequence"`
	Seats                []seatSnapJSON  `json:"seats"`
	Outcomes             json.RawMessage `json:"outcomes"`
	Disconnects          json.RawMessage `json:"disconnects"`
	NextDisconnect       json.RawMessage `json:"nextDisconnectVersions"`
	UnoWindow            json.RawMessage `json:"unoWindow"`
	HasUno               bool            `json:"hasUno"`
	GameCompletedInMatch bool            `json:"gameCompletedInMatch"`
	Game                 json.RawMessage `json:"game"`
	Match                json.RawMessage `json:"match"`
	GameID               string          `json:"gameId,omitempty"`
	UsedGameIDs          json.RawMessage `json:"usedGameIds"`
	SkippedTurns         json.RawMessage `json:"skippedTurns"`
	TurnVersion          int64           `json:"turnVersion"`
}

type seatSnapJSON struct {
	Index    int    `json:"index"`
	PlayerID string `json:"playerId"`
	Occupied bool   `json:"occupied"`
}

// EncodeSessionSnapshot serializes a session for reconciliation repair payloads.
func EncodeSessionSnapshot(sess *domain.Session) ([]byte, error) {
	return encodeSessionSnapshot(sess)
}

// DecodeSessionSnapshot restores a session from a reconciliation snapshot blob.
func DecodeSessionSnapshot(b []byte) (*domain.Session, error) {
	return decodeSessionSnapshot(b)
}

func encodeSessionSnapshot(sess *domain.Session) ([]byte, error) {
	if sess == nil || sess.Room() == nil {
		return nil, fmt.Errorf("session required")
	}
	room := sess.Room()
	outcomes, err := domainOutcomesToJSON(room.OutcomesMap())
	if err != nil {
		return nil, err
	}
	disc, err := encodeDisconnects(room.DisconnectsMap())
	if err != nil {
		return nil, err
	}
	next, err := encodeNextDisconnect(room.NextDisconnectVersions())
	if err != nil {
		return nil, err
	}
	uno, hasUno := room.UnoWindow()
	unoJSON, err := encodeDomainUnoExact(uno, hasUno)
	if err != nil {
		return nil, err
	}
	gameJSON, err := encodeGame(sess.Game())
	if err != nil {
		return nil, err
	}
	matchJSON, err := encodeMatch(sess.Match())
	if err != nil {
		return nil, err
	}
	usedJSON, err := encodeUsedGameIDs(sess.UsedGameIDs())
	if err != nil {
		return nil, err
	}
	skippedJSON, err := encodeSkipped(sess.SkippedTurns())
	if err != nil {
		return nil, err
	}
	var seats []seatSnapJSON
	for _, s := range room.Roster().Seats() {
		if !s.Occupied {
			continue
		}
		seats = append(seats, seatSnapJSON{
			Index: int(s.Index), PlayerID: string(s.PlayerID), Occupied: true,
		})
	}
	return json.Marshal(sessionSnapshotJSON{
		RoomID:               string(room.ID()),
		RoomType:             string(room.Type()),
		TournamentID:         string(room.TournamentID()),
		RoundNumber:          room.RoundNumber(),
		SlotID:               room.SlotID(),
		Status:               string(room.Status()),
		Visibility:           string(room.Visibility()),
		HostID:               string(room.HostID()),
		Capacity:             room.Roster().Capacity(),
		Sequence:             int64(room.Sequence()),
		Seats:                seats,
		Outcomes:             outcomes,
		Disconnects:          disc,
		NextDisconnect:       next,
		UnoWindow:            unoJSON,
		HasUno:               hasUno,
		GameCompletedInMatch: room.GameCompletedInMatch(),
		Game:                 gameJSON,
		Match:                matchJSON,
		GameID:               string(sess.GameID()),
		UsedGameIDs:          usedJSON,
		SkippedTurns:         skippedJSON,
		TurnVersion:          int64(sess.TurnVersion()),
	})
}

func decodeSessionSnapshot(b []byte) (*domain.Session, error) {
	if len(b) == 0 || string(b) == "null" {
		return nil, fmt.Errorf("empty session snapshot")
	}
	var st sessionSnapshotJSON
	if err := json.Unmarshal(b, &st); err != nil {
		return nil, err
	}
	outcomes, err := domainOutcomesFromJSON(st.Outcomes)
	if err != nil {
		return nil, err
	}
	disc, err := decodeDisconnects(st.Disconnects)
	if err != nil {
		return nil, err
	}
	next, err := decodeNextDisconnect(st.NextDisconnect)
	if err != nil {
		return nil, err
	}
	uno, hasUno, err := decodeDomainUno(st.UnoWindow)
	if err != nil {
		return nil, err
	}
	if !st.HasUno {
		hasUno = false
	}
	var seats []domain.Seat
	for _, s := range st.Seats {
		seats = append(seats, domain.Seat{
			Index: domain.SeatIndex(s.Index), PlayerID: domain.PlayerID(s.PlayerID), Occupied: s.Occupied,
		})
	}
	g, err := decodeGame(st.Game)
	if err != nil {
		return nil, err
	}
	m, err := decodeMatch(st.Match)
	if err != nil {
		return nil, err
	}
	used, err := decodeUsedGameIDs(st.UsedGameIDs)
	if err != nil {
		return nil, err
	}
	skipped, err := decodeSkipped(st.SkippedTurns)
	if err != nil {
		return nil, err
	}
	room := domain.RestoreRoom(domain.RestoreRoomInput{
		ID:                   domain.RoomID(st.RoomID),
		RoomType:             domain.RoomType(st.RoomType),
		TournamentID:         domain.TournamentID(st.TournamentID),
		RoundNumber:          st.RoundNumber,
		SlotID:               st.SlotID,
		Status:               domain.RoomStatus(st.Status),
		Visibility:           domain.Visibility(st.Visibility),
		HostID:               domain.PlayerID(st.HostID),
		Roster:               domain.RestoreRoster(st.Capacity, seats),
		Sequence:             domain.SequenceNumber(st.Sequence),
		Outcomes:             outcomes,
		Disconnects:          disc,
		NextDisconnectVer:    next,
		UnoWindow:            uno,
		HasUno:               hasUno,
		GameCompletedInMatch: st.GameCompletedInMatch,
	})
	return domain.RestoreSession(room, g, m, domain.GameID(st.GameID), used, domain.SequenceNumber(st.TurnVersion), skipped), nil
}
