package domain

import "strconv"

// PlayerRating is the aggregate consistency boundary for one player's
// persistent casual Elo and tournament placement rating.
// Pure stdlib; not goroutine-safe — callers serialize access.
type PlayerRating struct {
	playerID                  PlayerID
	config                    RatingConfig
	casualElo                 int
	tournamentPlacementRating int

	history []RatingHistoryEntry

	outcomes        map[CommandID]CommandOutcome
	processedEvents map[EventID]CommandOutcome
	casualKeys      map[GameID]CommandOutcome // (playerId, gameId) scoped to this aggregate
	tournamentKeys  map[string]CommandOutcome // (playerId, tournamentId, placementEventId)
}

// NewPlayerRating creates a PlayerRating with the given configuration.
func NewPlayerRating(playerID PlayerID, cfg RatingConfig) *PlayerRating {
	cfg = cfg.withDefaults()
	return &PlayerRating{
		playerID:                  playerID,
		config:                    cfg,
		casualElo:                 cfg.InitialCasualElo,
		tournamentPlacementRating: cfg.InitialTournamentRating,
		history:                   nil,
		outcomes:                  map[CommandID]CommandOutcome{},
		processedEvents:           map[EventID]CommandOutcome{},
		casualKeys:                map[GameID]CommandOutcome{},
		tournamentKeys:            map[string]CommandOutcome{},
	}
}

// RestorePlayerRating rebuilds an aggregate from durable rating values without history.
// Prior history remains authoritative in Postgres; empty in-memory history/keys mean
// this instance only applies the next command in the current transaction.
func RestorePlayerRating(playerID PlayerID, cfg RatingConfig, casualElo, tournamentPlacementRating int) *PlayerRating {
	cfg = cfg.withDefaults()
	return &PlayerRating{
		playerID:                  playerID,
		config:                    cfg,
		casualElo:                 casualElo,
		tournamentPlacementRating: tournamentPlacementRating,
		history:                   nil,
		outcomes:                  map[CommandID]CommandOutcome{},
		processedEvents:           map[EventID]CommandOutcome{},
		casualKeys:                map[GameID]CommandOutcome{},
		tournamentKeys:            map[string]CommandOutcome{},
	}
}

func (p *PlayerRating) PlayerID() PlayerID { return p.playerID }
func (p *PlayerRating) CasualElo() EloRating {
	return EloRating{Value: p.casualElo}
}
func (p *PlayerRating) TournamentPlacement() TournamentPlacementRating {
	return TournamentPlacementRating{Value: p.tournamentPlacementRating}
}
func (p *PlayerRating) TournamentPlacementRating() int { return p.tournamentPlacementRating }
func (p *PlayerRating) Floor() int                     { return p.config.Floor }
func (p *PlayerRating) KFactor() int                   { return p.config.KFactor }

// PublicSnapshot returns a public view of this player's ratings.
func (p *PlayerRating) PublicSnapshot() PlayerRatingSnapshot {
	return PlayerRatingSnapshot{
		PlayerID:                  p.playerID,
		CasualElo:                 p.casualElo,
		TournamentPlacementRating: p.tournamentPlacementRating,
	}
}

// History returns a defensive copy of immutable rating history entries.
func (p *PlayerRating) History() []RatingHistoryEntry {
	if len(p.history) == 0 {
		return nil
	}
	out := make([]RatingHistoryEntry, len(p.history))
	copy(out, p.history)
	return out
}

// ApplyCasualEloUpdate applies casual Elo from an authoritative completed non-abandoned ad_hoc game.
// Tournament and abandoned games are rejected without mutating state.
func (p *PlayerRating) ApplyCasualEloUpdate(cmd ApplyCasualEloUpdateCommand) CommandOutcome {
	if prior, ok := p.outcomes[cmd.CommandID]; ok {
		return duplicateOutcome(prior)
	}
	if cmd.EventID.Valid() {
		if prior, ok := p.processedEvents[cmd.EventID]; ok {
			return p.rememberDuplicate(cmd.CommandID, prior)
		}
	}
	if cmd.GameID.Valid() {
		if prior, ok := p.casualKeys[cmd.GameID]; ok {
			return p.rememberDuplicate(cmd.CommandID, prior)
		}
	}

	if !cmd.CommandID.Valid() || !cmd.PlayerID.Valid() || !cmd.GameID.Valid() {
		return p.reject(cmd.CommandID, Rejection{
			Code:    RejectInvalidIdentity,
			Message: "casual elo requires commandId, playerId, and gameId",
		})
	}
	if cmd.PlayerID != p.playerID {
		return p.reject(cmd.CommandID, Rejection{
			Code:    RejectInvalidIdentity,
			Message: "command playerId does not match aggregate",
		})
	}
	if !cmd.Authoritative {
		return p.reject(cmd.CommandID, Rejection{
			Code:    RejectNotAuthoritative,
			Message: "casual elo requires an authoritative GameCompleted",
		})
	}
	if !cmd.Completed {
		return p.reject(cmd.CommandID, Rejection{
			Code:    RejectIneligibleGame,
			Message: "casual elo requires a completed game",
		})
	}
	if cmd.IsAbandoned {
		return p.reject(cmd.CommandID, Rejection{
			Code:    RejectAbandonedGame,
			Message: "abandoned games do not update casual elo",
		})
	}
	if cmd.RoomType == RoomTypeTournament {
		return p.reject(cmd.CommandID, Rejection{
			Code:    RejectTournamentGame,
			Message: "tournament games do not update casual elo",
		})
	}
	if cmd.RoomType != RoomTypeAdHoc {
		return p.reject(cmd.CommandID, Rejection{
			Code:    RejectIneligibleGame,
			Message: "casual elo requires roomType=ad_hoc",
		})
	}

	standings := make([]RatedPlacement, 0, len(cmd.Participants))
	seen := map[PlayerID]struct{}{}
	var selfPlacement int
	selfFound := false
	for _, part := range cmd.Participants {
		if !part.PlayerID.Valid() || part.Placement < 1 {
			return p.reject(cmd.CommandID, Rejection{
				Code:    RejectInvalidOpponents,
				Message: "each participant requires playerId and placement >= 1",
			})
		}
		if _, dup := seen[part.PlayerID]; dup {
			return p.reject(cmd.CommandID, Rejection{
				Code:    RejectInvalidOpponents,
				Message: "duplicate participant playerId",
			})
		}
		seen[part.PlayerID] = struct{}{}
		rating := part.Rating
		if part.PlayerID == p.playerID {
			rating = p.casualElo
			selfPlacement = part.Placement
			selfFound = true
		}
		standings = append(standings, RatedPlacement{
			PlayerID:  part.PlayerID,
			Rating:    rating,
			Placement: part.Placement,
		})
	}
	if !selfFound {
		return p.reject(cmd.CommandID, Rejection{
			Code:    RejectInvalidOpponents,
			Message: "participants must include the aggregate player",
		})
	}
	if len(standings) < 2 {
		return p.reject(cmd.CommandID, Rejection{
			Code:    RejectInvalidOpponents,
			Message: "pairwise elo requires at least two participants",
		})
	}

	deltas := ComputePairwiseEloDeltas(standings, p.config.KFactor)
	rawDelta := deltas[p.playerID]
	previous := p.casualElo
	next := ApplyFloor(previous+rawDelta, p.config.Floor)
	appliedDelta := next - previous

	p.casualElo = next
	p.history = append(p.history, RatingHistoryEntry{
		SourceType:     SourceCasualElo,
		PreviousRating: previous,
		NewRating:      next,
		Delta:          appliedDelta,
		Reason:         ReasonCasualGameCompleted,
		GameID:         cmd.GameID,
		RoomID:         cmd.RoomID,
		EventID:        cmd.EventID,
		Placement:      selfPlacement,
	})

	fact := newFact(FactPlayerRatingUpdated, map[string]string{
		"playerId":       string(p.playerID),
		"gameId":         string(cmd.GameID),
		"previousRating": strconv.Itoa(previous),
		"newRating":      strconv.Itoa(next),
		"delta":          strconv.Itoa(appliedDelta),
		"placement":      strconv.Itoa(selfPlacement),
		"sourceType":     string(SourceCasualElo),
	})
	if cmd.EventID.Valid() {
		fact.Data["eventId"] = string(cmd.EventID)
	}
	if cmd.RoomID.Valid() {
		fact.Data["roomId"] = string(cmd.RoomID)
	}
	out := acceptedOutcome(cmd.CommandID, []Fact{fact})
	p.storeAccepted(cmd.CommandID, cmd.EventID, out)
	p.casualKeys[cmd.GameID] = out
	return out
}

// ApplyTournamentPlacementUpdate updates tournament placement rating from placement/final standing facts.
// Never mutates casual Elo.
func (p *PlayerRating) ApplyTournamentPlacementUpdate(cmd ApplyTournamentPlacementUpdateCommand) CommandOutcome {
	if prior, ok := p.outcomes[cmd.CommandID]; ok {
		return duplicateOutcome(prior)
	}
	if cmd.EventID.Valid() {
		if prior, ok := p.processedEvents[cmd.EventID]; ok {
			return p.rememberDuplicate(cmd.CommandID, prior)
		}
	}
	bizKey := tournamentBizKey(cmd.TournamentID, cmd.PlacementEventID)
	if cmd.TournamentID.Valid() && cmd.PlacementEventID.Valid() {
		if prior, ok := p.tournamentKeys[bizKey]; ok {
			return p.rememberDuplicate(cmd.CommandID, prior)
		}
	}

	if !cmd.CommandID.Valid() || !cmd.PlayerID.Valid() || !cmd.TournamentID.Valid() || !cmd.PlacementEventID.Valid() {
		return p.reject(cmd.CommandID, Rejection{
			Code:    RejectInvalidIdentity,
			Message: "tournament placement requires commandId, playerId, tournamentId, placementEventId",
		})
	}
	if cmd.PlayerID != p.playerID {
		return p.reject(cmd.CommandID, Rejection{
			Code:    RejectInvalidIdentity,
			Message: "command playerId does not match aggregate",
		})
	}
	if cmd.Placement < 1 {
		return p.reject(cmd.CommandID, Rejection{
			Code:    RejectInvalidPlacement,
			Message: "placement must be >= 1",
		})
	}
	reason := cmd.Reason
	if reason == "" {
		reason = ReasonTournamentPlacement
	}
	if reason != ReasonTournamentPlacement && reason != ReasonTournamentFinalStanding {
		return p.reject(cmd.CommandID, Rejection{
			Code:    RejectInvalidCommand,
			Message: "reason must be tournament_placement or tournament_final_standing",
		})
	}

	previous := p.tournamentPlacementRating
	next := ApplyFloor(previous+cmd.Delta, p.config.Floor)
	appliedDelta := next - previous

	p.tournamentPlacementRating = next
	p.history = append(p.history, RatingHistoryEntry{
		SourceType:       SourceTournamentPlacement,
		PreviousRating:   previous,
		NewRating:        next,
		Delta:            appliedDelta,
		Reason:           reason,
		EventID:          cmd.EventID,
		TournamentID:     cmd.TournamentID,
		PlacementEventID: cmd.PlacementEventID,
		Placement:        cmd.Placement,
	})

	fact := newFact(FactTournamentPlacementRatingUpdated, map[string]string{
		"playerId":         string(p.playerID),
		"tournamentId":     string(cmd.TournamentID),
		"placementEventId": string(cmd.PlacementEventID),
		"previousRating":   strconv.Itoa(previous),
		"newRating":        strconv.Itoa(next),
		"delta":            strconv.Itoa(appliedDelta),
		"placement":        strconv.Itoa(cmd.Placement),
		"sourceType":       string(SourceTournamentPlacement),
		"reason":           string(reason),
	})
	if cmd.EventID.Valid() {
		fact.Data["eventId"] = string(cmd.EventID)
	}
	out := acceptedOutcome(cmd.CommandID, []Fact{fact})
	p.storeAccepted(cmd.CommandID, cmd.EventID, out)
	p.tournamentKeys[bizKey] = out
	return out
}

func (p *PlayerRating) reject(commandID CommandID, rej Rejection) CommandOutcome {
	out := rejectedOutcome(commandID, rej)
	if commandID.Valid() {
		p.outcomes[commandID] = out
	}
	return out
}

func (p *PlayerRating) rememberDuplicate(commandID CommandID, prior CommandOutcome) CommandOutcome {
	dup := duplicateOutcome(prior)
	if commandID.Valid() {
		p.outcomes[commandID] = dup
	}
	return dup
}

func (p *PlayerRating) storeAccepted(commandID CommandID, eventID EventID, out CommandOutcome) {
	p.outcomes[commandID] = out
	if eventID.Valid() {
		p.processedEvents[eventID] = out
	}
}

func tournamentBizKey(tournamentID TournamentID, placementEventID PlacementEventID) string {
	return string(tournamentID) + "\x00" + string(placementEventID)
}
