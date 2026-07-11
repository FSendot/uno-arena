package game

// Clone returns a deep copy of the game aggregate for staging before commit.
func (g *Game) Clone() *Game {
	if g == nil {
		return nil
	}
	out := &Game{
		id:            g.id,
		seats:         append([]PlayerID(nil), g.seats...),
		hands:         cloneHands(g.hands),
		discard:       g.discard,
		active:        g.active,
		dir:           g.dir,
		current:       g.current,
		sequence:      g.sequence,
		pendingColor:  g.pendingColor,
		colorChooser:  g.colorChooser,
		penaltyAmount: g.penaltyAmount,
		penaltyTarget: g.penaltyTarget,
		completed:     g.completed,
		abandoned:     g.abandoned,
		placement:     append([]PlayerID(nil), g.placement...),
		cardPoints:    cloneIntMap(g.cardPoints),
		outcomes:      cloneOutcomes(g.outcomes),
	}
	if g.uno != nil {
		uw := *g.uno
		out.uno = &uw
	}
	return out
}

// Clone returns a deep copy of the match aggregate.
func (m *Match) Clone() *Match {
	if m == nil {
		return nil
	}
	return &Match{
		players:       append([]PlayerID(nil), m.players...),
		score:         cloneMatchScore(m.score),
		cardPoints:    cloneIntMap(m.cardPoints),
		forfeits:      append([]PlayerID(nil), m.forfeits...),
		completedAt:   m.completedAt,
		abandoned:     m.abandoned,
		completionVer: m.completionVer,
	}
}

func cloneHands(in map[PlayerID][]Card) map[PlayerID][]Card {
	if in == nil {
		return nil
	}
	out := make(map[PlayerID][]Card, len(in))
	for k, v := range in {
		out[k] = cloneHand(v)
	}
	return out
}

func cloneIntMap(in map[PlayerID]int) map[PlayerID]int {
	if in == nil {
		return nil
	}
	out := make(map[PlayerID]int, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func cloneOutcomes(in map[CommandID]CommandOutcome) map[CommandID]CommandOutcome {
	if in == nil {
		return nil
	}
	out := make(map[CommandID]CommandOutcome, len(in))
	for k, v := range in {
		cp := v
		if v.Rejection != nil {
			r := *v.Rejection
			cp.Rejection = &r
		}
		if v.Facts != nil {
			cp.Facts = append([]Fact(nil), v.Facts...)
			for i := range cp.Facts {
				cp.Facts[i].Data = copyStringMap(cp.Facts[i].Data)
			}
		}
		out[k] = cp
	}
	return out
}

func cloneMatchScore(in MatchScore) MatchScore {
	return MatchScore{wins: cloneIntMap(in.wins)}
}

func copyStringMap(in map[string]string) map[string]string {
	if in == nil {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
