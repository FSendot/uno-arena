package game

// MatchScore tracks best-of-three game wins. Completes at two wins.
type MatchScore struct {
	wins map[PlayerID]int
}

func NewMatchScore(players []PlayerID) MatchScore {
	wins := make(map[PlayerID]int, len(players))
	for _, p := range players {
		wins[p] = 0
	}
	return MatchScore{wins: wins}
}

func (m MatchScore) Wins() map[PlayerID]int {
	out := make(map[PlayerID]int, len(m.wins))
	for k, v := range m.wins {
		out[k] = v
	}
	return out
}

func (m MatchScore) WinCount(player PlayerID) int { return m.wins[player] }

func (m MatchScore) Completed() bool {
	for _, w := range m.wins {
		if w >= 2 {
			return true
		}
	}
	return false
}

func (m MatchScore) Winner() (PlayerID, bool) {
	for p, w := range m.wins {
		if w >= 2 {
			return p, true
		}
	}
	return "", false
}

func (m MatchScore) RecordWin(player PlayerID) (MatchScore, bool) {
	if m.Completed() {
		return m, false
	}
	if _, ok := m.wins[player]; !ok {
		return m, false
	}
	next := make(map[PlayerID]int, len(m.wins))
	for k, v := range m.wins {
		next[k] = v
	}
	next[player]++
	return MatchScore{wins: next}, true
}
