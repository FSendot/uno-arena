package game

import (
	"strconv"
	"strings"
	"time"
)

// Match tracks a best-of-three series with a pure MatchScore and tie-break inputs.
type Match struct {
	players       []PlayerID
	score         MatchScore
	cardPoints    map[PlayerID]int
	forfeits      []PlayerID
	completedAt   time.Time
	abandoned     bool
	completionVer int
}

func NewMatch(players []PlayerID) *Match {
	pts := make(map[PlayerID]int, len(players))
	for _, p := range players {
		pts[p] = 0
	}
	return &Match{
		players:    append([]PlayerID(nil), players...),
		score:      NewMatchScore(players),
		cardPoints: pts,
	}
}

func (m *Match) Score() MatchScore { return m.score }

func (m *Match) Players() []PlayerID {
	return append([]PlayerID(nil), m.players...)
}

func (m *Match) Completed() bool { return m.score.Completed() || m.abandoned }

func (m *Match) Winner() (PlayerID, bool) { return m.score.Winner() }

func (m *Match) CardPoints() map[PlayerID]int {
	out := make(map[PlayerID]int, len(m.cardPoints))
	for k, v := range m.cardPoints {
		out[k] = v
	}
	return out
}

func (m *Match) Forfeits() []PlayerID {
	return append([]PlayerID(nil), m.forfeits...)
}

func (m *Match) Abandoned() bool { return m.abandoned }

func (m *Match) CompletedAt() time.Time { return m.completedAt }

func (m *Match) RecordGameWin(winner PlayerID) (MatchScore, []Fact, bool, bool) {
	next, ok := m.score.RecordWin(winner)
	if !ok {
		return m.score, nil, false, false
	}
	m.score = next
	facts := []Fact{newFact(FactMatchScoreUpdated, map[string]string{"winner": string(winner)})}
	return m.score, facts, true, m.score.Completed()
}

func (m *Match) ApplyGameCompletion(g *Game, completedAt time.Time) (MatchScore, []Fact, bool, bool) {
	if g == nil || !g.Completed() {
		return m.score, nil, false, false
	}
	for p, pts := range g.CardPoints() {
		m.cardPoints[p] += pts
	}
	if g.Abandoned() {
		m.abandoned = true
	}
	place := g.PlacementOrder()
	if len(place) == 0 {
		return m.score, nil, false, false
	}
	score, facts, ok, matchDone := m.RecordGameWin(place[0])
	if matchDone || m.abandoned {
		m.completedAt = completedAt.UTC()
		m.completionVer++
		matchDone = true
	}
	return score, facts, ok, matchDone
}

// RecordForfeit notes a player forfeit for MatchCompleted tie-break markers.
func (m *Match) RecordForfeit(player PlayerID) {
	for _, p := range m.forfeits {
		if p == player {
			return
		}
	}
	m.forfeits = append(m.forfeits, player)
}

// MarkAbandoned completes the match without requiring two game wins.
func (m *Match) MarkAbandoned(completedAt time.Time) {
	m.abandoned = true
	m.completedAt = completedAt.UTC()
	m.completionVer++
}

// CompletionFacts builds authoritative MatchCompleted payload fields for tournament tie-break.
func (m *Match) CompletionFacts() map[string]string {
	wins := m.score.Wins()
	order := append([]PlayerID(nil), m.players...)
	for i := 0; i < len(order); i++ {
		best := i
		for j := i + 1; j < len(order); j++ {
			wj, wBest := wins[order[j]], wins[order[best]]
			pj, pBest := m.cardPoints[order[j]], m.cardPoints[order[best]]
			if wj > wBest || (wj == wBest && pj < pBest) || (wj == wBest && pj == pBest && j < best) {
				best = j
			}
		}
		order[i], order[best] = order[best], order[i]
	}
	winParts := make([]string, 0, len(order))
	pointParts := make([]string, 0, len(order))
	ranked := make([]string, 0, len(order))
	for _, p := range order {
		ranked = append(ranked, string(p))
		winParts = append(winParts, string(p)+":"+strconv.Itoa(wins[p]))
		pointParts = append(pointParts, string(p)+":"+strconv.Itoa(m.cardPoints[p]))
	}
	ff := make([]string, 0, len(m.forfeits))
	for _, p := range m.forfeits {
		ff = append(ff, string(p))
	}
	completedAt := m.completedAt
	if completedAt.IsZero() {
		completedAt = time.Now().UTC()
	}
	return map[string]string{
		"rankedPlayerIds":   strings.Join(ranked, ","),
		"matchWins":         strings.Join(winParts, ","),
		"cardPoints":        strings.Join(pointParts, ","),
		"completedAt":       completedAt.Format(time.RFC3339Nano),
		"forfeits":          strings.Join(ff, ","),
		"isAbandoned":       strconv.FormatBool(m.abandoned),
		"completionVersion": strconv.Itoa(m.completionVer),
	}
}
