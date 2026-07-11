package domain

import "testing"

func TestAnonymize_AdHocStripsPlayerIdentity(t *testing.T) {
	t.Parallel()
	p := NewPublicAnalyticsProjection()
	out := mustApply(t, p, gameplayEvent("evt_adhoc_1", map[string]any{
		"visibility":           "anonymized_adhoc",
		"metricType":           "card_played",
		"roomId":               "room_1",
		"gameId":               "game_1",
		"publicCard":           "red-7",
		"publicCardCountTotal": 42,
		"playerId":             "p_secret",
		"displayName":          "Alice",
	}))
	if !hasFact(out.Facts, FactPublicGameplayMetricProjected) {
		t.Fatalf("facts=%v", factNames(out.Facts))
	}
	snap := p.Snapshot()
	if snap.Authoritative || p.Authoritative() {
		t.Fatal("analytics snapshot must be non-authoritative")
	}
	if len(snap.GameplayMetrics) != 1 {
		t.Fatalf("metrics=%d", len(snap.GameplayMetrics))
	}
	m := snap.GameplayMetrics[0]
	if m.PublicPlayerID != "" || m.DisplayName != "" {
		t.Fatalf("ad-hoc retained identity: %+v", m)
	}
	if m.Visibility != VisibilityAnonymizedAdhoc {
		t.Fatalf("visibility=%s", m.Visibility)
	}
	if m.PublicCardRank != "7" || m.PublicCardColor != "red" {
		t.Fatalf("public card=%s/%s", m.PublicCardColor, m.PublicCardRank)
	}
	if m.RoomID != "room_1" || m.MetricType != "card_played" {
		t.Fatalf("metric=%+v", m)
	}
}

func TestAnonymize_PublicTournamentKeepsPublicPlayerFacts(t *testing.T) {
	t.Parallel()
	p := NewPublicAnalyticsProjection()
	mustApply(t, p, gameplayEvent("evt_tour_metric", map[string]any{
		"visibility":   "public_tournament",
		"metricType":   "card_played",
		"roomId":       "room_t1",
		"gameId":       "game_t1",
		"tournamentId": "tour_1",
		"publicCard":   "blue-2",
		"playerId":     "p1",
		"displayName":  "Bob",
		"roomSequence": 9,
	}))
	m := p.Snapshot().GameplayMetrics[0]
	if m.PublicPlayerID != "p1" || m.DisplayName != "Bob" {
		t.Fatalf("expected public player facts, got %+v", m)
	}
	if m.TournamentID != "tour_1" || m.RoomSequence != 9 {
		t.Fatalf("metric=%+v", m)
	}
}

func TestAnonymize_PublicWithoutTournamentRejectsIdentity(t *testing.T) {
	t.Parallel()
	p := NewPublicAnalyticsProjection()
	out := p.Apply(gameplayEvent("evt_public_no_tour", map[string]any{
		"visibility":  "public",
		"metricType":  "card_played",
		"roomId":      "room_1",
		"playerId":    "p1",
		"displayName": "Bob",
	}))
	if out.Kind != OutcomeQuarantined || out.Rejection == nil || out.Rejection.Code != RejectNonPublicSource {
		t.Fatalf("expected RejectNonPublicSource without tournament provenance, got %+v", out)
	}
	if len(p.Snapshot().GameplayMetrics) != 0 {
		t.Fatal("must not project player identity without tournament provenance")
	}
}

func TestAnonymize_PolicyStripsNestedIdentity(t *testing.T) {
	t.Parallel()
	pol := AnonymizationPolicy{}
	in := map[string]any{
		"metricType": "turn",
		"playerId":   "p1",
		"nested": map[string]any{
			"displayName": "Alice",
			"roomId":      "room_1",
		},
	}
	out := pol.AnonymizeGameplayPayload(in)
	if pol.ContainsIdentity(out) {
		t.Fatalf("still has identity: %#v", out)
	}
	if _, ok := out["playerId"]; ok {
		t.Fatal("playerId not stripped")
	}
	nested := out["nested"].(map[string]any)
	if _, ok := nested["displayName"]; ok {
		t.Fatal("nested displayName not stripped")
	}
	if nested["roomId"] != "room_1" {
		t.Fatalf("roomId=%v", nested["roomId"])
	}
	// Input must not be mutated.
	if in["playerId"] != "p1" {
		t.Fatal("anonymize mutated input")
	}
}
