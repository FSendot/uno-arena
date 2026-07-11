package domain

import "testing"

func TestApply_DuplicateEventIDStable(t *testing.T) {
	t.Parallel()
	p := NewPublicAnalyticsProjection()
	evt := gameplayEvent("dup_1", map[string]any{
		"visibility": "anonymized_adhoc",
		"metricType": "turn_advanced",
		"roomId":     "room_1",
	})
	first := mustApply(t, p, evt)
	if len(p.Snapshot().GameplayMetrics) != 1 {
		t.Fatal("expected one metric")
	}
	second := p.Apply(evt)
	if second.Kind != OutcomeDuplicate {
		t.Fatalf("kind=%s want duplicate", second.Kind)
	}
	if second.Rejection != nil {
		t.Fatalf("duplicate must be stable success, got %+v", second.Rejection)
	}
	if !hasFact(second.Facts, FactPublicGameplayMetricProjected) {
		t.Fatalf("facts=%v", factNames(second.Facts))
	}
	if len(p.Snapshot().GameplayMetrics) != 1 {
		t.Fatal("duplicate must not append again")
	}
	if p.ProjectionVersion() != 1 {
		t.Fatalf("version=%d", p.ProjectionVersion())
	}
	if len(second.Facts) != len(first.Facts) {
		t.Fatal("fact count changed on duplicate")
	}
}

func TestApply_DefensiveCopies(t *testing.T) {
	t.Parallel()
	p := NewPublicAnalyticsProjection()
	payload := map[string]any{
		"visibility": "anonymized_adhoc",
		"metricType": "card_played",
		"roomId":     "room_1",
	}
	evt := gameplayEvent("copy_1", payload)
	out := mustApply(t, p, evt)

	payload["roomId"] = "tampered_room"
	payload["hand"] = []any{"red-1"}
	if p.Snapshot().GameplayMetrics[0].RoomID != "room_1" {
		t.Fatal("mutating input payload affected stored metric")
	}

	out.Facts[0].Data["roomId"] = "tampered"
	dup := p.Apply(evt)
	if dup.Facts[0].Data["roomId"] == "tampered" {
		t.Fatal("duplicate facts not defensively copied")
	}

	snap := p.Snapshot()
	snap.GameplayMetrics[0].RoomID = "mutated"
	snap.TournamentStats = append(snap.TournamentStats, TournamentStatistic{TournamentID: "x"})
	if p.Snapshot().GameplayMetrics[0].RoomID != "room_1" {
		t.Fatal("mutating snapshot affected projection")
	}
	if len(p.Snapshot().TournamentStats) != 0 {
		t.Fatal("mutating snapshot slice affected projection")
	}
}

func TestApply_PublicTournamentAndRatingMetrics(t *testing.T) {
	t.Parallel()
	p := NewPublicAnalyticsProjection()

	mustApply(t, p, tournamentEvent("tour_stat_1", map[string]any{
		"tournamentId":         "tour_9",
		"roundNumber":          2,
		"slotId":               "slot_a",
		"eventType":            "RoundAdvanced",
		"phase":                "quarterfinal",
		"registeredCount":      64,
		"advancingPlayerCount": 3,
		"publicPayload": map[string]any{
			"bracketLabel": "QF-1",
		},
	}))

	mustApply(t, p, ratingEvent("rating_1", map[string]any{
		"playerId":       "p1",
		"sourceType":     "casual_elo",
		"previousRating": 1000,
		"newRating":      1016,
		"boardType":      "casual_elo",
	}))

	mustApply(t, p, UpstreamEvent{
		EventID:       "snap_1",
		EventType:     EventLeaderboardSnapshot,
		Source:        SourceRankingLeaderboardSnapshot,
		SchemaVersion: CurrentSchemaVersion,
		Payload: map[string]any{
			"boardType":  "casual_elo",
			"snapshotId": "lb_1",
			"sourceType": "casual_elo",
			"entries": []any{
				map[string]any{"playerId": "p1", "rating": 1016},
				map[string]any{"playerId": "p2", "rating": 990},
			},
		},
	})

	snap := p.Snapshot()
	if snap.Authoritative || p.Authoritative() {
		t.Fatal("must remain non-authoritative")
	}
	if len(snap.TournamentStats) != 1 {
		t.Fatalf("tournaments=%d", len(snap.TournamentStats))
	}
	ts := snap.TournamentStats[0]
	if ts.TournamentID != "tour_9" || ts.Phase != "quarterfinal" || ts.RegisteredCount != 64 {
		t.Fatalf("tournament=%+v", ts)
	}
	if ts.PublicPayload["bracketLabel"] != "QF-1" {
		t.Fatalf("payload=%v", ts.PublicPayload)
	}
	ts.PublicPayload["bracketLabel"] = "tampered"
	if p.Snapshot().TournamentStats[0].PublicPayload["bracketLabel"] != "QF-1" {
		t.Fatal("public payload map not defensively copied")
	}

	if len(snap.RatingStats) != 3 {
		t.Fatalf("ratings=%d want 3 (1 update + 2 snapshot rows)", len(snap.RatingStats))
	}
	if snap.RatingStats[0].PlayerID != "p1" || snap.RatingStats[0].NewRating != 1016 {
		t.Fatalf("rating0=%+v", snap.RatingStats[0])
	}
	if snap.RatingStats[1].SnapshotID != "lb_1" || snap.RatingStats[2].PlayerID != "p2" {
		t.Fatalf("snapshot rows=%+v", snap.RatingStats[1:])
	}
}

func TestApply_ApplicationFacingAliases(t *testing.T) {
	t.Parallel()
	p := NewPublicAnalyticsProjection()
	out := p.ProjectGameplayMetric(UpstreamEvent{
		EventID:       "alias_g",
		Source:        SourceRoomGameplayMetrics,
		SchemaVersion: CurrentSchemaVersion,
		Payload: map[string]any{
			"visibility": "public",
			"metricType": "match_completed",
			"roomId":     "room_2",
			"gameId":     "game_2",
		},
	})
	if out.Kind != OutcomeAccepted || !hasFact(out.Facts, FactPublicGameplayMetricProjected) {
		t.Fatalf("out=%+v", out)
	}
	out = p.ProjectTournamentStatistic(UpstreamEvent{
		EventID:       "alias_t",
		Source:        SourceTournamentRoundCompleted,
		SchemaVersion: CurrentSchemaVersion,
		Payload:       map[string]any{"tournamentId": "t1", "phase": "registration", "registeredCount": 8},
	})
	if out.Kind != OutcomeAccepted || !hasFact(out.Facts, FactPublicTournamentStatisticProjected) {
		t.Fatalf("out=%+v", out)
	}
	out = p.ProjectRatingStatistic(UpstreamEvent{
		EventID:       "alias_r",
		Source:        SourceRankingPlayerRatingUpdated,
		SchemaVersion: CurrentSchemaVersion,
		Payload: map[string]any{
			"playerId": "p9", "sourceType": "tournament_placement",
			"previousRating": 0, "newRating": 50,
		},
	})
	if out.Kind != OutcomeAccepted || !hasFact(out.Facts, FactPublicRatingStatisticProjected) {
		t.Fatalf("out=%+v", out)
	}
}

func TestRebuildFrom_ReplaysDeterministically(t *testing.T) {
	t.Parallel()
	events := []UpstreamEvent{
		gameplayEvent("r1", map[string]any{
			"visibility": "anonymized_adhoc", "metricType": "a", "roomId": "r",
		}),
		tournamentEvent("r2", map[string]any{
			"tournamentId": "t1", "phase": "final",
		}),
	}
	p1, outs1 := RebuildFrom(events)
	p2, outs2 := RebuildFrom(events)
	if len(outs1) != 2 || len(outs2) != 2 {
		t.Fatalf("outs=%d/%d", len(outs1), len(outs2))
	}
	if p1.ProjectionVersion() != p2.ProjectionVersion() {
		t.Fatal("versions diverge")
	}
	s1, err := p1.SnapshotJSON()
	if err != nil {
		t.Fatal(err)
	}
	s2, err := p2.SnapshotJSON()
	if err != nil {
		t.Fatal(err)
	}
	if string(s1) != string(s2) {
		t.Fatalf("rebuild not deterministic:\n%s\n%s", s1, s2)
	}
}
