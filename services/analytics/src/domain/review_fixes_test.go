package domain

import (
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Finding 1: rating_statistics ReplacingMergeTree key must retain every leaderboard row.
func TestMigration_RatingStatisticsOrderByRetainsLeaderboardRows(t *testing.T) {
	t.Parallel()
	raw, err := os.ReadFile(filepath.Join("..", "..", "migrations", "001_init.sql"))
	if err != nil {
		t.Fatal(err)
	}
	sql := string(raw)
	idx := strings.Index(sql, "analytics.rating_statistics")
	if idx < 0 {
		t.Fatal("rating_statistics table missing")
	}
	section := sql[idx:]
	if end := strings.Index(section, "CREATE TABLE"); end > 0 {
		// keep through next CREATE or end; first CREATE is this table's engine block
		next := strings.Index(section[1:], "CREATE TABLE")
		if next > 0 {
			section = section[:next+1]
		}
	}
	if !strings.Contains(section, "ORDER BY (generation_id, event_id, snapshot_id, player_id)") {
		t.Fatalf("rating_statistics must ORDER BY (generation_id, event_id, snapshot_id, player_id); section=\n%s", section)
	}
	// Bare event_id-only key would collapse leaderboard rows sharing one upstream event.
	if strings.Contains(section, "ORDER BY (event_id)\n") || strings.Contains(section, "ORDER BY (event_id)\r") {
		t.Fatal("rating_statistics must not use ORDER BY (event_id) alone")
	}
	if !strings.Contains(sql, "analytics.processed_events") || !strings.Contains(sql, "ORDER BY (generation_id, event_id)") {
		t.Fatal("processed_events must still dedupe ingestion by (generation_id, event_id)")
	}
}

// Finding 6: gameplay_metrics must persist intentional public tournament player fields.
func TestMigration_GameplayMetricsRoundTripsPublicPlayerFields(t *testing.T) {
	t.Parallel()
	raw, err := os.ReadFile(filepath.Join("..", "..", "migrations", "001_init.sql"))
	if err != nil {
		t.Fatal(err)
	}
	sql := string(raw)
	idx := strings.Index(sql, "analytics.gameplay_metrics")
	if idx < 0 {
		t.Fatal("gameplay_metrics table missing")
	}
	section := sql[idx:]
	if next := strings.Index(section[1:], "CREATE TABLE"); next > 0 {
		section = section[:next+1]
	}
	for _, col := range []string{"public_player_id", "display_name"} {
		if !strings.Contains(section, col) {
			t.Fatalf("gameplay_metrics missing %s for public tournament player round-trip", col)
		}
	}
}

// Finding 2: trusted Source/Topic required; payload visibility is not proof.
func TestSource_RequiredTrustedTopicBoundToEventType(t *testing.T) {
	t.Parallel()
	p := NewPublicAnalyticsProjection()

	missing := p.Apply(UpstreamEvent{
		EventID:       "src_missing",
		EventType:     EventGameplayMetric,
		SchemaVersion: CurrentSchemaVersion,
		Payload: map[string]any{
			"visibility":   "public_tournament",
			"metricType":   "card_played",
			"roomId":       "room_1",
			"tournamentId": "tour_1",
			"playerId":     "p1",
		},
	})
	if missing.Kind != OutcomeQuarantined || missing.Rejection == nil || missing.Rejection.Code != RejectNonPublicSource {
		t.Fatalf("missing source: %+v", missing)
	}

	mismatched := p.Apply(UpstreamEvent{
		EventID:       "src_mismatch",
		EventType:     EventGameplayMetric,
		Source:        SourceRankingPlayerRatingUpdated,
		SchemaVersion: CurrentSchemaVersion,
		Payload: map[string]any{
			"visibility": "public_tournament",
			"metricType": "card_played",
			"roomId":     "room_1",
		},
	})
	if mismatched.Kind != OutcomeQuarantined || mismatched.Rejection == nil || mismatched.Rejection.Code != RejectNonPublicSource {
		t.Fatalf("mismatched source: %+v", mismatched)
	}

	unknown := p.Apply(UpstreamEvent{
		EventID:       "src_unknown",
		EventType:     EventGameplayMetric,
		Source:        SourceTopic("room.nonexistent.events"),
		SchemaVersion: CurrentSchemaVersion,
		Payload: map[string]any{
			"visibility": "anonymized_adhoc",
			"metricType": "x",
			"roomId":     "room_1",
		},
	})
	if unknown.Kind != OutcomeQuarantined || unknown.Rejection == nil || unknown.Rejection.Code != RejectNonPublicSource {
		t.Fatalf("unknown source: %+v", unknown)
	}
}

func TestSource_PublicPlayerFactsRequireTournamentProvenance(t *testing.T) {
	t.Parallel()
	p := NewPublicAnalyticsProjection()

	// Visibility claim alone is insufficient without tournamentId.
	out := p.Apply(gameplayEvent("prov_1", map[string]any{
		"visibility":  "public_tournament",
		"metricType":  "card_played",
		"roomId":      "room_1",
		"playerId":    "p1",
		"displayName": "Bob",
	}))
	if out.Kind != OutcomeQuarantined || out.Rejection == nil || out.Rejection.Code != RejectNonPublicSource {
		t.Fatalf("expected RejectNonPublicSource without tournament provenance, got %+v", out)
	}
	if p.ProjectionVersion() != 0 {
		t.Fatal("must not mutate")
	}

	// Trusted source + tournament provenance keeps public player facts.
	mustApply(t, p, gameplayEvent("prov_2", map[string]any{
		"visibility":   "public_tournament",
		"metricType":   "card_played",
		"roomId":       "room_1",
		"tournamentId": "tour_1",
		"playerId":     "p1",
		"displayName":  "Bob",
	}))
	m := p.Snapshot().GameplayMetrics[0]
	if m.PublicPlayerID != "p1" || m.DisplayName != "Bob" || m.TournamentID != "tour_1" {
		t.Fatalf("metric=%+v", m)
	}
}

// Finding 3: validate all leaderboard entries into temp state; append only if entire event valid.
func TestLeaderboard_AtomicAppendOrQuarantine(t *testing.T) {
	t.Parallel()
	p := NewPublicAnalyticsProjection()
	out := p.Apply(UpstreamEvent{
		EventID:       "lb_atomic_1",
		EventType:     EventLeaderboardSnapshot,
		Source:        SourceRankingLeaderboardSnapshot,
		SchemaVersion: CurrentSchemaVersion,
		Payload: map[string]any{
			"boardType":  "casual_elo",
			"snapshotId": "lb_1",
			"sourceType": "casual_elo",
			"entries": []any{
				map[string]any{"playerId": "p1", "rating": 1016},
				map[string]any{"rating": 990}, // missing playerId
				map[string]any{"playerId": "p3", "rating": 980},
			},
		},
	})
	if out.Kind != OutcomeQuarantined {
		t.Fatalf("kind=%s want quarantined", out.Kind)
	}
	if len(p.Snapshot().RatingStats) != 0 {
		t.Fatalf("partial append leaked %d rows", len(p.Snapshot().RatingStats))
	}
	if p.ProjectionVersion() != 0 {
		t.Fatal("version must not advance on quarantine")
	}

	mustApply(t, p, UpstreamEvent{
		EventID:       "lb_atomic_2",
		EventType:     EventLeaderboardSnapshot,
		Source:        SourceRankingLeaderboardSnapshot,
		SchemaVersion: CurrentSchemaVersion,
		Payload: map[string]any{
			"boardType":  "casual_elo",
			"snapshotId": "lb_2",
			"sourceType": "casual_elo",
			"entries": []any{
				map[string]any{"playerId": "p1", "rating": 1016},
				map[string]any{"playerId": "p2", "rating": 990},
			},
		},
	})
	if len(p.Snapshot().RatingStats) != 2 {
		t.Fatalf("ratings=%d", len(p.Snapshot().RatingStats))
	}
}

// Finding 4: nested publicPayload allowlist; unknown keys including playerEmail quarantine.
func TestPublicPayload_UnknownNestedKeysQuarantined(t *testing.T) {
	t.Parallel()
	p := NewPublicAnalyticsProjection()
	out := p.Apply(tournamentEvent("pp_email", map[string]any{
		"tournamentId": "tour_1",
		"phase":        "final",
		"publicPayload": map[string]any{
			"bracketLabel": "F-1",
			"playerEmail":  "alice@example.com",
		},
	}))
	if out.Kind != OutcomeQuarantined || out.Rejection == nil {
		t.Fatalf("playerEmail must quarantine, got %+v", out)
	}
	switch out.Rejection.Code {
	case RejectDisallowedField, RejectForbiddenField:
	default:
		t.Fatalf("playerEmail must quarantine as disallowed/forbidden, got %+v", out.Rejection)
	}
	if p.ProjectionVersion() != 0 {
		t.Fatal("must not mutate")
	}
}

func TestPublicPayload_MalformedJSONQuarantined(t *testing.T) {
	t.Parallel()
	p := NewPublicAnalyticsProjection()
	out := p.Apply(tournamentEvent("pp_bad_json", map[string]any{
		"tournamentId":      "tour_1",
		"phase":             "final",
		"publicPayloadJson": "{not-json",
	}))
	if out.Kind != OutcomeQuarantined || out.Rejection == nil || out.Rejection.Code != RejectInvalidSchema {
		t.Fatalf("malformed publicPayloadJson must quarantine, got %+v", out)
	}
}

func TestPublicPayload_NonStringValueQuarantined(t *testing.T) {
	t.Parallel()
	p := NewPublicAnalyticsProjection()
	out := p.Apply(tournamentEvent("pp_typed", map[string]any{
		"tournamentId": "tour_1",
		"phase":        "final",
		"publicPayload": map[string]any{
			"bracketLabel": 42,
		},
	}))
	if out.Kind != OutcomeQuarantined || out.Rejection == nil || out.Rejection.Code != RejectInvalidSchema {
		t.Fatalf("non-string nested value must quarantine, got %+v", out)
	}
}

// Finding 5: malformed types/ranges/schema quarantine; never coerce/drop/wrap silently.
func TestMalformed_NumericTypesQuarantined(t *testing.T) {
	t.Parallel()
	p := NewPublicAnalyticsProjection()
	cases := []struct {
		name     string
		evt      UpstreamEvent
		wantCode RejectionCode
	}{
		{
			name: "rating as string",
			evt: ratingEvent("mal_1", map[string]any{
				"playerId": "p1", "sourceType": "casual_elo",
				"previousRating": "1000", "newRating": 1016,
			}),
			wantCode: RejectInvalidSchema,
		},
		{
			name: "registeredCount as bool",
			evt: tournamentEvent("mal_2", map[string]any{
				"tournamentId": "t1", "registeredCount": true,
			}),
			wantCode: RejectInvalidSchema,
		},
		{
			name: "negative publicCardCountTotal",
			evt: gameplayEvent("mal_3", map[string]any{
				"visibility": "anonymized_adhoc", "metricType": "x", "roomId": "r1",
				"publicCardCountTotal": -1,
			}),
			wantCode: RejectInvalidSchema,
		},
		{
			name: "roomId wrong type",
			evt: gameplayEvent("mal_4", map[string]any{
				"visibility": "anonymized_adhoc", "metricType": "x", "roomId": 123,
			}),
			wantCode: RejectInvalidSchema,
		},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			before := p.ProjectionVersion()
			out := p.Apply(tt.evt)
			if out.Kind != OutcomeQuarantined || out.Rejection == nil || out.Rejection.Code != tt.wantCode {
				t.Fatalf("out=%+v want %s", out, tt.wantCode)
			}
			if p.ProjectionVersion() != before {
				t.Fatal("must not mutate")
			}
		})
	}
}

func TestLeaderboard_MalformedEntryTypeQuarantinesWithoutPartialAppend(t *testing.T) {
	t.Parallel()
	p := NewPublicAnalyticsProjection()
	out := p.Apply(UpstreamEvent{
		EventID:       "lb_mal_entry",
		EventType:     EventLeaderboardSnapshot,
		Source:        SourceRankingLeaderboardSnapshot,
		SchemaVersion: CurrentSchemaVersion,
		Payload: map[string]any{
			"boardType":  "casual_elo",
			"snapshotId": "lb_x",
			"entries": []any{
				map[string]any{"playerId": "p1", "rating": 10},
				"not-an-object",
			},
		},
	})
	if out.Kind != OutcomeQuarantined {
		t.Fatalf("kind=%s", out.Kind)
	}
	if len(p.Snapshot().RatingStats) != 0 {
		t.Fatal("partial append")
	}
}

// Native int must be range-checked before narrowing; wrap would silently accept wrong values.
func TestMalformed_NativeIntOutOfDestinationBoundsQuarantined(t *testing.T) {
	t.Parallel()

	// Architecture-portable: only construct ints that round-trip through int64→int.
	aboveInt32 := int64(math.MaxInt32) + 1
	belowInt32 := int64(math.MinInt32) - 1
	aboveUint32 := int64(math.MaxUint32) + 1
	aboveUint16 := int(math.MaxUint16) + 1 // fits every Go int width

	cases := []struct {
		name string
		skip bool
		evt  UpstreamEvent
	}{
		{
			name: "int above int32 max",
			skip: int64(int(aboveInt32)) != aboveInt32,
			evt: ratingEvent("mal_int32_hi", map[string]any{
				"playerId": "p1", "sourceType": "casual_elo",
				"previousRating": int(aboveInt32), "newRating": 1016,
			}),
		},
		{
			name: "int below int32 min",
			skip: int64(int(belowInt32)) != belowInt32,
			evt: ratingEvent("mal_int32_lo", map[string]any{
				"playerId": "p1", "sourceType": "casual_elo",
				"previousRating": int(belowInt32), "newRating": 1016,
			}),
		},
		{
			name: "int above uint32 max",
			skip: int64(int(aboveUint32)) != aboveUint32,
			evt: tournamentEvent("mal_uint32_hi", map[string]any{
				"tournamentId": "t1", "registeredCount": int(aboveUint32),
			}),
		},
		{
			name: "uint16 overflow",
			skip: false,
			evt: gameplayEvent("mal_uint16_hi", map[string]any{
				"visibility": "anonymized_adhoc", "metricType": "x", "roomId": "r1",
				"publicCardCountTotal": aboveUint16,
			}),
		},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if tt.skip {
				t.Skip("native int cannot represent out-of-range value on this architecture")
			}
			p := NewPublicAnalyticsProjection()
			out := p.Apply(tt.evt)
			if out.Kind != OutcomeQuarantined || out.Rejection == nil || out.Rejection.Code != RejectInvalidSchema {
				t.Fatalf("out=%+v want quarantined RejectInvalidSchema", out)
			}
			if p.ProjectionVersion() != 0 {
				t.Fatal("must not mutate")
			}
		})
	}
}
