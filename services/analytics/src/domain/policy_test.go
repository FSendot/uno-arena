package domain

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestPolicy_ForbiddenNestedFieldsQuarantined(t *testing.T) {
	t.Parallel()
	p := NewPublicAnalyticsProjection()
	attacks := []struct {
		name    string
		payload map[string]any
	}{
		{"hand", map[string]any{
			"visibility": "anonymized_adhoc", "metricType": "x", "hand": []any{"red-1"},
		}},
		{"nested sessionToken", map[string]any{
			"visibility": "public", "metricType": "x",
			"publicPayload": map[string]any{"sessionToken": "tok"},
		}},
		{"deckOrder", map[string]any{
			"visibility": "public_tournament", "metricType": "x", "deckOrder": []any{"a", "b"},
		}},
		{"drawnCards", map[string]any{
			"visibility": "anonymized_adhoc", "metricType": "x", "drawnCards": []any{"green-4"},
		}},
		{"audit", map[string]any{
			"visibility": "public", "metricType": "x", "audit": map[string]any{"id": "a1"},
		}},
	}
	before := p.ProjectionVersion()
	for _, tt := range attacks {
		t.Run(tt.name, func(t *testing.T) {
			out := p.Apply(gameplayEvent(EventID("atk_"+tt.name), tt.payload))
			if out.Kind != OutcomeQuarantined {
				t.Fatalf("kind=%s want quarantined", out.Kind)
			}
			if out.Rejection == nil || out.Rejection.Code != RejectForbiddenField {
				t.Fatalf("rejection=%+v", out.Rejection)
			}
			if !hasFact(out.Facts, FactProjectionEventQuarantined) {
				t.Fatalf("facts=%v", factNames(out.Facts))
			}
			if p.ProjectionVersion() != before {
				t.Fatal("quarantine must not mutate projection")
			}
		})
	}
}

func TestPolicy_DisallowedFieldQuarantined(t *testing.T) {
	t.Parallel()
	p := NewPublicAnalyticsProjection()
	out := p.Apply(gameplayEvent("dis_1", map[string]any{
		"visibility":        "anonymized_adhoc",
		"metricType":        "card_played",
		"internalDebugFlag": true,
	}))
	if out.Kind != OutcomeQuarantined {
		t.Fatalf("kind=%s", out.Kind)
	}
	if out.Rejection == nil || out.Rejection.Code != RejectDisallowedField {
		t.Fatalf("rejection=%+v", out.Rejection)
	}
}

func TestPolicy_UnknownEventTypeQuarantined(t *testing.T) {
	t.Parallel()
	p := NewPublicAnalyticsProjection()
	out := p.Apply(UpstreamEvent{
		EventID:       "unk_1",
		EventType:     EventType("PrivateHandRevealed"),
		Source:        SourceRoomGameplayMetrics,
		SchemaVersion: CurrentSchemaVersion,
		Payload:       map[string]any{"metricType": "x"},
	})
	if out.Kind != OutcomeQuarantined {
		t.Fatalf("kind=%s", out.Kind)
	}
	// Unknown type may fail as unknown_event_type or non_public_source (not bound to topic).
	if out.Rejection == nil {
		t.Fatal("expected rejection")
	}
	switch out.Rejection.Code {
	case RejectUnknownEventType, RejectNonPublicSource:
	default:
		t.Fatalf("rejection=%+v", out.Rejection)
	}
}

func TestSnapshotJSON_NoPrivateKeys(t *testing.T) {
	t.Parallel()
	p := NewPublicAnalyticsProjection()
	mustApply(t, p, gameplayEvent("safe_1", map[string]any{
		"visibility":   "public_tournament",
		"metricType":   "card_played",
		"roomId":       "room_1",
		"tournamentId": "tour_1",
		"publicCard":   "yellow-3",
		"playerId":     "p1",
	}))
	raw, err := p.SnapshotJSON()
	if err != nil {
		t.Fatal(err)
	}
	s := strings.ToLower(string(raw))
	forbidden := []string{
		`"hand"`, `"hands"`, `"cards"`, `"deck"`, `"session"`, `"token"`,
		`"audit"`, `"commandid"`, `"drawncards"`, `"password"`, `"secret"`,
	}
	for _, key := range forbidden {
		if strings.Contains(s, key) {
			t.Fatalf("snapshot JSON leaked %s: %s", key, s)
		}
	}
	var snap AnalyticsSnapshot
	if err := json.Unmarshal(raw, &snap); err != nil {
		t.Fatal(err)
	}
	if snap.Authoritative {
		t.Fatal("JSON must encode authoritative=false")
	}
}
