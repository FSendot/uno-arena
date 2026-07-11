package events

import (
	"strings"
	"testing"
	"time"
)

func TestNewMetadataAndValidate(t *testing.T) {
	at := time.Date(2026, 5, 17, 12, 0, 1, 0, time.UTC)
	m := NewMetadata("evt_1", "MatchCompleted", "corr_1", "cmd_1", at)
	if err := m.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if m.SchemaVersion != 1 {
		t.Fatalf("schemaVersion=%d", m.SchemaVersion)
	}
	if !m.OccurredAt.Equal(at) {
		t.Fatalf("occurredAt=%v", m.OccurredAt)
	}
	if m.CausationID != "cmd_1" {
		t.Fatalf("causationId=%q", m.CausationID)
	}
}

func TestNewMetadataDefaultsOccurredAt(t *testing.T) {
	before := time.Now().UTC().Add(-time.Second)
	m := NewMetadata("evt_2", "GameCompleted", "corr_2", "", time.Time{})
	if err := m.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if m.OccurredAt.Before(before) {
		t.Fatalf("occurredAt not set: %v", m.OccurredAt)
	}
	if m.OccurredAt.Location() != time.UTC {
		t.Fatalf("occurredAt location=%v", m.OccurredAt.Location())
	}
}

func TestNewMetadataNormalizesToUTC(t *testing.T) {
	loc := time.FixedZone("UTC-3", -3*60*60)
	at := time.Date(2026, 5, 17, 9, 0, 1, 0, loc)
	m := NewMetadata("evt_3", "X", "corr", "cause", at)
	if m.OccurredAt.Location() != time.UTC {
		t.Fatalf("want UTC, got %v", m.OccurredAt.Location())
	}
	if !m.OccurredAt.Equal(at.UTC()) {
		t.Fatalf("got %v want %v", m.OccurredAt, at.UTC())
	}
}

func TestValidateRejectsIncomplete(t *testing.T) {
	at := time.Date(2026, 5, 17, 12, 0, 1, 0, time.UTC)
	cases := []struct {
		name string
		m    Metadata
		want string
	}{
		{"missing eventId", Metadata{EventType: "X", SchemaVersion: 1, CorrelationID: "c", OccurredAt: at}, "eventId"},
		{"missing eventType", Metadata{EventID: "e", SchemaVersion: 1, CorrelationID: "c", OccurredAt: at}, "eventType"},
		{"bad schema", Metadata{EventID: "e", EventType: "X", SchemaVersion: 0, CorrelationID: "c", OccurredAt: at}, "schemaVersion"},
		{"missing correlation", Metadata{EventID: "e", EventType: "X", SchemaVersion: 1, OccurredAt: at}, "correlationId"},
		{"missing occurredAt", Metadata{EventID: "e", EventType: "X", SchemaVersion: 1, CorrelationID: "c"}, "occurredAt"},
		{"blank causation", Metadata{EventID: "e", EventType: "X", SchemaVersion: 1, CorrelationID: "c", CausationID: "   ", OccurredAt: at}, "causationId"},
		{"blank eventId", Metadata{EventID: " ", EventType: "X", SchemaVersion: 1, CorrelationID: "c", OccurredAt: at}, "eventId"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.m.Validate()
			if err == nil {
				t.Fatalf("expected error containing %q", tc.want)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %q does not contain %q", err, tc.want)
			}
		})
	}
}

func TestValidateAllowsEmptyCausation(t *testing.T) {
	m := Metadata{
		EventID:       "evt",
		EventType:     "X",
		SchemaVersion: 1,
		CorrelationID: "corr",
		OccurredAt:    time.Now().UTC(),
	}
	if err := m.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
}
