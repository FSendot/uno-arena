package audit

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestNewRejectionValidateKnownPrincipal(t *testing.T) {
	at := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
	submitted := int64(10)
	current := int64(12)
	r := NewRejection("cmd_1", "corr_1", "sess_1", "p1", "stale_sequence", at).
		WithRoom("room_1").
		WithTournament("t_1").
		WithSequences(&submitted, &current)
	if err := r.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if r.SessionID != "sess_1" || r.PlayerID != "p1" {
		t.Fatalf("identity: %+v", r)
	}
	if r.RoomID != "room_1" || r.TournamentID != "t_1" {
		t.Fatalf("scope: %+v", r)
	}
	if r.SubmittedSequence == nil || *r.SubmittedSequence != 10 {
		t.Fatalf("submitted=%v", r.SubmittedSequence)
	}
	if r.CurrentSequence == nil || *r.CurrentSequence != 12 {
		t.Fatalf("current=%v", r.CurrentSequence)
	}
}

func TestNewRejectionValidateUnknownPrincipal(t *testing.T) {
	at := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
	r := NewRejection("cmd_preauth", "corr_preauth", "", "", "unauthenticated", at)
	if err := r.Validate(); err != nil {
		t.Fatalf("Validate with absent sessionId/playerId: %v", err)
	}
	if r.SessionID != "" || r.PlayerID != "" {
		t.Fatalf("must not fabricate identity: %+v", r)
	}
	if r.RoomID != "" || r.TournamentID != "" {
		t.Fatalf("must not fabricate room/tournament: %+v", r)
	}
}

func TestNewRejectionDefaultsTimestamp(t *testing.T) {
	before := time.Now().UTC().Add(-time.Second)
	r := NewRejection("cmd", "corr", "sess", "player", "duplicate", time.Time{})
	if err := r.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if r.Timestamp.Before(before) {
		t.Fatalf("timestamp not set: %v", r.Timestamp)
	}
	if r.Timestamp.Location() != time.UTC {
		t.Fatalf("timestamp location=%v", r.Timestamp.Location())
	}
}

func TestValidateRequiresCoreFields(t *testing.T) {
	at := time.Now().UTC()
	cases := []struct {
		name string
		r    RejectionRecord
		want string
	}{
		{"empty", RejectionRecord{}, "commandId"},
		{"missing correlation", RejectionRecord{CommandID: "c", Reason: "r", Timestamp: at}, "correlationId"},
		{"missing reason", RejectionRecord{CommandID: "c", CorrelationID: "corr", Timestamp: at}, "reason"},
		{"missing timestamp", RejectionRecord{CommandID: "c", CorrelationID: "corr", Reason: "r"}, "timestamp"},
		{"blank commandId", RejectionRecord{CommandID: " ", CorrelationID: "corr", Reason: "r", Timestamp: at}, "commandId"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.r.Validate()
			if err == nil {
				t.Fatalf("expected error containing %q", tc.want)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %q does not contain %q", err, tc.want)
			}
		})
	}
}

func TestRejectionRecordJSONShapeKnownPrincipal(t *testing.T) {
	at := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
	r := NewRejection("cmd_1", "corr_1", "sess_1", "p1", "spoofing", at)
	raw, err := json.Marshal(r)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	s := string(raw)
	for _, key := range []string{
		`"commandId"`, `"correlationId"`, `"sessionId"`, `"playerId"`, `"reason"`, `"timestamp"`,
	} {
		if !strings.Contains(s, key) {
			t.Fatalf("missing %s in %s", key, s)
		}
	}
	for _, key := range []string{`"roomId"`, `"tournamentId"`, `"submittedSequenceNumber"`, `"currentSequenceNumber"`} {
		if strings.Contains(s, key) {
			t.Fatalf("optional %s should be omitted: %s", key, s)
		}
	}
}

func TestRejectionRecordJSONShapeUnknownPrincipal(t *testing.T) {
	at := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
	r := NewRejection("cmd_1", "corr_1", "", "", "pre_auth_reject", at)
	raw, err := json.Marshal(r)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	s := string(raw)
	for _, key := range []string{`"commandId"`, `"correlationId"`, `"reason"`, `"timestamp"`} {
		if !strings.Contains(s, key) {
			t.Fatalf("missing %s in %s", key, s)
		}
	}
	for _, key := range []string{`"sessionId"`, `"playerId"`, `"roomId"`, `"tournamentId"`} {
		if strings.Contains(s, key) {
			t.Fatalf("unknown principal must omit %s: %s", key, s)
		}
	}
}

func TestRejectionIsNotDomainEventContract(t *testing.T) {
	// Guardrail: audit records must not look like domain event metadata.
	r := NewRejection("cmd", "corr", "sess", "player", "rate_limited", time.Now().UTC())
	raw, err := json.Marshal(r)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	s := string(raw)
	for _, forbidden := range []string{`"eventId"`, `"eventType"`, `"schemaVersion"`, `"occurredAt"`} {
		if strings.Contains(s, forbidden) {
			t.Fatalf("audit record must not carry domain-event field %s: %s", forbidden, s)
		}
	}
}
