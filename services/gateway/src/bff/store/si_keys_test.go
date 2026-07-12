package store

import (
	"strings"
	"testing"
	"time"
)

func TestSessionInvalidationKeySpace(t *testing.T) {
	ks := NewSessionInvalidationKeySpace("")
	if ks.Prefix() != SessionInvalidationKeyPrefix {
		t.Fatalf("prefix=%q", ks.Prefix())
	}
	if got := ks.Session("sess-1"); got != "gateway:si:v1:session:sess-1" {
		t.Fatalf("session=%q", got)
	}
	if got := ks.Event("evt-1"); got != "gateway:si:v1:event:evt-1" {
		t.Fatalf("event=%q", got)
	}
	if got := ks.KafkaQuarantine("player-1"); !strings.Contains(got, "{player-1}") {
		t.Fatalf("quarantine=%q", got)
	}
	if ks.NotifyChannel() != SessionInvalidationNotifyChannel {
		t.Fatalf("channel=%q", ks.NotifyChannel())
	}
}

func TestValidateSessionInvalidationID(t *testing.T) {
	if err := ValidateSessionInvalidationID("ok_1"); err != nil {
		t.Fatal(err)
	}
	if err := ValidateSessionInvalidationID("bad:id"); err == nil {
		t.Fatal("colon must fail")
	}
	if err := ValidateSessionInvalidationID(""); err == nil {
		t.Fatal("empty must fail")
	}
}

func TestDefaultSessionInvalidationTTL_CoversSourcePlusDLQ(t *testing.T) {
	// ADR-0032: source 24h + DLQ 30d; 31d is an acceptable production bound.
	min := 24*time.Hour + 30*24*time.Hour
	if DefaultSessionInvalidationTTL < min {
		t.Fatalf("ttl=%s want >= %s", DefaultSessionInvalidationTTL, min)
	}
}
