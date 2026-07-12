package main

import (
	"os"
	"strings"
	"testing"
)

func TestAnalyticsBackfillCursor_HMACTamperAndRoundTrip(t *testing.T) {
	restore := SetAnalyticsBackfillCursorMACKeyForTest("unit-tournament-analytics-backfill-cursor")
	t.Cleanup(restore)

	cur := AnalyticsBackfillCursor{
		OutboxID: 42, SourceTopic: TopicMatchAssigned, RecoveryJobID: "job-1",
		FromCheckpoint: "1", ToCheckpoint: "99",
	}
	enc, err := EncodeAnalyticsBackfillCursor(cur)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(enc, "tab1.") {
		t.Fatalf("prefix: %q", enc)
	}
	got, err := DecodeAnalyticsBackfillCursor(enc)
	if err != nil {
		t.Fatal(err)
	}
	if got != cur {
		t.Fatalf("got %+v want %+v", got, cur)
	}

	tampered := enc[:len(enc)-1] + "x"
	if _, err := DecodeAnalyticsBackfillCursor(tampered); err == nil {
		t.Fatal("tampered cursor must fail")
	}
	if _, err := DecodeAnalyticsBackfillCursor("OFFSET 10"); err == nil {
		t.Fatal("OFFSET leakage must fail")
	}
	if _, err := DecodeAnalyticsBackfillCursor("ab1.notbase64.notmac"); err == nil {
		t.Fatal("room prefix must not decode")
	}
	if _, err := DecodeAnalyticsBackfillCursor("tab1.notbase64.notmac"); err == nil {
		t.Fatal("malformed must fail")
	}
}

func TestAnalyticsBackfillCursor_ProdRequiresSecret(t *testing.T) {
	restore := SetAnalyticsBackfillCursorMACKeyForTest("")
	t.Cleanup(restore)
	t.Setenv("TOURNAMENT_ANALYTICS_BACKFILL_CURSOR_SECRET", "")
	t.Setenv("DEPLOYMENT_ENV", "production")
	_, err := EncodeAnalyticsBackfillCursor(AnalyticsBackfillCursor{
		OutboxID: 1, SourceTopic: TopicMatchAssigned, RecoveryJobID: "j",
	})
	if err == nil || !strings.Contains(err.Error(), "TOURNAMENT_ANALYTICS_BACKFILL_CURSOR_SECRET") {
		t.Fatalf("prod must require secret: %v", err)
	}
	_ = os.Getenv
}

func TestAnalyticsBackfillSource_ReadOnlyNoMutationSQL(t *testing.T) {
	b, err := os.ReadFile("store/analytics_backfill.go")
	if err != nil {
		t.Fatal(err)
	}
	src := strings.ToUpper(string(b))
	for _, bad := range []string{"UPDATE ", "DELETE ", "INSERT ", "TRUNCATE "} {
		if strings.Contains(src, bad) {
			t.Fatalf("analytics backfill store must stay read-only; found %q", strings.TrimSpace(bad))
		}
	}
	if !strings.Contains(string(b), "outbox_events") {
		t.Fatal("must read Tournament-owned outbox_events")
	}
	if strings.Contains(string(b), "OFFSET") || strings.Contains(string(b), "offset") {
		t.Fatal("must not use SQL OFFSET")
	}
	if strings.Contains(string(b), "integration_outbox_events") {
		t.Fatal("must not reference Room outbox table")
	}
}
