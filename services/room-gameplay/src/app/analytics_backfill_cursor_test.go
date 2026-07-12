package app_test

import (
	"os"
	"strings"
	"testing"

	"unoarena/services/room-gameplay/app"
)

func TestAnalyticsBackfillCursor_HMACTamperAndRoundTrip(t *testing.T) {
	restore := app.SetAnalyticsBackfillCursorMACKeyForTest("unit-cursor-secret")
	t.Cleanup(restore)

	cur := app.AnalyticsBackfillCursor{
		OutboxID: 42, SourceTopic: "room.gameplay.metrics", RecoveryJobID: "job-1",
		FromCheckpoint: "1", ToCheckpoint: "100",
	}
	enc, err := app.EncodeAnalyticsBackfillCursor(cur)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(enc, "ab1.") {
		t.Fatalf("prefix: %s", enc)
	}
	got, err := app.DecodeAnalyticsBackfillCursor(enc)
	if err != nil {
		t.Fatal(err)
	}
	if got != cur {
		t.Fatalf("roundtrip %+v vs %+v", got, cur)
	}

	tampered := enc[:len(enc)-1] + "x"
	if _, err := app.DecodeAnalyticsBackfillCursor(tampered); err == nil {
		t.Fatal("tampered must fail")
	}
	if _, err := app.DecodeAnalyticsBackfillCursor("OFFSET 10"); err == nil {
		t.Fatal("offset leakage must fail")
	}
	if _, err := app.DecodeAnalyticsBackfillCursor("ab1.notbase64.notmac"); err == nil {
		t.Fatal("garbage must fail")
	}
}

func TestAnalyticsBackfillCursor_ProdRequiresSecret(t *testing.T) {
	restore := app.SetAnalyticsBackfillCursorMACKeyForTest("")
	t.Cleanup(restore)
	t.Setenv("ROOM_ANALYTICS_BACKFILL_CURSOR_SECRET", "")
	t.Setenv("DEPLOYMENT_ENV", "production")
	_, err := app.EncodeAnalyticsBackfillCursor(app.AnalyticsBackfillCursor{
		OutboxID: 1, SourceTopic: "room.gameplay.metrics", RecoveryJobID: "j",
	})
	if err == nil || !strings.Contains(err.Error(), "ROOM_ANALYTICS_BACKFILL_CURSOR_SECRET") {
		t.Fatalf("want secret required, got %v", err)
	}
}

func TestAnalyticsBackfillSource_ReadOnlyNoMutationSQL(t *testing.T) {
	b, err := os.ReadFile("../store/analytics_backfill.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(b)
	upper := strings.ToUpper(src)
	for _, bad := range []string{
		"UPDATE INTEGRATION_OUTBOX_EVENTS",
		"DELETE FROM INTEGRATION_OUTBOX_EVENTS",
	} {
		if strings.Contains(upper, bad) {
			t.Fatalf("mutation SQL found: %s", bad)
		}
	}
	if strings.Contains(src, "OFFSET") {
		t.Fatal("OFFSET pagination forbidden")
	}
}
