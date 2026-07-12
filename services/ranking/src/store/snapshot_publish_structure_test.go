package store_test

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestSnapshotPublish_UsesAuthoritativePostgresClock(t *testing.T) {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("caller")
	}
	dir := filepath.Dir(file)

	publishSrc := readGo(t, filepath.Join(dir, "snapshot_publish.go"))
	pubStateSrc := readGo(t, filepath.Join(dir, "publication_state.go"))

	for _, needle := range []string{
		"last_published_at IS NULL OR last_published_at <= clock_timestamp()",
		"make_interval(secs =>",
		"generated_at, player_count",
		"SELECT clock_timestamp()",
		"last_published_at = $",
	} {
		if !strings.Contains(publishSrc, needle) {
			t.Fatalf("snapshot_publish.go missing DB-clock SQL fragment %q", needle)
		}
	}

	// Eligibility and publication stamps must use wall-clock, not transaction-stable now().
	if strings.Contains(publishSrc, "<= now()") {
		t.Fatal("cooldown eligibility must use clock_timestamp(), not transaction-stable now()")
	}
	if strings.Contains(publishSrc, "SELECT now()") {
		t.Fatal("publication timestamp must use SELECT clock_timestamp(), not SELECT now()")
	}
	if strings.Contains(publishSrc, "1, now(),") {
		t.Fatal("leaderboard_snapshots.generated_at must bind the late clock_timestamp() value, not now()")
	}
	if strings.Contains(publishSrc, "last_published_at = now()") {
		t.Fatal("last_published_at checkpoint must bind the late clock_timestamp() value, not now()")
	}

	// Production publish path must not take/use a process-supplied wall clock.
	if strings.Contains(publishSrc, "now time.Time") {
		t.Fatal("PublishNextDirtyLeaderboardSnapshot must not accept process wall-clock now")
	}
	if strings.Contains(publishSrc, "time.Now()") {
		t.Fatal("snapshot_publish.go must not call time.Now() on the production path")
	}
	if strings.Contains(publishSrc, "cutoff :=") {
		t.Fatal("cooldown eligibility must use Postgres clock_timestamp(), not a Go-computed cutoff")
	}

	if !strings.Contains(pubStateSrc, "last_dirty_at = now()") {
		t.Fatal("markBoardDirty must stamp last_dirty_at with Postgres now()")
	}
	if strings.Contains(pubStateSrc, "last_dirty_at = $2") {
		t.Fatal("markBoardDirty must not persist process-supplied wall clocks")
	}
}

func readGo(t *testing.T, path string) string {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}
