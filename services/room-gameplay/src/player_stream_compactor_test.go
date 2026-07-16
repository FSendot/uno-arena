package main

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"
)

type fakePlayerStreamCatalog struct {
	pages  [][]string
	calls  []string
	limits []int
}

func (c *fakePlayerStreamCatalog) ListPlayerStreamRoomIDs(_ context.Context, after string, limit int) ([]string, error) {
	c.calls = append(c.calls, after)
	c.limits = append(c.limits, limit)
	if len(c.pages) == 0 {
		return nil, nil
	}
	page := c.pages[0]
	c.pages = c.pages[1:]
	return page, nil
}

type trimCall struct {
	stream string
	maxLen int64
	limit  int64
}

type fakePlayerStreamTrimmer struct {
	calls []trimCall
	fail  map[string]error
}

func (t *fakePlayerStreamTrimmer) TrimApprox(_ context.Context, stream string, maxLen, limit int64) error {
	t.calls = append(t.calls, trimCall{stream: stream, maxLen: maxLen, limit: limit})
	return t.fail[stream]
}

func TestPlayerStreamCompactorUsesOneAuthoritativeKeysetPagePerRun(t *testing.T) {
	catalog := &fakePlayerStreamCatalog{pages: [][]string{{"room-a", "room-b"}, {"room-c"}}}
	trimmer := &fakePlayerStreamTrimmer{}
	worker := NewPlayerStreamCompactor(catalog, trimmer)
	worker.Configure(2, 512, 2048, 10*time.Second, time.Second)
	if err := worker.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(catalog.calls, []string{""}) || !reflect.DeepEqual(catalog.limits, []int{2}) {
		t.Fatalf("catalog calls=%v limits=%v", catalog.calls, catalog.limits)
	}
	if got := []string{trimmer.calls[0].stream, trimmer.calls[1].stream}; !reflect.DeepEqual(got, []string{"room:room-a:player", "room:room-b:player"}) {
		t.Fatalf("streams=%v", got)
	}
	for _, call := range trimmer.calls {
		if call.maxLen != 512 || call.limit != 2048 {
			t.Fatalf("trim call=%+v", call)
		}
	}
	if worker.afterRoomID != "room-b" {
		t.Fatalf("cursor=%q", worker.afterRoomID)
	}
	if err := worker.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(catalog.calls, []string{"", "room-b"}) || worker.afterRoomID != "" {
		t.Fatalf("catalog calls=%v reset cursor=%q", catalog.calls, worker.afterRoomID)
	}
}

func TestPlayerStreamCompactorReportsErrorsAndContinuesBoundedPage(t *testing.T) {
	catalog := &fakePlayerStreamCatalog{pages: [][]string{{"bad:id", "room-ok", "room-fail"}}}
	trimmer := &fakePlayerStreamTrimmer{fail: map[string]error{"room:room-fail:player": errors.New("redis unavailable")}}
	worker := NewPlayerStreamCompactor(catalog, trimmer)
	worker.Configure(3, 1024, 4096, time.Second, time.Second)
	err := worker.RunOnce(context.Background())
	if err == nil || len(trimmer.calls) != 2 || worker.afterRoomID != "room-fail" {
		t.Fatalf("err=%v calls=%v cursor=%q", err, trimmer.calls, worker.afterRoomID)
	}
}

func TestPlayerStreamCompactorConfigurationIsBounded(t *testing.T) {
	if got := boundedPlayerStreamPageSize(9999); got != 500 {
		t.Fatalf("page=%d", got)
	}
	if got := boundedPlayerStreamMaxLen(0); got != defaultPlayerStreamMaxLen {
		t.Fatalf("default maxlen=%d", got)
	}
	if got := boundedPlayerStreamMaxLen(2_000_000); got != 1_000_000 {
		t.Fatalf("max maxlen=%d", got)
	}
	if got := boundedPlayerStreamTrimLimit(999_999); got != 100_000 {
		t.Fatalf("trim limit=%d", got)
	}
	if got := boundedPlayerStreamCadence(time.Millisecond); got != time.Second {
		t.Fatalf("cadence=%s", got)
	}
	if got := boundedPlayerStreamOperationTimeout(time.Minute); got != 30*time.Second {
		t.Fatalf("operation timeout=%s", got)
	}
}

func TestPlayerStreamCompactorCadenceSupportsDeterministicJitter(t *testing.T) {
	worker := NewPlayerStreamCompactor(&fakePlayerStreamCatalog{}, &fakePlayerStreamTrimmer{})
	worker.jitter = func(delay time.Duration) time.Duration { return delay + 250*time.Millisecond }
	if got := worker.jitter(time.Second); got != 1250*time.Millisecond {
		t.Fatalf("jitter=%s", got)
	}
}

func TestPlayerStreamCompactorConfigurationLoadsEnvironment(t *testing.T) {
	t.Setenv("ROOM_PLAYER_STREAM_MAXLEN", "2048")
	t.Setenv("ROOM_PLAYER_STREAM_COMPACTOR_PAGE_SIZE", "75")
	t.Setenv("ROOM_PLAYER_STREAM_TRIM_LIMIT", "8192")
	t.Setenv("ROOM_PLAYER_STREAM_COMPACTOR_INTERVAL_MILLIS", "15000")
	t.Setenv("ROOM_PLAYER_STREAM_OPERATION_TIMEOUT_MILLIS", "2500")
	cfg := loadRoomRuntimeConfig()
	if cfg.PlayerStreamMaxLen != 2048 || cfg.PlayerStreamCompactorPage != 75 || cfg.PlayerStreamTrimLimit != 8192 ||
		cfg.PlayerStreamCompactorCadence != 15*time.Second || cfg.PlayerStreamOperationTimeout != 2500*time.Millisecond {
		t.Fatalf("player stream compactor config=%+v", cfg)
	}
}
