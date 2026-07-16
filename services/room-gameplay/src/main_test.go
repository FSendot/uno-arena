package main

import (
	"net/http"
	"testing"
	"time"
)

func TestRoomHTTPServerBoundsSlowPeersWithoutBreakingStreams(t *testing.T) {
	server := newRoomHTTPServer(http.NotFoundHandler())
	if server.ReadHeaderTimeout != 5*time.Second || server.ReadTimeout != 10*time.Second || server.IdleTimeout != time.Minute {
		t.Fatalf("timeouts header=%s read=%s idle=%s", server.ReadHeaderTimeout, server.ReadTimeout, server.IdleTimeout)
	}
	if server.WriteTimeout != 0 {
		t.Fatalf("streaming write timeout=%s want=0", server.WriteTimeout)
	}
	if server.MaxHeaderBytes != 1<<20 {
		t.Fatalf("max header bytes=%d", server.MaxHeaderBytes)
	}
	if roomShutdownTimeout != 5*time.Second {
		t.Fatalf("shutdown timeout=%s", roomShutdownTimeout)
	}
}

func TestIntegrityTimeoutStaysInsideExternalRowLockDeadline(t *testing.T) {
	for _, tc := range []struct {
		in, want time.Duration
	}{
		{0, 3 * time.Second},
		{time.Millisecond, 250 * time.Millisecond},
		{2 * time.Second, 2 * time.Second},
		{time.Minute, 4 * time.Second},
	} {
		if got := boundedIntegrityHTTPTimeout(tc.in); got != tc.want {
			t.Fatalf("timeout(%s)=%s want=%s", tc.in, got, tc.want)
		}
	}
}
