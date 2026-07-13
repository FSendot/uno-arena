package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"unoarena/services/room-gameplay/store"
)

type fakeContinuationQueue struct {
	mu       sync.Mutex
	items    []store.NextGameContinuation
	claimErr error
	acked    []store.NextGameContinuation
	released []store.NextGameContinuation
}

func (q *fakeContinuationQueue) ClaimDue(context.Context, time.Time, int) ([]store.NextGameContinuation, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return append([]store.NextGameContinuation(nil), q.items...), q.claimErr
}
func (q *fakeContinuationQueue) Release(_ context.Context, item store.NextGameContinuation, _ time.Time) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.released = append(q.released, item)
	return nil
}
func (q *fakeContinuationQueue) Ack(_ context.Context, item store.NextGameContinuation) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.acked = append(q.acked, item)
	return nil
}

func TestTimerWorker_NextGameContinuationDispatchesDeterministicInternalCommand(t *testing.T) {
	item := store.NextGameContinuation{
		RoomID: "room-1", CompletedGameID: "game-1",
		CommandID: "auto-next-room-1-after-game-1", NextGameID: "game-next-game-1",
	}
	var gotPath, gotCredential string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotCredential = r.Header.Get("X-Service-Credential")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"commandId":"auto-next-room-1-after-game-1","type":"StartNextGame","status":"accepted","schemaVersion":1}`))
	}))
	t.Cleanup(srv.Close)
	queue := &fakeContinuationQueue{items: []store.NextGameContinuation{item}}
	w := NewTimerWorkerWithContinuations(nil, queue, srv.URL, "timer-secret")
	w.tickNextGameContinuations(context.Background(), time.Now().UTC())
	if gotPath != "/internal/v1/rooms/room-1/timer-commands" || gotCredential != "timer-secret" {
		t.Fatalf("request path=%q credential=%q", gotPath, gotCredential)
	}
	if len(queue.acked) != 1 || len(queue.released) != 0 {
		t.Fatalf("acked=%d released=%d", len(queue.acked), len(queue.released))
	}
}

func TestTimerWorker_NextGameContinuationReleasesTransientFailure(t *testing.T) {
	item := store.NextGameContinuation{RoomID: "room-1", CommandID: "auto-next", NextGameID: "game-2"}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "GI unavailable", http.StatusBadGateway)
	}))
	t.Cleanup(srv.Close)
	queue := &fakeContinuationQueue{items: []store.NextGameContinuation{item}}
	w := NewTimerWorkerWithContinuations(nil, queue, srv.URL, "timer-secret")
	w.tickNextGameContinuations(context.Background(), time.Now().UTC())
	if len(queue.released) != 1 || len(queue.acked) != 0 {
		t.Fatalf("acked=%d released=%d", len(queue.acked), len(queue.released))
	}
}

func TestTimerWorker_NextGameContinuationDoesNotAckRetryableRejectedEnvelope(t *testing.T) {
	item := store.NextGameContinuation{RoomID: "room-1", CommandID: "auto-next", NextGameID: "game-2"}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"commandId":"auto-next","type":"StartNextGame","status":"rejected","schemaVersion":1,"reason":"deal_mismatch"}`))
	}))
	t.Cleanup(srv.Close)
	queue := &fakeContinuationQueue{items: []store.NextGameContinuation{item}}
	w := NewTimerWorkerWithContinuations(nil, queue, srv.URL, "timer-secret")
	w.tickNextGameContinuations(context.Background(), time.Now().UTC())
	if len(queue.released) != 1 || len(queue.acked) != 0 {
		t.Fatalf("acked=%d released=%d", len(queue.acked), len(queue.released))
	}
}

func TestTimerWorker_NextGameContinuationAcksTerminalRejectedEnvelope(t *testing.T) {
	item := store.NextGameContinuation{RoomID: "room-1", CommandID: "auto-next", NextGameID: "game-2"}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"commandId":"auto-next","type":"StartNextGame","status":"rejected","schemaVersion":1,"reason":"already_terminal"}`))
	}))
	t.Cleanup(srv.Close)
	queue := &fakeContinuationQueue{items: []store.NextGameContinuation{item}}
	w := NewTimerWorkerWithContinuations(nil, queue, srv.URL, "timer-secret")
	w.tickNextGameContinuations(context.Background(), time.Now().UTC())
	if len(queue.acked) != 1 || len(queue.released) != 0 {
		t.Fatalf("acked=%d released=%d", len(queue.acked), len(queue.released))
	}
}

func TestTimerWorker_NextGameContinuationClaimFailureDoesNotDispatch(t *testing.T) {
	queue := &fakeContinuationQueue{claimErr: errors.New("postgres unavailable")}
	w := NewTimerWorkerWithContinuations(nil, queue, "http://unused", "timer-secret")
	w.tickNextGameContinuations(context.Background(), time.Now().UTC())
	if len(queue.acked) != 0 || len(queue.released) != 0 {
		t.Fatal("claim failure must have no side effects")
	}
}

func TestContinuationRetryDelay_IsExponentiallyBounded(t *testing.T) {
	for _, tc := range []struct {
		attempts int
		want     time.Duration
	}{
		{attempts: 0, want: time.Second},
		{attempts: 1, want: time.Second},
		{attempts: 2, want: 2 * time.Second},
		{attempts: 5, want: 15 * time.Second},
		{attempts: 100, want: 15 * time.Second},
	} {
		if got := continuationRetryDelay(tc.attempts); got != tc.want {
			t.Fatalf("attempts=%d delay=%s want=%s", tc.attempts, got, tc.want)
		}
	}
}
