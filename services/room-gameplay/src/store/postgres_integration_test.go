//go:build integration

package store_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"unoarena/services/room-gameplay/app"
	"unoarena/services/room-gameplay/domain"
	"unoarena/services/room-gameplay/game"
	"unoarena/services/room-gameplay/store"
)

func integrationDSN(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("ROOM_POSTGRES_URL")
	if dsn == "" {
		dsn = os.Getenv("DATABASE_URL")
	}
	if dsn == "" {
		t.Skip("ROOM_POSTGRES_URL / DATABASE_URL not set")
	}
	if err := requireSafeRoomTestDatabase(dsn); err != nil {
		t.Fatalf("%v", err)
	}
	return dsn
}

func applyMigration(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	path := filepath.Join("..", "..", "migrations", "001_init.sql")
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(context.Background(), string(b)); err != nil {
		t.Fatalf("apply migration: %v", err)
	}
	if _, err := pool.Exec(context.Background(), `
		CREATE TABLE IF NOT EXISTS schema_bootstrap_meta (
			version TEXT PRIMARY KEY,
			checksum TEXT NOT NULL,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)
	`); err != nil {
		t.Fatalf("bootstrap meta table: %v", err)
	}
	if _, err := pool.Exec(context.Background(), `
		INSERT INTO schema_bootstrap_meta (version, checksum) VALUES ($1, $2)
		ON CONFLICT (version) DO UPDATE SET checksum = EXCLUDED.checksum
	`, store.ExpectedBootstrapVersion, store.ExpectedSchemaChecksum); err != nil {
		t.Fatalf("bootstrap meta: %v", err)
	}
}

func resetPublic(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	_, err := pool.Exec(context.Background(), `
		DROP TABLE IF EXISTS
			realtime_outbox_events, integration_outbox_events,
			processed_reconciliation_offsets, pending_integrity_reconciliations,
			room_maintenance_leases,
			pending_rejection_audits, player_stream_highwater, tournament_provisions,
		player_session_bindings, next_game_continuations, reconnect_deadlines, uno_deadlines,
		command_idempotency, current_games, room_roster, room_runtime_assignments, rooms,
			schema_migrations, schema_bootstrap_meta CASCADE
	`)
	if err != nil {
		t.Fatal(err)
	}
}

func TestIntegration_RuntimeAssignmentCreatedAndGenerationFenced(t *testing.T) {
	pool := openPool(t)
	sessions := store.NewSessionStore(pool)
	room, outcome := domain.CreateRoom(domain.CreateRoomCommand{
		CommandID: "runtime-create", RoomID: "runtime-room", HostID: "host", Visibility: domain.VisibilityPrivate, MaxSeats: 2,
	})
	if room == nil || !outcome.Accepted() {
		t.Fatalf("create room outcome = %#v", outcome)
	}
	if err := sessions.Commit(context.Background(), app.CommitRequest{Session: domain.OpenSession(room)}); err != nil {
		t.Fatal(err)
	}
	var generation int64
	var desired string
	if err := pool.QueryRow(context.Background(), `SELECT generation, desired_state FROM room_runtime_assignments WHERE room_id = 'runtime-room'`).Scan(&generation, &desired); err != nil {
		t.Fatal(err)
	}
	if generation != 1 || desired != "running" {
		t.Fatalf("assignment = generation %d desired %s", generation, desired)
	}
	claimed, err := sessions.ClaimRuntimeAssignments(context.Background(), "integration-controller", 1, 5*time.Second)
	if err != nil {
		t.Fatalf("claim runtime assignment with null inet: %v", err)
	}
	if len(claimed) != 1 || claimed[0].RoomID != "runtime-room" || claimed[0].PodIP != "" {
		t.Fatalf("claimed assignment = %#v", claimed)
	}
	if _, err := sessions.BeginExistingGeneration(context.Background(), "runtime-room", 2); !errors.Is(err, app.ErrRuntimeGenerationStale) {
		t.Fatalf("stale generation error = %v", err)
	}
}

func openPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := integrationDSN(t)
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	resetPublic(t, pool)
	applyMigration(t, pool)
	return pool
}

func TestIntegration_SchemaVerify(t *testing.T) {
	pool := openPool(t)
	if err := store.VerifySchema(context.Background(), pool, store.DefaultSchemaExpectation()); err != nil {
		t.Fatal(err)
	}
}

func TestIntegration_ReconciliationClaimsAreBoundedSkipLockedAndRecoverAfterLease(t *testing.T) {
	pool := openPool(t)
	ctx := context.Background()
	for _, id := range []string{"claim-1", "claim-2", "claim-3"} {
		if _, err := pool.Exec(ctx, `
			INSERT INTO pending_integrity_reconciliations (command_id, room_id, expected_revision, payload, next_attempt_at)
			VALUES ($1, 'room-claims', 0, '{}', clock_timestamp())
		`, id); err != nil {
			t.Fatal(err)
		}
	}
	storeA := store.NewSessionStore(pool)
	storeB := store.NewSessionStore(pool)
	first, err := storeA.ClaimPendingReconciliationMarkers(ctx, "worker-a", 2, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 2 {
		t.Fatalf("first bounded claim=%d want=2", len(first))
	}
	if renewed, err := storeA.RenewReconciliationClaim(ctx, first[0].CommandID, "worker-a", time.Minute); err != nil || !renewed {
		t.Fatalf("owner renewal=%v err=%v", renewed, err)
	}
	if renewed, err := storeB.RenewReconciliationClaim(ctx, first[0].CommandID, "worker-b", time.Minute); err != nil || renewed {
		t.Fatalf("non-owner renewal=%v err=%v", renewed, err)
	}
	second, err := storeB.ClaimPendingReconciliationMarkers(ctx, "worker-b", 2, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(second) != 1 || second[0].CommandID == first[0].CommandID || second[0].CommandID == first[1].CommandID {
		t.Fatalf("second claim must skip active leases: first=%+v second=%+v", first, second)
	}
	if _, err := pool.Exec(ctx, `
		UPDATE pending_integrity_reconciliations
		SET lease_until = clock_timestamp() - interval '1 microsecond'
		WHERE room_id = 'room-claims'
	`); err != nil {
		t.Fatal(err)
	}
	recovered, err := storeB.ClaimPendingReconciliationMarkers(ctx, "worker-b", 3, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(recovered) != 3 {
		t.Fatalf("expired claims recovered=%d want=3", len(recovered))
	}
}

func TestIntegration_TimerIndexRebuildUsesContextLeaseAndCompletionFence(t *testing.T) {
	pool := openPool(t)
	ctx := context.Background()
	sessions := store.NewSessionStore(pool)
	generation, err := sessions.TimerIndexRebuildGeneration(ctx)
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := sessions.ClaimTimerIndexRebuild(ctx, "timer-a", generation, time.Minute)
	if err != nil || !claimed {
		t.Fatalf("first claim=%v err=%v", claimed, err)
	}
	claimed, err = sessions.ClaimTimerIndexRebuild(ctx, "timer-b", generation, time.Minute)
	if err != nil || claimed {
		t.Fatalf("concurrent claim=%v err=%v", claimed, err)
	}
	if renewed, err := sessions.RenewTimerIndexRebuild(ctx, "timer-b", time.Minute); err != nil || renewed {
		t.Fatalf("non-owner renewal=%v err=%v", renewed, err)
	}
	if renewed, err := sessions.RenewTimerIndexRebuild(ctx, "timer-a", time.Minute); err != nil || !renewed {
		t.Fatalf("owner renewal=%v err=%v", renewed, err)
	}
	if err := sessions.CompleteTimerIndexRebuild(ctx, "timer-a"); err != nil {
		t.Fatal(err)
	}
	claimed, err = sessions.ClaimTimerIndexRebuild(ctx, "timer-b", generation, time.Minute)
	if err != nil || claimed {
		t.Fatalf("same rollout must observe completion: claim=%v err=%v", claimed, err)
	}
	if completed, err := sessions.TimerIndexRebuildCompletedAfter(ctx, generation); err != nil || !completed {
		t.Fatalf("same rollout completion=%v err=%v", completed, err)
	}
	laterGeneration, err := sessions.TimerIndexRebuildGeneration(ctx)
	if err != nil {
		t.Fatal(err)
	}
	claimed, err = sessions.ClaimTimerIndexRebuild(ctx, "timer-c", laterGeneration, time.Minute)
	if err != nil || !claimed {
		t.Fatalf("later worker may initiate a fresh guarded rebuild: claim=%v err=%v", claimed, err)
	}
	if _, err := pool.Exec(ctx, `UPDATE room_maintenance_leases SET lease_until = now() - interval '1 second' WHERE lease_name = 'timer-index-rebuild'`); err != nil {
		t.Fatal(err)
	}
	if err := sessions.CompleteTimerIndexRebuild(ctx, "timer-c"); err == nil {
		t.Fatal("expired owner must not record rebuild completion")
	}
}

func TestIntegration_CreateRoomUniqueInsertAndRoundTrip(t *testing.T) {
	pool := openPool(t)
	sessions := store.NewSessionStore(pool)
	ctx := context.Background()

	room, out := domain.CreateRoom(domain.CreateRoomCommand{
		CommandID: "cmd_create", RoomID: "room_rt", HostID: "host", Visibility: domain.VisibilityPublic, MaxSeats: 4,
	})
	if out.Rejection != nil {
		t.Fatal(out.Rejection)
	}
	sess := domain.OpenSession(room)
	uow, err := sessions.BeginCreate(ctx, "room_rt")
	if err != nil {
		t.Fatal(err)
	}
	if uow.Exists() {
		t.Fatal("expected no prior room")
	}
	if err := uow.CommitAccepted(app.DurableAcceptedCommit{
		Session: sess, CommandID: "cmd_create", CommandType: "CreateRoom", Outcome: out,
		CreatePath: true, IntegrityRevision: 1, SetIntegrityRevision: true, LogOffset: 1,
	}); err != nil {
		t.Fatal(err)
	}

	got, ok := sessions.Get(ctx, "room_rt")
	if !ok {
		t.Fatal("missing room")
	}
	if got.Room().Sequence() != room.Sequence() || got.Room().HostID() != "host" {
		t.Fatalf("roundtrip mismatch: %+v", got.Room())
	}
	if got.Room().Roster().Capacity() != 4 {
		t.Fatalf("capacity=%d", got.Room().Roster().Capacity())
	}
}

func TestIntegration_LockSerialization(t *testing.T) {
	pool := openPool(t)
	sessions := store.NewSessionStore(pool)
	ctx := context.Background()

	room, out := domain.CreateRoom(domain.CreateRoomCommand{
		CommandID: "c0", RoomID: "room_lock", HostID: "host", MaxSeats: 2,
	})
	sess := domain.OpenSession(room)
	uow, _ := sessions.BeginCreate(ctx, "room_lock")
	_ = uow.CommitAccepted(app.DurableAcceptedCommit{
		Session: sess, CommandID: "c0", CommandType: "CreateRoom", Outcome: out, CreatePath: true,
	})

	firstHeld := make(chan struct{})
	releaseFirst := make(chan struct{})
	secondStarted := make(chan struct{})
	secondAcquired := make(chan struct{})

	type lockResult struct {
		who string
		err error
	}
	results := make(chan lockResult, 2)

	go func() {
		u, err := sessions.BeginExisting(ctx, "room_lock")
		if err != nil {
			results <- lockResult{who: "first", err: err}
			return
		}
		close(firstHeld)
		<-releaseFirst
		results <- lockResult{who: "first", err: u.Rollback()}
	}()

	select {
	case <-firstHeld:
	case r := <-results:
		t.Fatalf("first lock: %v", r.err)
	case <-time.After(3 * time.Second):
		t.Fatal("first BeginExisting did not acquire")
	}

	go func() {
		close(secondStarted)
		u, err := sessions.BeginExisting(ctx, "room_lock")
		if err != nil {
			results <- lockResult{who: "second", err: err}
			return
		}
		close(secondAcquired)
		results <- lockResult{who: "second", err: u.Rollback()}
	}()

	select {
	case <-secondStarted:
	case <-time.After(time.Second):
		t.Fatal("second goroutine did not start")
	}

	select {
	case <-secondAcquired:
		t.Fatal("second BeginExisting returned while first still held FOR UPDATE")
	case r := <-results:
		t.Fatalf("unexpected result while first held (who=%s): %v", r.who, r.err)
	case <-time.After(200 * time.Millisecond):
		// expected: still blocked on the row lock
	}

	close(releaseFirst)

	seen := map[string]bool{}
	// Drain first rollback if it races ahead; only second acquisition failure is fatal here.
waitSecond:
	for {
		select {
		case <-secondAcquired:
			break waitSecond
		case r := <-results:
			if r.who == "first" {
				if r.err != nil {
					t.Fatalf("first rollback: %v", r.err)
				}
				seen[r.who] = true
				continue
			}
			t.Fatalf("second lock failed: %v", r.err)
		case <-time.After(3 * time.Second):
			t.Fatal("second BeginExisting did not acquire after first released")
		}
	}

	for len(seen) < 2 {
		select {
		case r := <-results:
			if r.err != nil {
				t.Fatalf("%s rollback: %v", r.who, r.err)
			}
			seen[r.who] = true
		case <-time.After(3 * time.Second):
			t.Fatalf("missing rollback results, seen=%v", seen)
		}
	}
}

func TestIntegration_DuplicateCommandNoSecondGI(t *testing.T) {
	pool := openPool(t)
	sessions := store.NewSessionStore(pool)
	gi := app.NewFakeGameIntegrity()
	svc := app.NewService(app.ServiceDeps{
		Sessions: sessions, Commands: sessions, Integrity: gi,
		Publisher: app.NewFakeEventPublisher(), Audit: app.NewFakeAuditSink(),
		Deals: app.NewFakeDealSource(), Clock: app.SystemClock{}, SessionsV: app.AllowAllSessionValidator{},
	})
	ctx := context.Background()
	res := svc.HandleCommand(ctx, app.CommandInput{
		CommandID: "dup1", Type: app.CmdCreateRoom, SchemaVersion: 1,
		PlayerID: "host", SessionID: "sess", RoomID: "room_dup",
		Payload: mustJSON(map[string]any{"roomId": "room_dup", "visibility": "public", "maxSeats": 2}),
	})
	if res.Err != nil || res.Result.Status != "accepted" {
		t.Fatalf("create: %+v err=%v", res.Result, res.Err)
	}
	n := len(gi.Appends)
	res2 := svc.HandleCommand(ctx, app.CommandInput{
		CommandID: "dup1", Type: app.CmdCreateRoom, SchemaVersion: 1,
		PlayerID: "host", SessionID: "sess", RoomID: "room_dup",
		Payload: mustJSON(map[string]any{"roomId": "room_dup", "visibility": "public", "maxSeats": 2}),
	})
	if res2.Err != nil {
		t.Fatal(res2.Err)
	}
	if len(gi.Appends) != n {
		t.Fatalf("second GI append occurred: %d -> %d", n, len(gi.Appends))
	}
}

func TestIntegration_AutoNextGamePersistsAndRecoversDurableContinuation(t *testing.T) {
	pool := openPool(t)
	sessions := store.NewSessionStore(pool)
	ctx := context.Background()

	room, out := domain.CreateRoom(domain.CreateRoomCommand{
		CommandID: "auto_create", RoomID: "room_auto_next", HostID: "host", MaxSeats: 2,
	})
	if out.Rejection != nil {
		t.Fatal(out.Rejection)
	}
	if joined := room.JoinRoom(domain.JoinRoomCommand{CommandID: "auto_join", PlayerID: "guest", ExpectedSequence: room.Sequence()}); joined.Rejection != nil {
		t.Fatal(joined.Rejection)
	}
	if locked := room.LockRoom(domain.LockRoomCommand{CommandID: "auto_lock", ActorID: "host", ExpectedSequence: room.Sequence()}); locked.Rejection != nil {
		t.Fatal(locked.Rejection)
	}
	sess := domain.OpenSession(room)
	started := sess.StartMatch(domain.StartMatchCommand{
		CommandID: "auto_start", ActorID: "host", GameID: "game-1", ExpectedSequence: room.Sequence(),
	}, game.DealMaterial{
		Hands: map[game.PlayerID][]game.Card{
			"host":  {{ID: "winning-card", Color: game.ColorRed, Face: game.Face3}},
			"guest": {{ID: "guest-card", Color: game.ColorBlue, Face: game.Face2}},
		},
		DiscardTop:  game.Card{ID: "discard", Color: game.ColorRed, Face: game.Face5},
		ActiveColor: game.ColorRed, CurrentSeat: 0, Direction: game.DirectionClockwise,
		DrawPileSize: 93, HasDrawPileSize: true,
	})
	if started.Rejection != nil {
		t.Fatal(started.Rejection)
	}
	uow, err := sessions.BeginCreate(ctx, room.ID())
	if err != nil {
		t.Fatal(err)
	}
	if err := uow.CommitAccepted(app.DurableAcceptedCommit{
		Session: sess, CommandID: "seed_auto", CommandType: "Seed", Outcome: started,
		CreatePath: true, IntegrityRevision: 0, SetIntegrityRevision: true,
	}); err != nil {
		t.Fatal(err)
	}

	gi := app.NewFakeGameIntegrity()
	deals := app.NewFakeDealSource()
	svc := app.NewService(app.ServiceDeps{
		Sessions: sessions, Commands: sessions, Integrity: gi,
		Publisher: app.NewFakeEventPublisher(), Audit: app.NewFakeAuditSink(),
		Deals: deals, Clock: app.SystemClock{}, SessionsV: app.AllowAllSessionValidator{},
	})
	seq := int64(sess.Room().Sequence())
	result := svc.HandleCommand(ctx, app.CommandInput{
		CommandID: "auto_win", Type: app.CmdPlayCard, SchemaVersion: 1,
		RoomID: "room_auto_next", PlayerID: "host", SessionID: "host-session",
		ExpectedSequenceNumber: &seq,
		Payload:                mustJSON(map[string]any{"roomId": "room_auto_next", "cardId": "winning-card"}),
	})
	if result.Err != nil || result.Result.Status != "accepted" {
		t.Fatalf("play=%+v err=%v", result.Result, result.Err)
	}
	live, ok := sessions.Get(ctx, "room_auto_next")
	if !ok || !live.Room().GameCompletedInMatch() || live.Game() == nil || !live.Game().Completed() {
		t.Fatalf("completed game must commit before continuation: ok=%v session=%+v", ok, live)
	}
	if gi.Len() != 1 || deals.DealCalls != 0 {
		t.Fatalf("origin must not synchronously continue: GI=%d deals=%d", gi.Len(), deals.DealCalls)
	}

	queue := store.NewNextGameContinuationQueue(pool).WithLeaseTTL(5 * time.Millisecond)
	claimed, err := queue.ClaimDue(ctx, time.Now().UTC().Add(time.Second), 1)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim durable continuation: %v %#v", err, claimed)
	}
	item := claimed[0]
	if item.CommandID != "auto-next-room_auto_next-after-game-1" || item.NextGameID != "game-next-game-1" {
		t.Fatalf("continuation identities=%+v", item)
	}
	// Simulate worker death after claim: lease expiry must make the same identity reclaimable.
	time.Sleep(10 * time.Millisecond)
	reclaimed, err := queue.ClaimDue(ctx, time.Now().UTC().Add(time.Second), 1)
	if err != nil || len(reclaimed) != 1 || reclaimed[0].CommandID != item.CommandID {
		t.Fatalf("reclaim after worker crash: %v %#v", err, reclaimed)
	}

	// A stale supplied token models concurrent Room commands after claim. The
	// system continuation resolves sequence under the Room row lock.
	stale := int64(0)
	deals.DealFn = func(_, _ string, _ []string) (game.DealMaterial, error) {
		return game.DealMaterial{}, nil
	}
	rejected := svc.HandleCommand(ctx, app.CommandInput{
		CommandID: item.CommandID, Type: app.CmdStartNextGame, SchemaVersion: 1,
		RoomID: item.RoomID, ExpectedSequenceNumber: &stale, AsSystem: true,
		Payload: mustJSON(map[string]any{"roomId": item.RoomID, "gameId": item.NextGameID}),
	})
	if rejected.Err != nil || rejected.Result.Status != "rejected" {
		t.Fatalf("retryable continuation rejection=%+v err=%v", rejected.Result, rejected.Err)
	}
	var poisoned int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM command_idempotency WHERE command_id = $1`, item.CommandID).Scan(&poisoned); err != nil || poisoned != 0 {
		t.Fatalf("retryable rejection poisoned deterministic identity: count=%d err=%v", poisoned, err)
	}
	deals.DealFn = nil
	continued := svc.HandleCommand(ctx, app.CommandInput{
		CommandID: item.CommandID, Type: app.CmdStartNextGame, SchemaVersion: 1,
		RoomID: item.RoomID, ExpectedSequenceNumber: &stale, AsSystem: true,
		Payload: mustJSON(map[string]any{"roomId": item.RoomID, "gameId": item.NextGameID}),
	})
	if continued.Err != nil || continued.Result.Status != "accepted" {
		t.Fatalf("continue=%+v err=%v", continued.Result, continued.Err)
	}
	live, ok = sessions.Get(ctx, "room_auto_next")
	if !ok || live.Room().GameCompletedInMatch() || live.Game() == nil || live.Game().Completed() {
		t.Fatalf("durable next game not active: ok=%v session=%+v", ok, live)
	}
	if got, want := string(live.GameID()), "game-next-game-1"; got != want {
		t.Fatalf("game id=%q want %q", got, want)
	}
	if gi.Len() != 2 || deals.DealCalls != 2 || deals.ConfirmCalls != 1 {
		t.Fatalf("GI=%d deals=%d confirms=%d", gi.Len(), deals.DealCalls, deals.ConfirmCalls)
	}
	remaining, err := queue.ClaimDue(ctx, time.Now().UTC().Add(time.Minute), 1)
	if err != nil || len(remaining) != 0 {
		t.Fatalf("successful continuation must atomically remove queue row: %v %#v", err, remaining)
	}

	// Duplicate originating delivery must not append or reserve again.
	dup := svc.HandleCommand(ctx, app.CommandInput{
		CommandID: "auto_win", Type: app.CmdPlayCard, SchemaVersion: 1,
		RoomID: "room_auto_next", PlayerID: "host", SessionID: "host-session",
		ExpectedSequenceNumber: &seq,
		Payload:                mustJSON(map[string]any{"roomId": "room_auto_next", "cardId": "winning-card"}),
	})
	if dup.Err != nil || gi.Len() != 2 || deals.DealCalls != 2 {
		t.Fatalf("duplicate=%+v GI=%d deals=%d", dup, gi.Len(), deals.DealCalls)
	}
}

func TestIntegration_StaleSequenceRejectedNoGI(t *testing.T) {
	pool := openPool(t)
	sessions := store.NewSessionStore(pool)
	gi := app.NewFakeGameIntegrity()
	svc := app.NewService(app.ServiceDeps{
		Sessions: sessions, Commands: sessions, Integrity: gi,
		Publisher: app.NewFakeEventPublisher(), Audit: app.NewFakeAuditSink(),
		Deals: app.NewFakeDealSource(), Clock: app.SystemClock{}, SessionsV: app.AllowAllSessionValidator{},
	})
	ctx := context.Background()
	create := svc.HandleCommand(ctx, app.CommandInput{
		CommandID: "stale_create", Type: app.CmdCreateRoom, SchemaVersion: 1,
		PlayerID: "host", SessionID: "sess", RoomID: "room_stale",
		Payload: mustJSON(map[string]any{"roomId": "room_stale", "maxSeats": 2}),
	})
	if create.Err != nil || create.Result.Status != "accepted" {
		t.Fatalf("create: %+v err=%v", create.Result, create.Err)
	}
	n := gi.Len()
	stale := int64(0)
	res := svc.HandleCommand(ctx, app.CommandInput{
		CommandID: "stale_join", Type: app.CmdJoinRoom, SchemaVersion: 1,
		PlayerID: "p2", SessionID: "sess2", RoomID: "room_stale",
		ExpectedSequenceNumber: &stale,
		Payload:                mustJSON(map[string]any{"roomId": "room_stale"}),
	})
	if res.Err != nil {
		t.Fatal(res.Err)
	}
	if res.Result.Status == "accepted" {
		t.Fatalf("expected stale rejection, got %+v", res.Result)
	}
	if gi.Len() != n {
		t.Fatalf("stale command must not call GI: %d -> %d", n, gi.Len())
	}
}

func TestIntegration_GIFailureRollback(t *testing.T) {
	pool := openPool(t)
	sessions := store.NewSessionStore(pool)
	gi := app.NewFakeGameIntegrity()
	gi.FailAll = context.DeadlineExceeded
	svc := app.NewService(app.ServiceDeps{
		Sessions: sessions, Commands: sessions, Integrity: gi,
		Publisher: app.NewFakeEventPublisher(), Audit: app.NewFakeAuditSink(),
		Deals: app.NewFakeDealSource(), Clock: app.SystemClock{}, SessionsV: app.AllowAllSessionValidator{},
	})
	res := svc.HandleCommand(context.Background(), app.CommandInput{
		CommandID: "failgi", Type: app.CmdCreateRoom, SchemaVersion: 1,
		PlayerID: "host", SessionID: "sess", RoomID: "room_fail",
		Payload: mustJSON(map[string]any{"roomId": "room_fail", "maxSeats": 2}),
	})
	if res.Err == nil {
		t.Fatal("expected GI failure")
	}
	if _, ok := sessions.Get(context.Background(), "room_fail"); ok {
		t.Fatal("room should not commit after GI failure")
	}
}

func TestIntegration_AtomicDualOutboxesAndDeadlines(t *testing.T) {
	pool := openPool(t)
	sessions := store.NewSessionStore(pool)
	ctx := context.Background()
	room, out := domain.CreateRoom(domain.CreateRoomCommand{
		CommandID: "c1", RoomID: "room_ob", HostID: "host", MaxSeats: 2,
	})
	sess := domain.OpenSession(room)
	uow, err := sessions.BeginCreate(ctx, "room_ob")
	if err != nil {
		t.Fatal(err)
	}
	err = uow.CommitAccepted(app.DurableAcceptedCommit{
		Session: sess, CommandID: "c1", CommandType: "CreateRoom", Outcome: out, CreatePath: true,
		LogOffset: 7, IntegrityRevision: 7, SetIntegrityRevision: true,
		Outbox: app.OutboxEntry{
			RoomID: "room_ob", CommandID: "c1", LogOffset: 7,
			Events: []app.PublishedEvent{
				{EventID: "e1", EventType: "RoomCreated", Stream: app.StreamPlayer, RoomID: "room_ob", SequenceNumber: 1, SchemaVersion: 1, PlayerID: "host", SessionID: "s1", Payload: []byte(`{}`)},
				{EventID: "e2", EventType: "SnapshotSanitized", Topic: app.TopicSpectatorSafe, Stream: app.StreamSpectator, RoomID: "room_ob", SequenceNumber: 1, SchemaVersion: 1, Payload: []byte(`{}`)},
				{EventID: "e3", EventType: "GameCompleted", Topic: app.TopicGameCompleted, RoomID: "room_ob", SequenceNumber: 1, SchemaVersion: 1, Payload: []byte(`{}`)},
			},
		},
		BindPlayerSession: true, PlayerID: "host", PlayerSessionID: "s1",
	})
	if err != nil {
		t.Fatal(err)
	}
	var rt, ig int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM realtime_outbox_events`).Scan(&rt)
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM integration_outbox_events`).Scan(&ig)
	if rt != 1 || ig != 2 {
		t.Fatalf("outboxes realtime=%d integration=%d want realtime=1 integration=2", rt, ig)
	}
	if _, err := sessions.ListPendingOutbox(ctx, 10); err != store.ErrCapabilityOnly {
		t.Fatalf("expected ErrCapabilityOnly, got %v", err)
	}
}

func TestIntegration_BindingsProvisionStreamAudit(t *testing.T) {
	pool := openPool(t)
	sessions := store.NewSessionStore(pool)
	ctx := context.Background()
	room, out := domain.CreateRoom(domain.CreateRoomCommand{
		CommandID: "c2", RoomID: "room_bind", HostID: "host", MaxSeats: 2,
	})
	sess := domain.OpenSession(room)
	uow, _ := sessions.BeginCreate(ctx, "room_bind")
	key := app.ProvisionKey{TournamentID: "t1", RoundNumber: 1, SlotID: "slot-a"}
	_ = uow.CommitAccepted(app.DurableAcceptedCommit{
		Session: sess, CommandID: "c2", Outcome: out, CreatePath: true,
		BindPlayerSession: true, PlayerID: "host", PlayerSessionID: "sess-host",
		ProvisionKey: &key, ProvisionRoomID: "room_bind",
		SetStreamSeq: true, StreamSeqHighWater: 9,
	})
	if sid, ok := sessions.PlayerSession(ctx, "room_bind", "host"); !ok || sid != "sess-host" {
		t.Fatalf("binding=%q ok=%v", sid, ok)
	}
	if rid, ok := sessions.GetProvision(ctx, key); !ok || rid != "room_bind" {
		t.Fatalf("provision=%q ok=%v", rid, ok)
	}
	if sessions.PeekStreamSeq(ctx, "room_bind") != 9 {
		t.Fatalf("stream seq=%d", sessions.PeekStreamSeq(ctx, "room_bind"))
	}
}

func TestIntegration_RedisClaimAckReaperRebuild(t *testing.T) {
	redisURL := os.Getenv("ROOM_REDIS_URL")
	if redisURL == "" {
		t.Skip("ROOM_REDIS_URL not set")
	}
	pool := openPool(t)
	rdb, err := store.NewRedisFromURL(redisURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = rdb.Close() })
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		t.Fatalf("redis unavailable (must not skip): %v", err)
	}

	prefix := os.Getenv("ROOM_REDIS_KEY_PREFIX")
	if prefix == "" {
		prefix = fmt.Sprintf("roomtest_%d:", time.Now().UnixNano())
	}
	idx := store.NewTimerIndex(rdb).WithKeyPrefix(prefix).WithLeaseTTL(5 * time.Millisecond)
	t.Cleanup(func() { _ = idx.FlushPrefixedKeys(context.Background()) })
	if err := idx.LoadScripts(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := idx.FlushPrefixedKeys(context.Background()); err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	now := time.Now().UTC()
	id := store.TimerID{
		Family: "uno", RoomID: "r1", PlayerID: "p1", GameID: "g1", Trigger: "tr",
		OpeningSeq: 1, ExpiresAt: now.Add(-time.Second),
	}
	if err := idx.Schedule(ctx, id); err != nil {
		t.Fatal(err)
	}

	claimed, err := idx.ClaimDue(ctx, "uno", now, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(claimed) != 1 {
		t.Fatalf("claim want 1 got %d", len(claimed))
	}
	if claimed[0].RoomID != "r1" || claimed[0].PlayerID != "p1" {
		t.Fatalf("claimed=%+v", claimed[0])
	}

	// Still inflight: second claim must not re-deliver.
	claimed2, err := idx.ClaimDue(ctx, "uno", now.Add(time.Second), 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(claimed2) != 0 {
		t.Fatalf("inflight must block re-claim, got %d", len(claimed2))
	}

	if err := idx.Ack(ctx, claimed[0]); err != nil {
		t.Fatal(err)
	}
	claimed3, err := idx.ClaimDue(ctx, "uno", now.Add(2*time.Second), 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(claimed3) != 0 {
		t.Fatalf("acked timer must not reappear, got %d", len(claimed3))
	}

	// Reaper: schedule, claim, expire lease, reap, claim again.
	id2 := store.TimerID{
		Family: "uno", RoomID: "r2", PlayerID: "p2", GameID: "g2", Trigger: "tr2",
		OpeningSeq: 2, ExpiresAt: now.Add(-time.Second),
	}
	if err := idx.Schedule(ctx, id2); err != nil {
		t.Fatal(err)
	}
	claimedLease, err := idx.ClaimDue(ctx, "uno", now, 10)
	if err != nil || len(claimedLease) != 1 {
		t.Fatalf("claim for reaper: %v %#v", err, claimedLease)
	}
	time.Sleep(20 * time.Millisecond)
	n, err := idx.ReapExpiredLeases(ctx, "uno", time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if n < 1 {
		t.Fatalf("reaper moved=%d want >=1", n)
	}
	reclaimed, err := idx.ClaimDue(ctx, "uno", time.Now().UTC(), 10)
	if err != nil || len(reclaimed) != 1 {
		t.Fatalf("reclaim after reaper: %v %#v", err, reclaimed)
	}
	_ = idx.Ack(ctx, reclaimed[0])

	// Rebuild from Postgres authority.
	_, _ = pool.Exec(ctx, `
		INSERT INTO rooms (room_id, room_type, status, visibility, capacity, sequence_number, host_player_id, match_score)
		VALUES ('r_rebuild','ad_hoc','waiting','public',2,1,'host','{}')
		ON CONFLICT DO NOTHING
	`)
	_, err = pool.Exec(ctx, `
		INSERT INTO uno_deadlines (room_id, game_id, player_id, triggering_game_event_id, expires_at, opening_room_sequence, status)
		VALUES ('r_rebuild','g','p','trig', now() - interval '1 second', 1, 'open')
	`)
	if err != nil {
		t.Fatal(err)
	}
	if err := idx.RebuildFromPostgres(ctx, pool); err != nil {
		t.Fatal(err)
	}
	rebuilt, err := idx.ClaimDue(ctx, "uno", time.Now().UTC(), 10)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, c := range rebuilt {
		if c.RoomID == "r_rebuild" && c.PlayerID == "p" {
			found = true
			_ = idx.Ack(ctx, c)
		}
	}
	if !found {
		t.Fatalf("rebuild missing r_rebuild deadline, claimed=%#v", rebuilt)
	}
}

func TestIntegration_IdentityGILiveHTTP(t *testing.T) {
	pool := openPool(t)
	sessions := store.NewSessionStore(pool)

	idSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"valid":true}`))
	}))
	t.Cleanup(idSrv.Close)
	giSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"logOffset": 1, "revision": 1})
	}))
	t.Cleanup(giSrv.Close)

	svc := app.NewService(app.ServiceDeps{
		Sessions: sessions, Commands: sessions,
		Integrity: app.NewHTTPGameIntegrity(giSrv.URL, "cred", nil),
		Publisher: app.NewFakeEventPublisher(), Audit: app.NewFakeAuditSink(),
		Deals: app.NewFakeDealSource(), Clock: app.SystemClock{},
		SessionsV: app.NewHTTPSessionValidator(idSrv.URL, "cred", nil),
	})
	res := svc.HandleCommand(context.Background(), app.CommandInput{
		CommandID: "http1", Type: app.CmdCreateRoom, SchemaVersion: 1,
		PlayerID: "host", SessionID: "sess", RoomID: "room_http",
		Payload: mustJSON(map[string]any{"roomId": "room_http", "maxSeats": 2}),
	})
	if res.Err != nil {
		t.Fatal(res.Err)
	}
}

func TestIntegration_GISuccessCommitFailureLeavesAutonomousMarker(t *testing.T) {
	pool := openPool(t)
	sessions := store.NewSessionStore(pool)
	sessions.FailNextCommitAccepted = context.DeadlineExceeded
	gi := app.NewFakeGameIntegrity()
	svc := app.NewService(app.ServiceDeps{
		Sessions: sessions, Commands: sessions, Integrity: gi,
		Publisher: app.NewFakeEventPublisher(), Audit: app.NewFakeAuditSink(),
		Deals: app.NewFakeDealSource(), Clock: app.SystemClock{}, SessionsV: app.AllowAllSessionValidator{},
	})
	res := svc.HandleCommand(context.Background(), app.CommandInput{
		CommandID: "marker1", Type: app.CmdCreateRoom, SchemaVersion: 1,
		PlayerID: "host", SessionID: "sess", RoomID: "room_marker",
		Payload: mustJSON(map[string]any{"roomId": "room_marker", "maxSeats": 2}),
	})
	if res.Err == nil {
		t.Fatal("expected commit failure")
	}
	if _, ok := sessions.Get(context.Background(), "room_marker"); ok {
		t.Fatal("room must not exist after commit failure")
	}
	if gi.Len() != 1 {
		t.Fatalf("expected GI append, got %d", gi.Len())
	}
	markers, err := sessions.ListPendingReconciliationMarkers(context.Background(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(markers) != 1 {
		t.Fatalf("expected 1 pending marker, got %d", len(markers))
	}
	if markers[0].RoomID != "room_marker" || markers[0].CommandID != "marker1" {
		t.Fatalf("marker=%+v", markers[0])
	}
	if !markers[0].HasLogOffset {
		t.Fatal("finalize must have recorded log_offset before commit failure")
	}
	var blob app.ReconciliationRepairBlob
	if err := json.Unmarshal(markers[0].Payload, &blob); err != nil {
		t.Fatal(err)
	}
	if len(blob.SessionSnapshot) == 0 || len(blob.GIPayload) == 0 {
		t.Fatalf("incomplete blob: %+v", blob)
	}
}

func TestIntegration_IntentFailureZeroGI(t *testing.T) {
	pool := openPool(t)
	sessions := store.NewSessionStore(pool)
	sessions.FailNextBeginIntent = fmt.Errorf("intent insert failed")
	gi := app.NewFakeGameIntegrity()
	svc := app.NewService(app.ServiceDeps{
		Sessions: sessions, Commands: sessions, Integrity: gi,
		Publisher: app.NewFakeEventPublisher(), Audit: app.NewFakeAuditSink(),
		Deals: app.NewFakeDealSource(), Clock: app.SystemClock{}, SessionsV: app.AllowAllSessionValidator{},
	})
	res := svc.HandleCommand(context.Background(), app.CommandInput{
		CommandID: "intent_fail", Type: app.CmdCreateRoom, SchemaVersion: 1,
		PlayerID: "host", SessionID: "sess", RoomID: "room_intent_fail",
		Payload: mustJSON(map[string]any{"roomId": "room_intent_fail", "maxSeats": 2}),
	})
	if res.Err == nil {
		t.Fatal("expected intent failure")
	}
	if gi.Len() != 0 {
		t.Fatalf("GI must not be called when intent fails, got %d", gi.Len())
	}
}

func TestIntegration_AppendSuccessFinalizeFailureRepairedOnce(t *testing.T) {
	pool := openPool(t)
	sessions := store.NewSessionStore(pool)
	sessions.FailNextFinalizeIntent = fmt.Errorf("finalize boom")
	gi := app.NewFakeGameIntegrity()
	svc := app.NewService(app.ServiceDeps{
		Sessions: sessions, Commands: sessions, Integrity: gi,
		Publisher: app.NewFakeEventPublisher(), Audit: app.NewFakeAuditSink(),
		Deals: app.NewFakeDealSource(), Clock: app.SystemClock{}, SessionsV: app.AllowAllSessionValidator{},
	})
	res := svc.HandleCommand(context.Background(), app.CommandInput{
		CommandID: "fin_fail", Type: app.CmdCreateRoom, SchemaVersion: 1,
		PlayerID: "host", SessionID: "sess", RoomID: "room_fin_fail",
		Payload: mustJSON(map[string]any{"roomId": "room_fin_fail", "maxSeats": 2}),
	})
	if res.Err == nil {
		t.Fatal("expected finalize failure")
	}
	if gi.Len() != 1 {
		t.Fatalf("GI appends=%d", gi.Len())
	}
	markers, err := sessions.ListPendingReconciliationMarkers(context.Background(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(markers) != 1 || markers[0].HasLogOffset {
		t.Fatalf("pending unfinalized intent required, got %+v", markers)
	}

	n, err := sessions.ReconcilePending(context.Background(), gi, nil, 10)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("reconciled=%d", n)
	}
	if _, ok := sessions.Get(context.Background(), "room_fin_fail"); !ok {
		t.Fatal("repair must create room")
	}
	n2, err := sessions.ReconcilePending(context.Background(), gi, nil, 10)
	if err != nil {
		t.Fatal(err)
	}
	if n2 != 0 {
		t.Fatalf("second reconcile should be no-op, got %d", n2)
	}
	var reconcileEvents int
	_ = pool.QueryRow(context.Background(), `
		SELECT count(*) FROM integration_outbox_events WHERE event_type = $1
	`, app.EventRoomStateReconciled).Scan(&reconcileEvents)
	if reconcileEvents != 1 {
		t.Fatalf("RoomStateReconciled=%d", reconcileEvents)
	}
}

func TestIntegration_NoMatchingGIEventLeavesPending(t *testing.T) {
	pool := openPool(t)
	sessions := store.NewSessionStore(pool)
	gi := app.NewFakeGameIntegrity()
	// Plant a pending intent with no corresponding GI append.
	err := sessions.BeginReconciliationIntent(context.Background(), "orphan_cmd", "room_orphan", 0,
		[]byte(`{"commandId":"orphan_cmd"}`), app.DurableAcceptedCommit{
			Session:   domain.OpenSession(mustCreateRoom(t, "orphan_cmd", "room_orphan")),
			CommandID: "orphan_cmd", CommandType: app.CmdCreateRoom, CreatePath: true,
			Outcome: domain.CommandOutcome{Kind: domain.OutcomeAccepted, CommandID: "orphan_cmd"},
		})
	if err != nil {
		t.Fatal(err)
	}
	_, err = sessions.ReconcilePending(context.Background(), gi, nil, 10)
	if err == nil {
		t.Fatal("expected fail-closed missing GI event")
	}
	markers, err := sessions.ListPendingReconciliationMarkers(context.Background(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(markers) != 1 || markers[0].CommandID != "orphan_cmd" {
		t.Fatalf("must remain pending: %+v", markers)
	}
	_, err = sessions.ReconcilePending(context.Background(), gi, nil, 10)
	if err == nil {
		t.Fatal("retry must still fail closed")
	}
}

type transientNoWriteIntegrity struct {
	mu        sync.Mutex
	failed    bool
	delegate  *app.FakeGameIntegrity
	appendReq []app.AppendRequest
}

func newTransientNoWriteIntegrity() *transientNoWriteIntegrity {
	return &transientNoWriteIntegrity{delegate: app.NewFakeGameIntegrity()}
}

func (g *transientNoWriteIntegrity) Append(ctx context.Context, req app.AppendRequest) (app.AppendResult, error) {
	g.mu.Lock()
	g.appendReq = append(g.appendReq, req)
	if !g.failed {
		g.failed = true
		g.mu.Unlock()
		return app.AppendResult{}, errors.New("transient GI 503 before write")
	}
	g.mu.Unlock()
	return g.delegate.Append(ctx, req)
}

func (g *transientNoWriteIntegrity) Replay(ctx context.Context, roomID string, fromOffset int64) (app.ReplayResult, error) {
	if g.delegate.Len() == 0 {
		return app.ReplayResult{RoomID: roomID, Revision: fromOffset}, nil
	}
	return g.delegate.Replay(ctx, roomID, fromOffset)
}

func TestIntegration_AuthoritativeNoWriteReplayReappendsExactIntent(t *testing.T) {
	pool := openPool(t)
	sessions := store.NewSessionStore(pool)
	gi := newTransientNoWriteIntegrity()
	svc := app.NewService(app.ServiceDeps{
		Sessions: sessions, Commands: sessions, Integrity: gi,
		Publisher: app.NewFakeEventPublisher(), Audit: app.NewFakeAuditSink(),
		Deals: app.NewFakeDealSource(), Clock: app.SystemClock{}, SessionsV: app.AllowAllSessionValidator{},
	})
	in := app.CommandInput{
		CommandID: "transient_no_write", Type: app.CmdCreateRoom, SchemaVersion: 1,
		PlayerID: "host", SessionID: "sess", RoomID: "room_transient_no_write",
		Payload: mustJSON(map[string]any{"roomId": "room_transient_no_write", "maxSeats": 2}),
	}
	first := svc.HandleCommand(context.Background(), in)
	if first.Err == nil {
		t.Fatal("expected uncertain first append response")
	}
	if _, ok := sessions.Get(context.Background(), "room_transient_no_write"); ok {
		t.Fatal("local Room snapshot must not commit before GI recovery")
	}

	n, err := sessions.ReconcilePending(context.Background(), gi, nil, 10)
	if err != nil || n != 1 {
		t.Fatalf("safe deterministic recovery: n=%d err=%v", n, err)
	}
	if _, ok := sessions.Get(context.Background(), "room_transient_no_write"); !ok {
		t.Fatal("reconciler must commit snapshot after proven no-write reappend")
	}
	if len(gi.appendReq) != 2 {
		t.Fatalf("append attempts=%d want initial + exact retry", len(gi.appendReq))
	}
	firstReq, retryReq := gi.appendReq[0], gi.appendReq[1]
	if firstReq.RoomID != retryReq.RoomID || firstReq.GameID != retryReq.GameID ||
		firstReq.EventID != retryReq.EventID || firstReq.ExpectedRevision != retryReq.ExpectedRevision ||
		firstReq.EventType != retryReq.EventType || !sameJSON(firstReq.Payload, retryReq.Payload) {
		t.Fatalf("reappend changed persisted intent:\nfirst=%+v\nretry=%+v", firstReq, retryReq)
	}
}

func TestIntegration_HTTPGIStreamNotFoundRecoversUncertainFirstAppend(t *testing.T) {
	pool := openPool(t)
	sessions := store.NewSessionStore(pool)
	var mu sync.Mutex
	appendAttempts := 0
	var committed struct {
		EventID   string          `json:"eventId"`
		EventType string          `json:"eventType"`
		GameID    string          `json:"gameId"`
		Payload   json.RawMessage `json:"payload"`
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/internal/v1/game-logs/{roomId}/append", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Service-Credential") != "room-cred" {
			http.Error(w, `{"code":"unauthorized"}`, http.StatusUnauthorized)
			return
		}
		var body struct {
			EventID          string          `json:"eventId"`
			ExpectedRevision int64           `json:"expectedRevision"`
			EventType        string          `json:"eventType"`
			GameID           string          `json:"gameId"`
			Payload          json.RawMessage `json:"payload"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, `{"code":"bad_request"}`, http.StatusBadRequest)
			return
		}
		mu.Lock()
		defer mu.Unlock()
		appendAttempts++
		if appendAttempts == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"code":"store_unavailable","message":"transient before write"}`))
			return
		}
		if body.ExpectedRevision != 0 {
			w.WriteHeader(http.StatusConflict)
			_, _ = w.Write([]byte(`{"code":"revision_mismatch"}`))
			return
		}
		committed.EventID, committed.EventType, committed.GameID, committed.Payload = body.EventID, body.EventType, body.GameID, body.Payload
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"kind":"accepted","logOffset":0,"revision":1}`))
	})
	mux.HandleFunc("/internal/v1/game-logs/{roomId}/replay", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Service-Credential") != "audit-cred" || r.Header.Get("X-Audit-Actor") == "" || r.Header.Get("X-Audit-Reason") == "" {
			http.Error(w, `{"code":"unauthorized"}`, http.StatusUnauthorized)
			return
		}
		mu.Lock()
		defer mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		if committed.EventID == "" {
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"code":"invalid_command","message":"stream not found"}`))
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"roomId": r.PathValue("roomId"), "revision": 1,
			"entries": []map[string]any{{
				"offset": 0, "eventId": committed.EventID, "eventType": committed.EventType,
				"gameId": committed.GameID, "payload": committed.Payload,
			}},
		})
	})
	server := httptest.NewServer(mux)
	defer server.Close()
	gi := app.NewHTTPGameIntegrity(server.URL, "room-cred", server.Client())
	gi.AuditCredential = "audit-cred"
	svc := app.NewService(app.ServiceDeps{
		Sessions: sessions, Commands: sessions, Integrity: gi,
		Publisher: app.NewFakeEventPublisher(), Audit: app.NewFakeAuditSink(),
		Deals: app.NewFakeDealSource(), Clock: app.SystemClock{}, SessionsV: app.AllowAllSessionValidator{},
	})
	first := svc.HandleCommand(context.Background(), app.CommandInput{
		CommandID: "http_transient", Type: app.CmdCreateRoom, SchemaVersion: 1,
		PlayerID: "host", SessionID: "sess", RoomID: "room_http_transient",
		Payload: mustJSON(map[string]any{"roomId": "room_http_transient", "maxSeats": 2}),
	})
	if first.Err == nil {
		t.Fatal("expected uncertain first GI append")
	}
	n, err := sessions.ReconcilePending(context.Background(), gi, nil, 10)
	if err != nil || n != 1 {
		t.Fatalf("HTTP stream-not-found recovery: n=%d err=%v", n, err)
	}
	if _, ok := sessions.Get(context.Background(), "room_http_transient"); !ok {
		t.Fatal("reconciler did not commit recovered Room snapshot")
	}
	if appendAttempts != 2 || committed.EventID != "http_transient" {
		t.Fatalf("appendAttempts=%d committed=%+v", appendAttempts, committed)
	}
}

type advancedNoMatchIntegrity struct{ appendCalls int }

func (g *advancedNoMatchIntegrity) Append(context.Context, app.AppendRequest) (app.AppendResult, error) {
	g.appendCalls++
	return app.AppendResult{}, errors.New("must not append across advanced revision")
}

func (g *advancedNoMatchIntegrity) Replay(_ context.Context, roomID string, fromOffset int64) (app.ReplayResult, error) {
	return app.ReplayResult{RoomID: roomID, Revision: fromOffset + 1}, nil
}

func TestIntegration_NoMatchWithAdvancedGIRevisionRemainsFailClosed(t *testing.T) {
	pool := openPool(t)
	sessions := store.NewSessionStore(pool)
	err := sessions.BeginReconciliationIntent(context.Background(), "advanced_cmd", "room_advanced", 0,
		[]byte(`{"commandId":"advanced_cmd"}`), app.DurableAcceptedCommit{
			Session:   domain.OpenSession(mustCreateRoom(t, "advanced_cmd", "room_advanced")),
			CommandID: "advanced_cmd", CommandType: app.CmdCreateRoom, CreatePath: true,
			Outcome: domain.CommandOutcome{Kind: domain.OutcomeAccepted, CommandID: "advanced_cmd"},
		})
	if err != nil {
		t.Fatal(err)
	}
	gi := &advancedNoMatchIntegrity{}
	_, err = sessions.ReconcilePending(context.Background(), gi, nil, 10)
	if err == nil || gi.appendCalls != 0 {
		t.Fatalf("advanced revision must stay fail-closed: err=%v appendCalls=%d", err, gi.appendCalls)
	}
	markers, listErr := sessions.ListPendingReconciliationMarkers(context.Background(), 10)
	if listErr != nil || len(markers) != 1 || markers[0].CommandID != "advanced_cmd" {
		t.Fatalf("advanced marker must remain pending: err=%v markers=%+v", listErr, markers)
	}
}

func sameJSON(a, b []byte) bool {
	var av, bv any
	return json.Unmarshal(a, &av) == nil && json.Unmarshal(b, &bv) == nil && reflect.DeepEqual(av, bv)
}

func TestIntegration_NormalPathMarksIntentDone(t *testing.T) {
	pool := openPool(t)
	sessions := store.NewSessionStore(pool)
	gi := app.NewFakeGameIntegrity()
	svc := app.NewService(app.ServiceDeps{
		Sessions: sessions, Commands: sessions, Integrity: gi,
		Publisher: app.NewFakeEventPublisher(), Audit: app.NewFakeAuditSink(),
		Deals: app.NewFakeDealSource(), Clock: app.SystemClock{}, SessionsV: app.AllowAllSessionValidator{},
	})
	res := svc.HandleCommand(context.Background(), app.CommandInput{
		CommandID: "normal1", Type: app.CmdCreateRoom, SchemaVersion: 1,
		PlayerID: "host", SessionID: "sess", RoomID: "room_normal",
		Payload: mustJSON(map[string]any{"roomId": "room_normal", "maxSeats": 2}),
	})
	if res.Err != nil {
		t.Fatal(res.Err)
	}
	markers, err := sessions.ListPendingReconciliationMarkers(context.Background(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(markers) != 0 {
		t.Fatalf("normal path must mark intent done, pending=%+v", markers)
	}
	var status string
	err = pool.QueryRow(context.Background(), `
		SELECT status FROM pending_integrity_reconciliations WHERE command_id = 'normal1'
	`).Scan(&status)
	if err != nil || status != "done" {
		t.Fatalf("status=%q err=%v", status, err)
	}
}

func mustCreateRoom(t *testing.T, cmd, roomID string) *domain.Room {
	t.Helper()
	room, out := domain.CreateRoom(domain.CreateRoomCommand{
		CommandID: domain.CommandID(cmd), RoomID: domain.RoomID(roomID), HostID: "host", MaxSeats: 2,
	})
	if out.Rejection != nil {
		t.Fatal(out.Rejection)
	}
	return room
}

func TestIntegration_ReconciliationRepairsOnceSecondNoOp(t *testing.T) {
	pool := openPool(t)
	sessions := store.NewSessionStore(pool)
	sessions.FailNextCommitAccepted = context.DeadlineExceeded
	gi := app.NewFakeGameIntegrity()
	svc := app.NewService(app.ServiceDeps{
		Sessions: sessions, Commands: sessions, Integrity: gi,
		Publisher: app.NewFakeEventPublisher(), Audit: app.NewFakeAuditSink(),
		Deals: app.NewFakeDealSource(), Clock: app.SystemClock{}, SessionsV: app.AllowAllSessionValidator{},
	})
	res := svc.HandleCommand(context.Background(), app.CommandInput{
		CommandID: "repair1", Type: app.CmdCreateRoom, SchemaVersion: 1,
		PlayerID: "host", SessionID: "sess", RoomID: "room_repair",
		Payload: mustJSON(map[string]any{"roomId": "room_repair", "maxSeats": 2}),
	})
	if res.Err == nil {
		t.Fatal("expected commit failure")
	}

	n, err := sessions.ReconcilePending(context.Background(), gi, nil, 10)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("reconciled markers=%d", n)
	}
	got, ok := sessions.Get(context.Background(), "room_repair")
	if !ok || got.Room().HostID() != "host" {
		t.Fatalf("repair missing room ok=%v", ok)
	}
	var processed int
	_ = pool.QueryRow(context.Background(), `
		SELECT count(*) FROM processed_reconciliation_offsets WHERE room_id = 'room_repair'
	`).Scan(&processed)
	if processed != 1 {
		t.Fatalf("processed=%d", processed)
	}
	var pending int
	_ = pool.QueryRow(context.Background(), `
		SELECT count(*) FROM pending_integrity_reconciliations WHERE room_id = 'room_repair' AND status = 'pending'
	`).Scan(&pending)
	if pending != 0 {
		t.Fatalf("pending still %d", pending)
	}
	var reconcileEvents int
	_ = pool.QueryRow(context.Background(), `
		SELECT count(*) FROM integration_outbox_events WHERE event_type = $1
	`, app.EventRoomStateReconciled).Scan(&reconcileEvents)
	if reconcileEvents != 1 {
		t.Fatalf("RoomStateReconciled events=%d", reconcileEvents)
	}

	// Second run: no-op (processed offsets).
	n2, err := sessions.ReconcilePending(context.Background(), gi, nil, 10)
	if err != nil {
		t.Fatal(err)
	}
	if n2 != 0 {
		t.Fatalf("second run should see no pending, got %d", n2)
	}
	_ = pool.QueryRow(context.Background(), `
		SELECT count(*) FROM integration_outbox_events WHERE event_type = $1
	`, app.EventRoomStateReconciled).Scan(&reconcileEvents)
	if reconcileEvents != 1 {
		t.Fatalf("double-applied RoomStateReconciled: %d", reconcileEvents)
	}
}

type leaseStealingIntegrity struct {
	inner app.GameIntegrity
	steal func() error
	once  sync.Once
	err   error
}

func (g *leaseStealingIntegrity) Append(ctx context.Context, req app.AppendRequest) (app.AppendResult, error) {
	return g.inner.Append(ctx, req)
}

func (g *leaseStealingIntegrity) Replay(ctx context.Context, roomID string, fromOffset int64) (app.ReplayResult, error) {
	g.once.Do(func() { g.err = g.steal() })
	if g.err != nil {
		return app.ReplayResult{}, g.err
	}
	return g.inner.Replay(ctx, roomID, fromOffset)
}

func TestIntegration_ReconciliationClaimFencePreventsStaleCompletion(t *testing.T) {
	pool := openPool(t)
	ctx := context.Background()
	sessions := store.NewSessionStore(pool)
	sessions.FailNextCommitAccepted = context.DeadlineExceeded
	gi := app.NewFakeGameIntegrity()
	svc := app.NewService(app.ServiceDeps{
		Sessions: sessions, Commands: sessions, Integrity: gi,
		Publisher: app.NewFakeEventPublisher(), Audit: app.NewFakeAuditSink(),
		Deals: app.NewFakeDealSource(), Clock: app.SystemClock{}, SessionsV: app.AllowAllSessionValidator{},
	})
	res := svc.HandleCommand(ctx, app.CommandInput{
		CommandID: "stale_claim", Type: app.CmdCreateRoom, SchemaVersion: 1,
		PlayerID: "host", SessionID: "sess", RoomID: "room_stale_claim",
		Payload: mustJSON(map[string]any{"roomId": "room_stale_claim", "maxSeats": 2}),
	})
	if res.Err == nil {
		t.Fatal("expected commit failure")
	}
	if _, err := pool.Exec(ctx, `
		UPDATE pending_integrity_reconciliations
		SET next_attempt_at = clock_timestamp()
		WHERE command_id = 'stale_claim'
	`); err != nil {
		t.Fatal(err)
	}
	markers, err := sessions.ClaimPendingReconciliationMarkers(ctx, "worker-a", 1, time.Minute)
	if err != nil || len(markers) != 1 {
		t.Fatalf("claim=%+v err=%v", markers, err)
	}
	stealingGI := &leaseStealingIntegrity{
		inner: gi,
		steal: func() error {
			_, err := pool.Exec(ctx, `
				UPDATE pending_integrity_reconciliations
				SET lease_owner = 'worker-b', lease_until = clock_timestamp() + interval '1 minute'
				WHERE command_id = 'stale_claim'
			`)
			return err
		},
	}
	if err := sessions.ReconcileClaimedMarker(ctx, stealingGI, nil, markers[0], "worker-a"); err == nil {
		t.Fatal("stale owner must not complete reconciliation")
	}
	if _, ok := sessions.Get(ctx, "room_stale_claim"); ok {
		t.Fatal("stale repair must not persist the room")
	}
	var status, owner string
	if err := pool.QueryRow(ctx, `
		SELECT status, lease_owner FROM pending_integrity_reconciliations WHERE command_id = 'stale_claim'
	`).Scan(&status, &owner); err != nil {
		t.Fatal(err)
	}
	if status != "pending" || owner != "worker-b" {
		t.Fatalf("stale completion changed marker: status=%q owner=%q", status, owner)
	}
	var reconciledEvents int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM integration_outbox_events WHERE event_type = $1
	`, app.EventRoomStateReconciled).Scan(&reconciledEvents); err != nil {
		t.Fatal(err)
	}
	if reconciledEvents != 0 {
		t.Fatalf("stale repair emitted reconciliation side effects=%d", reconciledEvents)
	}
}

func TestIntegration_ReconciliationLeaseExpiryDuringRepairTransactionRollsBack(t *testing.T) {
	pool := openPool(t)
	ctx := context.Background()
	sessions := store.NewSessionStore(pool)
	sessions.FailNextCommitAccepted = context.DeadlineExceeded
	gi := app.NewFakeGameIntegrity()
	svc := app.NewService(app.ServiceDeps{
		Sessions: sessions, Commands: sessions, Integrity: gi,
		Publisher: app.NewFakeEventPublisher(), Audit: app.NewFakeAuditSink(),
		Deals: app.NewFakeDealSource(), Clock: app.SystemClock{}, SessionsV: app.AllowAllSessionValidator{},
	})
	res := svc.HandleCommand(ctx, app.CommandInput{
		CommandID: "lease_expires_during_repair", Type: app.CmdCreateRoom, SchemaVersion: 1,
		PlayerID: "host", SessionID: "sess", RoomID: "room_lease_expires_during_repair",
		Payload: mustJSON(map[string]any{"roomId": "room_lease_expires_during_repair", "maxSeats": 2}),
	})
	if res.Err == nil {
		t.Fatal("expected commit failure")
	}
	if _, err := pool.Exec(ctx, `
		UPDATE pending_integrity_reconciliations
		SET next_attempt_at = clock_timestamp()
		WHERE command_id = 'lease_expires_during_repair'
	`); err != nil {
		t.Fatal(err)
	}
	markers, err := sessions.ClaimPendingReconciliationMarkers(ctx, "worker-a", 1, time.Minute)
	if err != nil || len(markers) != 1 {
		t.Fatalf("claim=%+v err=%v", markers, err)
	}
	if _, err := pool.Exec(ctx, `
		CREATE OR REPLACE FUNCTION reconciliation_test_expire_claim()
		RETURNS trigger AS $$
		BEGIN
			UPDATE pending_integrity_reconciliations
			SET lease_until = clock_timestamp() + interval '25 milliseconds'
			WHERE command_id = 'lease_expires_during_repair';
			PERFORM pg_sleep(0.100);
			RETURN NEW;
		END;
		$$ LANGUAGE plpgsql;
	`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		CREATE TRIGGER reconciliation_test_expire_claim_before_room_insert
		BEFORE INSERT ON rooms
		FOR EACH ROW EXECUTE FUNCTION reconciliation_test_expire_claim();
	`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DROP TRIGGER IF EXISTS reconciliation_test_expire_claim_before_room_insert ON rooms`)
		_, _ = pool.Exec(context.Background(), `DROP FUNCTION IF EXISTS reconciliation_test_expire_claim()`)
	})

	if err := sessions.ReconcileClaimedMarker(ctx, gi, nil, markers[0], "worker-a"); err == nil {
		t.Fatal("expired lease must reject terminal completion")
	}
	if _, ok := sessions.Get(ctx, "room_lease_expires_during_repair"); ok {
		t.Fatal("expired terminal fence must roll back the repaired room")
	}
	var status string
	if err := pool.QueryRow(ctx, `
		SELECT status FROM pending_integrity_reconciliations
		WHERE command_id = 'lease_expires_during_repair'
	`).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "pending" {
		t.Fatalf("marker status=%q want pending", status)
	}
	var reconciledEvents int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM integration_outbox_events WHERE event_type = $1
	`, app.EventRoomStateReconciled).Scan(&reconciledEvents); err != nil {
		t.Fatal(err)
	}
	if reconciledEvents != 0 {
		t.Fatalf("expired terminal fence committed reconciliation side effects=%d", reconciledEvents)
	}
}

func TestIntegration_ServiceRestartPersistence(t *testing.T) {
	dsn := integrationDSN(t)
	pool1, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	resetPublic(t, pool1)
	applyMigration(t, pool1)
	sessions1 := store.NewSessionStore(pool1)
	gi := app.NewFakeGameIntegrity()
	svc := app.NewService(app.ServiceDeps{
		Sessions: sessions1, Commands: sessions1, Integrity: gi,
		Publisher: app.NewFakeEventPublisher(), Audit: app.NewFakeAuditSink(),
		Deals: app.NewFakeDealSource(), Clock: app.SystemClock{}, SessionsV: app.AllowAllSessionValidator{},
	})
	res := svc.HandleCommand(context.Background(), app.CommandInput{
		CommandID: "persist1", Type: app.CmdCreateRoom, SchemaVersion: 1,
		PlayerID: "host", SessionID: "sess", RoomID: "room_persist",
		Payload: mustJSON(map[string]any{"roomId": "room_persist", "maxSeats": 3}),
	})
	if res.Err != nil {
		t.Fatal(res.Err)
	}
	pool1.Close()

	pool2, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool2.Close)
	sessions2 := store.NewSessionStore(pool2)
	got, ok := sessions2.Get(context.Background(), "room_persist")
	if !ok {
		t.Fatal("room missing after reopen")
	}
	if got.Room().HostID() != "host" || got.Room().Roster().Capacity() != 3 {
		t.Fatalf("persist mismatch: host=%s cap=%d", got.Room().HostID(), got.Room().Roster().Capacity())
	}
}

func TestIntegration_DurableProvision(t *testing.T) {
	pool := openPool(t)
	sessions := store.NewSessionStore(pool)
	gi := app.NewFakeGameIntegrity()
	svc := app.NewService(app.ServiceDeps{
		Sessions: sessions, Commands: sessions, Integrity: gi,
		Publisher: app.NewFakeEventPublisher(), Audit: app.NewFakeAuditSink(),
		Deals: app.NewFakeDealSource(), Clock: app.SystemClock{}, SessionsV: app.AllowAllSessionValidator{},
	})
	res := svc.Provision(context.Background(), app.ProvisionInput{
		CommandID: "prov1", TournamentID: "t1", RoundNumber: 1, SlotID: "slot-a",
		RoomID: "room_prov", HostID: "host", Visibility: "private", MaxSeats: 2,
		PlayerIDs: []string{"host", "p2"},
	})
	if res.Err != nil || res.Result.Status != "accepted" {
		t.Fatalf("provision: %+v err=%v", res.Result, res.Err)
	}
	if gi.Len() != 1 {
		t.Fatalf("expected GI append on durable provision, got %d", gi.Len())
	}
	firstRoomID := provisionPayloadRoomID(t, res.Result.Payload)
	if firstRoomID != "room_prov" {
		t.Fatalf("durable first success roomId=%q want room_prov payload=%s", firstRoomID, res.Result.Payload)
	}
	rid, ok := sessions.GetProvision(context.Background(), app.ProvisionKey{
		TournamentID: "t1", RoundNumber: 1, SlotID: "slot-a",
	})
	if !ok || rid != "room_prov" {
		t.Fatalf("provision key missing: %q ok=%v", rid, ok)
	}
	res2 := svc.Provision(context.Background(), app.ProvisionInput{
		CommandID: "prov2", TournamentID: "t1", RoundNumber: 1, SlotID: "slot-a",
		RoomID: "room_prov_other", HostID: "host", MaxSeats: 2,
	})
	if res2.Err != nil {
		t.Fatal(res2.Err)
	}
	if gi.Len() != 1 {
		t.Fatalf("duplicate provision must not re-append GI: %d", gi.Len())
	}
	dupRoomID := provisionPayloadRoomID(t, res2.Result.Payload)
	if dupRoomID != "room_prov" {
		t.Fatalf("durable duplicate roomId=%q want room_prov", dupRoomID)
	}
}

func provisionPayloadRoomID(t *testing.T, raw json.RawMessage) string {
	t.Helper()
	var payload map[string]any
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatalf("payload: %v", err)
	}
	roomID, _ := payload["roomId"].(string)
	if roomID == "" {
		t.Fatalf("missing roomId in %s", raw)
	}
	return roomID
}

func TestIntegration_PendingIntentBlocksSecondAppend(t *testing.T) {
	pool := openPool(t)
	sessions := store.NewSessionStore(pool)
	sessions.FailNextFinalizeIntent = fmt.Errorf("finalize boom")
	gi := app.NewFakeGameIntegrity()
	svc := app.NewService(app.ServiceDeps{
		Sessions: sessions, Commands: sessions, Integrity: gi,
		Publisher: app.NewFakeEventPublisher(), Audit: app.NewFakeAuditSink(),
		Deals: app.NewFakeDealSource(), Clock: app.SystemClock{}, SessionsV: app.AllowAllSessionValidator{},
	})
	res := svc.HandleCommand(context.Background(), app.CommandInput{
		CommandID: "pend_block", Type: app.CmdCreateRoom, SchemaVersion: 1,
		PlayerID: "host", SessionID: "sess", RoomID: "room_pend_block",
		Payload: mustJSON(map[string]any{"roomId": "room_pend_block", "maxSeats": 2}),
	})
	if res.Err == nil {
		t.Fatal("expected finalize failure")
	}
	if gi.Len() != 1 {
		t.Fatalf("appends=%d", gi.Len())
	}
	retry := svc.HandleCommand(context.Background(), app.CommandInput{
		CommandID: "pend_block", Type: app.CmdCreateRoom, SchemaVersion: 1,
		PlayerID: "host", SessionID: "sess", RoomID: "room_pend_block",
		Payload: mustJSON(map[string]any{"roomId": "room_pend_block", "maxSeats": 2}),
	})
	if retry.Err == nil || !errors.Is(retry.Err, app.ErrReconciliationPending) {
		t.Fatalf("want ErrReconciliationPending, got %v", retry.Err)
	}
	if !errors.Is(retry.Err, app.ErrRetryableUnavailable) {
		t.Fatalf("want retryable unavailable, got %v", retry.Err)
	}
	if gi.Len() != 1 {
		t.Fatalf("second Append forbidden, got %d", gi.Len())
	}
}

func TestIntegration_ConcurrentSameCommandOneAppend(t *testing.T) {
	dsn := integrationDSN(t)
	ctx := context.Background()

	const mainMaxConns = int32(4)
	const n = 8 // more concurrent commands than main MaxConns
	if n <= int(mainMaxConns) {
		t.Fatal("test must stress more concurrent commands than main MaxConns")
	}

	mainCfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	mainCfg.MaxConns = mainMaxConns
	mainPool, err := pgxpool.NewWithConfig(ctx, mainCfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(mainPool.Close)

	intentCfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	intentCfg.MaxConns = store.DefaultIntentPoolMaxConns
	intentCfg.MinConns = 1
	intentPool, err := pgxpool.NewWithConfig(ctx, intentCfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(intentPool.Close)

	resetPublic(t, mainPool)
	applyMigration(t, mainPool)

	sessions := store.NewSessionStoreWithPools(mainPool, intentPool)
	gi := app.NewFakeGameIntegrity()
	svc := app.NewService(app.ServiceDeps{
		Sessions: sessions, Commands: sessions, Integrity: gi,
		Publisher: app.NewFakeEventPublisher(), Audit: app.NewFakeAuditSink(),
		Deals: app.NewFakeDealSource(), Clock: app.SystemClock{}, SessionsV: app.AllowAllSessionValidator{},
	})

	barrier := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(n)
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		i := i
		go func() {
			defer wg.Done()
			<-barrier
			cmdCtx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
			defer cancel()
			res := svc.HandleCommand(cmdCtx, app.CommandInput{
				CommandID: "race_cmd", Type: app.CmdCreateRoom, SchemaVersion: 1,
				PlayerID: "host", SessionID: "sess", RoomID: "room_race",
				Payload: mustJSON(map[string]any{"roomId": "room_race", "maxSeats": 2}),
			})
			errs[i] = res.Err
		}()
	}
	close(barrier)

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(20 * time.Second):
		t.Fatal("concurrent same-command timed out (likely main-pool saturation deadlock)")
	}

	if gi.Len() > 1 {
		t.Fatalf("GI append count must be <=1, got %d", gi.Len())
	}
	if gi.Len() != 1 {
		t.Fatalf("expected exactly 1 GI append, got %d", gi.Len())
	}

	// Connections must return to idle; no leaked idle-in-transaction sessions.
	deadline := time.Now().Add(3 * time.Second)
	for {
		mainStat := mainPool.Stat()
		intentStat := intentPool.Stat()
		if mainStat.AcquiredConns() == 0 && intentStat.AcquiredConns() == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("leaked acquired connections: main=%d intent=%d (idle main=%d intent=%d)",
				mainStat.AcquiredConns(), intentStat.AcquiredConns(),
				mainStat.IdleConns(), intentStat.IdleConns())
		}
		time.Sleep(20 * time.Millisecond)
	}

	var pendingHits int
	for _, err := range errs {
		if errors.Is(err, app.ErrReconciliationPending) {
			pendingHits++
		}
	}
	_ = pendingHits // at least one path may be pending or idempotent replay
}

func TestIntegration_CancelledIntentMayRestart(t *testing.T) {
	pool := openPool(t)
	sessions := store.NewSessionStore(pool)
	gi := app.NewFakeGameIntegrity()
	gi.FailNext = fmt.Errorf("%w: 409 conflict", app.ErrIntegrityAppendDefinitive)
	svc := app.NewService(app.ServiceDeps{
		Sessions: sessions, Commands: sessions, Integrity: gi,
		Publisher: app.NewFakeEventPublisher(), Audit: app.NewFakeAuditSink(),
		Deals: app.NewFakeDealSource(), Clock: app.SystemClock{}, SessionsV: app.AllowAllSessionValidator{},
	})
	res := svc.HandleCommand(context.Background(), app.CommandInput{
		CommandID: "cancel_restart", Type: app.CmdCreateRoom, SchemaVersion: 1,
		PlayerID: "host", SessionID: "sess", RoomID: "room_cancel_restart",
		Payload: mustJSON(map[string]any{"roomId": "room_cancel_restart", "maxSeats": 2}),
	})
	if res.Err == nil {
		t.Fatal("expected definitive append failure")
	}
	var status string
	if err := pool.QueryRow(context.Background(), `
		SELECT status FROM pending_integrity_reconciliations WHERE command_id = 'cancel_restart'
	`).Scan(&status); err != nil || status != "cancelled" {
		t.Fatalf("status=%q err=%v", status, err)
	}
	retry := svc.HandleCommand(context.Background(), app.CommandInput{
		CommandID: "cancel_restart", Type: app.CmdCreateRoom, SchemaVersion: 1,
		PlayerID: "host", SessionID: "sess", RoomID: "room_cancel_restart",
		Payload: mustJSON(map[string]any{"roomId": "room_cancel_restart", "maxSeats": 2}),
	})
	if retry.Err != nil {
		t.Fatal(retry.Err)
	}
	if gi.Len() != 1 {
		t.Fatalf("restart must append once after cancel, got %d", gi.Len())
	}
}

func TestIntegration_DoneIntentFailClosed(t *testing.T) {
	pool := openPool(t)
	sessions := store.NewSessionStore(pool)
	gi := app.NewFakeGameIntegrity()
	svc := app.NewService(app.ServiceDeps{
		Sessions: sessions, Commands: sessions, Integrity: gi,
		Publisher: app.NewFakeEventPublisher(), Audit: app.NewFakeAuditSink(),
		Deals: app.NewFakeDealSource(), Clock: app.SystemClock{}, SessionsV: app.AllowAllSessionValidator{},
	})
	res := svc.HandleCommand(context.Background(), app.CommandInput{
		CommandID: "done_cmd", Type: app.CmdCreateRoom, SchemaVersion: 1,
		PlayerID: "host", SessionID: "sess", RoomID: "room_done",
		Payload: mustJSON(map[string]any{"roomId": "room_done", "maxSeats": 2}),
	})
	if res.Err != nil {
		t.Fatal(res.Err)
	}
	err := sessions.BeginReconciliationIntent(context.Background(), "done_cmd", "room_done", 0,
		[]byte(`{}`), app.DurableAcceptedCommit{
			Session:   domain.OpenSession(mustCreateRoom(t, "done_cmd2", "room_done2")),
			CommandID: "done_cmd", CommandType: app.CmdCreateRoom, CreatePath: true,
			Outcome: domain.CommandOutcome{Kind: domain.OutcomeAccepted, CommandID: "done_cmd"},
		})
	if !errors.Is(err, app.ErrReconciliationDone) {
		t.Fatalf("want ErrReconciliationDone, got %v", err)
	}
}

func TestIntegration_ExactEventIDMismatchLeavesPending(t *testing.T) {
	pool := openPool(t)
	sessions := store.NewSessionStore(pool)
	gi := app.NewFakeGameIntegrity()
	_, err := gi.Append(context.Background(), app.AppendRequest{
		RoomID: "room_mismatch", EventID: "other_cmd", ExpectedRevision: 0,
		EventType: app.CmdCreateRoom, Payload: []byte(`{"commandId":"other_cmd"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	room := mustCreateRoom(t, "want_cmd", "room_mismatch")
	if err := sessions.BeginReconciliationIntent(context.Background(), "want_cmd", "room_mismatch", 0,
		[]byte(`{"commandId":"want_cmd"}`), app.DurableAcceptedCommit{
			Session: domain.OpenSession(room), CommandID: "want_cmd", CommandType: app.CmdCreateRoom,
			CreatePath: true, Outcome: domain.CommandOutcome{Kind: domain.OutcomeAccepted, CommandID: "want_cmd"},
		}); err != nil {
		t.Fatal(err)
	}
	if err := sessions.FinalizeReconciliationIntent(context.Background(), "want_cmd", 0, 1); err != nil {
		t.Fatal(err)
	}
	_, err = sessions.ReconcilePending(context.Background(), gi, nil, 10)
	if err == nil {
		t.Fatal("eventId mismatch must fail closed")
	}
	markers, err := sessions.ListPendingReconciliationMarkers(context.Background(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(markers) != 1 {
		t.Fatalf("must remain pending: %+v", markers)
	}
}

func TestIntegration_BlankEventIDLeavesPending(t *testing.T) {
	pool := openPool(t)
	sessions := store.NewSessionStore(pool)
	gi := &blankEventGI{inner: app.NewFakeGameIntegrity()}
	_, _ = gi.inner.Append(context.Background(), app.AppendRequest{
		RoomID: "room_blank", EventID: "blank_cmd", ExpectedRevision: 0,
		EventType: app.CmdCreateRoom, Payload: []byte(`{}`),
	})
	room := mustCreateRoom(t, "blank_cmd", "room_blank")
	if err := sessions.BeginReconciliationIntent(context.Background(), "blank_cmd", "room_blank", 0,
		[]byte(`{}`), app.DurableAcceptedCommit{
			Session: domain.OpenSession(room), CommandID: "blank_cmd", CommandType: app.CmdCreateRoom,
			CreatePath: true, Outcome: domain.CommandOutcome{Kind: domain.OutcomeAccepted, CommandID: "blank_cmd"},
		}); err != nil {
		t.Fatal(err)
	}
	if err := sessions.FinalizeReconciliationIntent(context.Background(), "blank_cmd", 0, 1); err != nil {
		t.Fatal(err)
	}
	_, err := sessions.ReconcilePending(context.Background(), gi, nil, 10)
	if err == nil {
		t.Fatal("blank eventId must fail closed")
	}
}

type blankEventGI struct {
	inner *app.FakeGameIntegrity
}

func (b *blankEventGI) Append(ctx context.Context, req app.AppendRequest) (app.AppendResult, error) {
	return b.inner.Append(ctx, req)
}

func (b *blankEventGI) Replay(ctx context.Context, roomID string, fromOffset int64) (app.ReplayResult, error) {
	res, err := b.inner.Replay(ctx, roomID, fromOffset)
	if err != nil {
		return res, err
	}
	for i := range res.Entries {
		res.Entries[i].EventID = ""
	}
	return res, nil
}

func TestIntegration_UncertainAppendLeavesPending(t *testing.T) {
	pool := openPool(t)
	sessions := store.NewSessionStore(pool)
	gi := app.NewFakeGameIntegrity()
	gi.FailNext = fmt.Errorf("transport boom") // not definitive
	svc := app.NewService(app.ServiceDeps{
		Sessions: sessions, Commands: sessions, Integrity: gi,
		Publisher: app.NewFakeEventPublisher(), Audit: app.NewFakeAuditSink(),
		Deals: app.NewFakeDealSource(), Clock: app.SystemClock{}, SessionsV: app.AllowAllSessionValidator{},
	})
	res := svc.HandleCommand(context.Background(), app.CommandInput{
		CommandID: "uncert1", Type: app.CmdCreateRoom, SchemaVersion: 1,
		PlayerID: "host", SessionID: "sess", RoomID: "room_uncert",
		Payload: mustJSON(map[string]any{"roomId": "room_uncert", "maxSeats": 2}),
	})
	if res.Err == nil {
		t.Fatal("expected append failure")
	}
	var status string
	if err := pool.QueryRow(context.Background(), `
		SELECT status FROM pending_integrity_reconciliations WHERE command_id = 'uncert1'
	`).Scan(&status); err != nil || status != "pending" {
		t.Fatalf("uncertain must leave pending, status=%q err=%v", status, err)
	}
}

func TestIntegration_ReservationActionRecoveredOnReconcile(t *testing.T) {
	pool := openPool(t)
	sessions := store.NewSessionStore(pool)
	deals := app.NewFakeDealSource()
	gi := app.NewFakeGameIntegrity()

	// Setup match via service, then force confirm failure after append.
	svc := app.NewService(app.ServiceDeps{
		Sessions: sessions, Commands: sessions, Integrity: gi,
		Publisher: app.NewFakeEventPublisher(), Audit: app.NewFakeAuditSink(),
		Deals: deals, Clock: app.SystemClock{}, SessionsV: app.AllowAllSessionValidator{},
	})
	mustAcceptCmd(t, svc, app.CommandInput{
		CommandID: "c", Type: app.CmdCreateRoom, SchemaVersion: 1,
		PlayerID: "host", SessionID: "s", RoomID: "room_res_rec",
		Payload: mustJSON(map[string]any{"roomId": "room_res_rec", "maxSeats": 2}),
	})
	mustAcceptCmd(t, svc, app.CommandInput{
		CommandID: "j", Type: app.CmdJoinRoom, SchemaVersion: 1,
		PlayerID: "guest", SessionID: "s2", RoomID: "room_res_rec",
		ExpectedSequenceNumber: int64Ptr(1),
	})
	mustAcceptCmd(t, svc, app.CommandInput{
		CommandID: "l", Type: app.CmdLockRoom, SchemaVersion: 1,
		PlayerID: "host", SessionID: "s", RoomID: "room_res_rec",
		ExpectedSequenceNumber: int64Ptr(2),
	})
	mustAcceptCmd(t, svc, app.CommandInput{
		CommandID: "st", Type: app.CmdStartMatch, SchemaVersion: 1,
		PlayerID: "host", SessionID: "s", RoomID: "room_res_rec",
		ExpectedSequenceNumber: int64Ptr(3),
		Payload:                mustJSON(map[string]any{"gameId": "g1"}),
	})

	deals.FailConfirm = fmt.Errorf("confirm down")
	seq := int64(0)
	if live, ok := sessions.Get(context.Background(), domain.RoomID("room_res_rec")); ok {
		seq = int64(live.Room().Sequence())
	}
	res := svc.HandleCommand(context.Background(), app.CommandInput{
		CommandID: "draw_rec", Type: app.CmdDrawCard, SchemaVersion: 1,
		PlayerID: "host", SessionID: "s", RoomID: "room_res_rec",
		ExpectedSequenceNumber: &seq,
	})
	if res.Err == nil {
		t.Fatal("confirm failure expected")
	}
	markers, err := sessions.ListPendingReconciliationMarkers(context.Background(), 10)
	if err != nil || len(markers) != 1 {
		t.Fatalf("pending markers=%+v err=%v", markers, err)
	}
	var blob app.ReconciliationRepairBlob
	if err := json.Unmarshal(markers[0].Payload, &blob); err != nil {
		t.Fatal(err)
	}
	if blob.ReservationID == "" || blob.ReservationAction != app.ReservationActionConfirm {
		t.Fatalf("blob reservation: %+v", blob)
	}
	if deals.PendingLen() != 1 {
		t.Fatalf("pending reservations=%d", deals.PendingLen())
	}

	deals.FailConfirm = nil
	confirmsBefore := deals.ConfirmCalls
	n, err := sessions.ReconcilePending(context.Background(), gi, deals, 10)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("reconciled=%d", n)
	}
	if deals.ConfirmCalls != confirmsBefore+1 {
		t.Fatalf("confirm calls=%d want %d", deals.ConfirmCalls, confirmsBefore+1)
	}
	if deals.PendingLen() != 0 {
		t.Fatal("reservation must be confirmed by reconciler")
	}
	n2, err := sessions.ReconcilePending(context.Background(), gi, deals, 10)
	if err != nil {
		t.Fatal(err)
	}
	if n2 != 0 {
		t.Fatalf("second reconcile must be no-op, got %d", n2)
	}
	if deals.ConfirmCalls != confirmsBefore+1 {
		t.Fatalf("confirm must run exactly once, calls=%d", deals.ConfirmCalls)
	}
}

func TestIntegration_ReservationCancelActionOnNoop(t *testing.T) {
	pool := openPool(t)
	sessions := store.NewSessionStore(pool)
	deals := app.NewFakeDealSource()
	res, err := deals.ReserveDraw(context.Background(), "room_noop", "g1", "noop_cmd:draw", 1)
	if err != nil {
		t.Fatal(err)
	}
	room := mustCreateRoom(t, "noop_cmd", "room_noop")
	giPayload := []byte(`{"commandId":"noop_cmd"}`)
	blobCommit := app.DurableAcceptedCommit{
		Session: domain.OpenSession(room), CommandID: "noop_cmd", CommandType: app.CmdDrawCard,
		CreatePath: true, Outcome: domain.CommandOutcome{Kind: domain.OutcomeAccepted, CommandID: "noop_cmd"},
		ReservationID: res.ID, ReservationRoomID: "room_noop", ReservationGameID: "g1",
		ReservationAction: app.ReservationActionCancel,
	}
	if err := sessions.BeginReconciliationIntent(context.Background(), "noop_cmd", "room_noop", 0,
		giPayload, blobCommit); err != nil {
		t.Fatal(err)
	}
	gi := app.NewFakeGameIntegrity()
	_, err = gi.Append(context.Background(), app.AppendRequest{
		RoomID: "room_noop", EventID: "noop_cmd", ExpectedRevision: 0,
		EventType: app.CmdDrawCard, Payload: giPayload,
	})
	if err != nil {
		t.Fatal(err)
	}
	if deals.PendingLen() != 1 {
		t.Fatalf("pending=%d", deals.PendingLen())
	}
	n, err := sessions.ReconcilePending(context.Background(), gi, deals, 10)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("n=%d", n)
	}
	if deals.PendingLen() != 0 {
		t.Fatal("cancel action must release reservation")
	}
}

func mustAcceptCmd(t *testing.T, svc *app.Service, in app.CommandInput) {
	t.Helper()
	res := svc.HandleCommand(context.Background(), in)
	if res.Err != nil {
		t.Fatal(res.Err)
	}
	if res.Result.Status != "accepted" {
		t.Fatalf("%+v", res.Result)
	}
}

func TestIntegration_AnalyticsBackfill_KeysetReadOnly(t *testing.T) {
	restore := app.SetAnalyticsBackfillCursorMACKeyForTest("pg-analytics-backfill-cursor")
	t.Cleanup(restore)

	pool := openPool(t)
	ctx := context.Background()
	at := time.Date(2026, 7, 2, 10, 0, 0, 0, time.UTC)
	var ids []int64
	for i := 0; i < 5; i++ {
		ts := at.Add(time.Duration(i) * time.Minute)
		payload, _ := json.Marshal(map[string]any{
			"eventId": fmt.Sprintf("pg-%d", i), "eventType": "GameplayMetric", "schemaVersion": 1,
			"correlationId": "c", "occurredAt": ts.UTC().Format(time.RFC3339Nano),
			"roomId": "room_pg", "visibility": "anonymized_adhoc", "metricType": "play",
		})
		var id int64
		err := pool.QueryRow(ctx, `
			INSERT INTO integration_outbox_events (
				event_id, event_type, topic, partition_key, schema_version, room_id,
				payload, correlation_id, occurred_at
			) VALUES ($1,$2,$3,$4,1,$5,$6,$7,$8)
			RETURNING outbox_id
		`, fmt.Sprintf("pg-eid-%d", i), "GameplayMetric", "room.gameplay.metrics", "room_pg",
			"room_pg", payload, "c", ts).Scan(&id)
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	// Noise topic must not leak into room.gameplay.metrics pages.
	_, err := pool.Exec(ctx, `
		INSERT INTO integration_outbox_events (
			event_id, event_type, topic, partition_key, schema_version, room_id, payload
		) VALUES ('noise','MatchCompleted','room.match.completed','room_pg',1,'room_pg','{"eventId":"noise"}')
	`)
	if err != nil {
		t.Fatal(err)
	}

	reader := store.NewAnalyticsBackfillStore(pool)
	svc := app.NewService(app.ServiceDeps{
		Sessions: app.NewMemorySessionRepository(), Integrity: app.NewFakeGameIntegrity(),
		Publisher: app.NewFakeEventPublisher(), Audit: app.NewFakeAuditSink(),
		Deals: app.NewFakeDealSource(), Clock: app.SystemClock{}, SessionsV: app.AllowAllSessionValidator{},
	})
	svc.SetAnalyticsBackfillReader(reader)

	fromCP, toCP := fmt.Sprintf("%d", ids[0]), fmt.Sprintf("%d", ids[len(ids)-1])
	page1, err := svc.AnalyticsBackfill(ctx, app.AnalyticsBackfillRequest{
		RecoveryJobID: "pg-job", SourceTopic: "room.gameplay.metrics", SchemaVersion: 1,
		FromCheckpoint: fromCP, ToCheckpoint: toCP, Limit: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(page1.Records) != 2 || page1.NextCursor == "" {
		t.Fatalf("page1=%+v", page1)
	}
	page2, err := svc.AnalyticsBackfill(ctx, app.AnalyticsBackfillRequest{
		RecoveryJobID: "pg-job", SourceTopic: "room.gameplay.metrics", SchemaVersion: 1,
		FromCheckpoint: fromCP, ToCheckpoint: toCP, Limit: 2, Cursor: page1.NextCursor,
	})
	if err != nil {
		t.Fatal(err)
	}
	page3, err := svc.AnalyticsBackfill(ctx, app.AnalyticsBackfillRequest{
		RecoveryJobID: "pg-job", SourceTopic: "room.gameplay.metrics", SchemaVersion: 1,
		FromCheckpoint: fromCP, ToCheckpoint: toCP, Limit: 2, Cursor: page2.NextCursor,
	})
	if err != nil {
		t.Fatal(err)
	}
	total := len(page1.Records) + len(page2.Records) + len(page3.Records)
	if total != 5 || page3.NextCursor != "" {
		t.Fatalf("total=%d next3=%q", total, page3.NextCursor)
	}

	var countBefore, countAfter int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM integration_outbox_events`).Scan(&countBefore)
	_, _ = svc.AnalyticsBackfill(ctx, app.AnalyticsBackfillRequest{
		RecoveryJobID: "pg-job", SourceTopic: "room.gameplay.metrics", SchemaVersion: 1,
		FromCheckpoint: fromCP, ToCheckpoint: toCP, Limit: 100,
	})
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM integration_outbox_events`).Scan(&countAfter)
	if countBefore != countAfter {
		t.Fatalf("outbox mutated: %d -> %d", countBefore, countAfter)
	}

	var idx int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM pg_indexes
		WHERE tablename = 'integration_outbox_events'
		  AND indexname = 'integration_outbox_events_topic_outbox_idx'
	`).Scan(&idx); err != nil || idx != 1 {
		t.Fatalf("baseline topic/outbox index missing: idx=%d err=%v", idx, err)
	}
}

func TestPostgresPublicList_KeysetPrivacyAndIndex(t *testing.T) {
	pool := openPool(t)
	ctx := context.Background()
	restore := app.SetPublicListCursorMACKeyForTest("pg-public-list-cursor")
	defer restore()

	sessions := store.NewSessionStore(pool)
	svc := app.NewService(app.ServiceDeps{
		Sessions: sessions, Commands: sessions,
		Integrity: app.NewFakeGameIntegrity(), Publisher: app.NewFakeEventPublisher(),
		Audit: app.NewFakeAuditSink(), Deals: app.NewFakeDealSource(),
		Clock: app.SystemClock{}, SessionsV: app.AllowAllSessionValidator{},
	})
	svc.SetPublicListReader(sessions)

	for _, tc := range []struct {
		id, host string
		vis      domain.Visibility
	}{
		{"room_a", "h1", domain.VisibilityPublic},
		{"room_b", "h2", domain.VisibilityPublic},
		{"room_c", "h3", domain.VisibilityPublic},
		{"room_priv", "hp", domain.VisibilityPrivate},
	} {
		room, out := domain.CreateRoom(domain.CreateRoomCommand{
			CommandID: domain.CommandID("cmd_" + tc.id), RoomID: domain.RoomID(tc.id),
			HostID: domain.PlayerID(tc.host), Visibility: tc.vis, MaxSeats: 4,
		})
		if out.Rejection != nil {
			t.Fatal(out.Rejection)
		}
		uow, err := sessions.BeginCreate(ctx, domain.RoomID(tc.id))
		if err != nil {
			t.Fatal(err)
		}
		if err := uow.CommitAccepted(app.DurableAcceptedCommit{
			Session: domain.OpenSession(room), CommandID: "cmd_" + tc.id, CommandType: "CreateRoom",
			Outcome: out, CreatePath: true, IntegrityRevision: 1, SetIntegrityRevision: true, LogOffset: 1,
		}); err != nil {
			t.Fatal(err)
		}
	}

	page1, err := svc.PublicList(ctx, app.PublicListQuery{Status: "waiting", Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(page1.Rooms) != 2 || page1.NextCursor == "" {
		t.Fatalf("page1=%+v", page1)
	}
	for _, r := range page1.Rooms {
		if r.Visibility != "public" || r.RoomID == "room_priv" {
			t.Fatalf("privacy %#v", r)
		}
	}
	page2, err := svc.PublicList(ctx, app.PublicListQuery{Status: "waiting", Limit: 2, Cursor: page1.NextCursor})
	if err != nil {
		t.Fatal(err)
	}
	if len(page2.Rooms) != 1 || page2.NextCursor != "" {
		t.Fatalf("page2=%+v", page2)
	}

	var idx int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM pg_indexes
		WHERE tablename = 'rooms' AND indexname = 'rooms_public_list_idx'
	`).Scan(&idx); err != nil || idx != 1 {
		t.Fatalf("rooms_public_list_idx missing: idx=%d err=%v", idx, err)
	}
}

func int64Ptr(v int64) *int64 { return &v }

func mustJSON(v any) []byte {
	b, _ := json.Marshal(v)
	return b
}
