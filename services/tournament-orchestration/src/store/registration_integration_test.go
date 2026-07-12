//go:build integration

package store_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"unoarena/services/tournament-orchestration/domain"
	"unoarena/services/tournament-orchestration/store"
	"unoarena/shared/envelope"
)

func TestIntegration_TReg_SamePlayerConcurrent(t *testing.T) {
	pool, ts := openStore(t)
	ctx := context.Background()
	tid := "treg-same-player"
	createTournamentTReg(t, ts, tid, 100)

	const n = 50
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			cmdID := fmt.Sprintf("cmd-%d", i)
			uow, err := ts.BeginRegisterPlayer(ctx, tid, "player-1", cmdID)
			if err != nil {
				t.Errorf("begin: %v", err)
				return
			}
			defer func() { _ = uow.Rollback() }()
			if prior, ok := uow.LookupOutcome(cmdID); ok {
				_ = prior
				return
			}
			decision := domain.DecideRegisterPlayer(uow.RegisterContext(), domain.RegisterPlayerCommand{
				CommandID: domain.CommandID(cmdID), PlayerID: "player-1",
			})
			if decision.Kind == domain.RegistrationReserve {
				_, autoClosed, err := uow.ReserveRegistration()
				_ = autoClosed
				if store.IsRegistrationAlreadyPresent(err) {
					decision = domain.DecideRegisterPlayer(domain.RegistrationContext{
						TournamentID: domain.TournamentID(tid), Exists: true,
						Phase: domain.PhaseRegistration, PlayerRegistered: true,
					}, domain.RegisterPlayerCommand{CommandID: domain.CommandID(cmdID), PlayerID: "player-1"})
				} else if store.IsRegistrationCapacityExceeded(err) {
					t.Errorf("unexpected capacity: %v", err)
					return
				} else if err != nil {
					t.Errorf("reserve: %v", err)
					return
				} else {
					decision = domain.RegistrationDecision{
						Kind: domain.RegistrationReserve,
						Outcome: domain.AcceptedWithFacts(domain.CommandID(cmdID), []domain.Fact{
							domain.PlayerRegisteredFact(domain.TournamentID(tid), "player-1"),
						}),
					}
				}
			}
			res := envelope.Accepted(cmdID, "RegisterPlayer", nil, json.RawMessage(`{"facts":[]}`))
			if decision.Outcome.Rejected() {
				res = envelope.Rejected(cmdID, "RegisterPlayer", string(decision.Outcome.Rejection.Code), nil)
			} else if len(decision.Outcome.Facts) > 0 {
				payload, _ := json.Marshal(map[string]any{"facts": decision.Outcome.Facts})
				res = envelope.Accepted(cmdID, "RegisterPlayer", nil, payload)
			}
			if err := uow.FinalizeRegister(store.RegistrationCommitRequest{
				Op: store.RegistrationOpRegister, TournamentID: tid,
				CommandID: cmdID, CommandType: "RegisterPlayer", Outcome: res, Decision: decision,
			}); err != nil {
				if _, ok := store.AsPriorCommandOutcome(err); !ok {
					t.Errorf("finalize: %v", err)
				}
			}
		}(i)
	}
	wg.Wait()

	var rows, sum int
	if err := pool.QueryRow(ctx, `SELECT count(*)::int FROM tournament_registrations WHERE tournament_id=$1`, tid).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT COALESCE(SUM(count),0)::int FROM tournament_registration_shards WHERE tournament_id=$1`, tid).Scan(&sum); err != nil {
		t.Fatal(err)
	}
	if rows != 1 || sum != 1 {
		t.Fatalf("want 1 row and SUM=1, got rows=%d sum=%d", rows, sum)
	}
}

func TestIntegration_TReg_ExactCapacityConcurrent(t *testing.T) {
	for _, capacity := range []int{3, 63, 65} {
		t.Run(fmt.Sprintf("cap%d", capacity), func(t *testing.T) {
			pool, ts := openStore(t)
			ctx := context.Background()
			tid := fmt.Sprintf("treg-cap-%d", capacity)
			createTournamentTReg(t, ts, tid, capacity)

			var baseBefore int64
			_ = pool.QueryRow(ctx, `SELECT projection_version FROM bracket_projection_versions WHERE tournament_id=$1`, tid).Scan(&baseBefore)

			var wg sync.WaitGroup
			n := capacity + 10
			wg.Add(n)
			var accepted atomic.Int32
			for i := 0; i < n; i++ {
				go func(i int) {
					defer wg.Done()
					pid := fmt.Sprintf("p-%d", i)
					cmdID := fmt.Sprintf("c-%d", i)
					uow, err := ts.BeginRegisterPlayer(ctx, tid, pid, cmdID)
					if err != nil {
						t.Errorf("begin: %v", err)
						return
					}
					defer func() { _ = uow.Rollback() }()
					if prior, ok := uow.LookupOutcome(cmdID); ok {
						_ = prior
						return
					}
					decision := domain.DecideRegisterPlayer(uow.RegisterContext(), domain.RegisterPlayerCommand{
						CommandID: domain.CommandID(cmdID), PlayerID: domain.PlayerID(pid),
					})
					autoClosed := false
					if decision.Kind == domain.RegistrationReserve {
						var err error
						_, autoClosed, err = uow.ReserveRegistration()
						if store.IsRegistrationCapacityExceeded(err) {
							decision = domain.RegistrationDecision{
								Kind: domain.RegistrationReject, Outcome: domain.CapacityExceededOutcome(domain.CommandID(cmdID)),
							}
						} else if err != nil {
							t.Errorf("reserve: %v", err)
							return
						} else {
							accepted.Add(1)
							facts := []domain.Fact{domain.PlayerRegisteredFact(domain.TournamentID(tid), domain.PlayerID(pid))}
							if autoClosed {
								facts = append(facts, domain.RegistrationClosedFact(domain.TournamentID(tid), capacity))
							}
							decision = domain.RegistrationDecision{
								Kind:    domain.RegistrationReserve,
								Outcome: domain.AcceptedWithFacts(domain.CommandID(cmdID), facts),
							}
						}
					}
					var res envelope.Result
					if decision.Outcome.Rejected() {
						res = envelope.Rejected(cmdID, "RegisterPlayer", string(decision.Outcome.Rejection.Code), nil)
					} else {
						payload, _ := json.Marshal(map[string]any{"facts": decision.Outcome.Facts})
						res = envelope.Accepted(cmdID, "RegisterPlayer", nil, payload)
					}
					if err := uow.FinalizeRegister(store.RegistrationCommitRequest{
						Op: store.RegistrationOpRegister, TournamentID: tid,
						CommandID: cmdID, CommandType: "RegisterPlayer", Outcome: res, Decision: decision,
					}); err != nil {
						if _, ok := store.AsPriorCommandOutcome(err); !ok {
							t.Errorf("finalize: %v", err)
						}
					}
				}(i)
			}
			wg.Wait()

			var sum, rows int
			if err := pool.QueryRow(ctx, `SELECT COALESCE(SUM(count),0)::int FROM tournament_registration_shards WHERE tournament_id=$1`, tid).Scan(&sum); err != nil {
				t.Fatal(err)
			}
			if err := pool.QueryRow(ctx, `SELECT count(*)::int FROM tournament_registrations WHERE tournament_id=$1 AND status='registered'`, tid).Scan(&rows); err != nil {
				t.Fatal(err)
			}
			if sum != capacity || rows != capacity {
				t.Fatalf("want exact capacity=%d, got sum=%d rows=%d accepted=%d", capacity, sum, rows, accepted.Load())
			}
			var phase string
			var regCount int
			if err := pool.QueryRow(ctx, `SELECT phase, registered_count FROM tournaments WHERE tournament_id=$1`, tid).Scan(&phase, &regCount); err != nil {
				t.Fatal(err)
			}
			if phase != string(domain.PhaseSeeding) {
				t.Fatalf("phase=%s want seeding", phase)
			}
			if regCount != capacity {
				t.Fatalf("registered_count=%d want %d", regCount, capacity)
			}
			closeFacts := countCloseFactsInOutcomes(t, pool, ctx, tid)
			if closeFacts != 1 {
				t.Fatalf("want exactly one TournamentRegistrationClosed fact across outcomes, got %d", closeFacts)
			}
			var baseAfter int64
			_ = pool.QueryRow(ctx, `SELECT projection_version FROM bracket_projection_versions WHERE tournament_id=$1`, tid).Scan(&baseAfter)
			if baseAfter != baseBefore+1 {
				t.Fatalf("base projection bump want +1 (%d→%d)", baseBefore, baseAfter)
			}

			// Overflow: capacity exceeded or wrong_phase; outcome stored.
			uow, err := ts.BeginRegisterPlayer(ctx, tid, "overflow-player", "overflow-cmd")
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = uow.Rollback() }()
			decision := domain.DecideRegisterPlayer(uow.RegisterContext(), domain.RegisterPlayerCommand{
				CommandID: "overflow-cmd", PlayerID: "overflow-player",
			})
			res := envelope.Rejected("overflow-cmd", "RegisterPlayer", string(domain.RejectWrongPhase), nil)
			if decision.Kind == domain.RegistrationReserve {
				_, _, err = uow.ReserveRegistration()
				if !store.IsRegistrationCapacityExceeded(err) {
					t.Fatalf("overflow reserve want capacity exceeded, got %v", err)
				}
				res = envelope.Rejected("overflow-cmd", "RegisterPlayer", string(domain.RejectCapacityExceeded), nil)
			} else if !decision.Outcome.Rejected() {
				t.Fatalf("overflow want reject, got %s", decision.Kind)
			} else {
				res = envelope.Rejected("overflow-cmd", "RegisterPlayer", string(decision.Outcome.Rejection.Code), nil)
			}
			if err := uow.FinalizeRegister(store.RegistrationCommitRequest{
				Op: store.RegistrationOpRegister, TournamentID: tid,
				CommandID: "overflow-cmd", CommandType: "RegisterPlayer", Outcome: res,
				Decision: domain.RegistrationDecision{Kind: domain.RegistrationReject},
			}); err != nil {
				t.Fatal(err)
			}
			prior, ok := ts.LookupOutcome(ctx, "overflow-cmd")
			if !ok || prior.Status != envelope.StatusRejected {
				t.Fatalf("overflow outcome not persisted: ok=%v status=%v", ok, prior.Status)
			}
		})
	}
}

func TestIntegration_TReg_DistinctShardsConcurrent(t *testing.T) {
	pool, ts := openStore(t)
	ctx := context.Background()
	tid := "treg-shards-concurrent"
	createTournamentTReg(t, ts, tid, 1000)

	// Hold lock on shard 0 by starting a tx that locks the shard row, while another
	// player whose start shard differs proceeds.
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock_shared(hashtextextended($1, 0))`, "tournament:rewrite:"+tid); err != nil {
		t.Fatal(err)
	}

	// Find two players on distinct start shards; hold the first player's start shard.
	var holdShard int
	foundHold := false
	otherPID := ""
	for i := 0; i < 2000; i++ {
		pid := fmt.Sprintf("other-%d", i)
		s := domain.RegistrationStartShard(tid, pid)
		if !foundHold {
			holdShard = s
			foundHold = true
			continue
		}
		if s != holdShard {
			otherPID = pid
			break
		}
	}
	if otherPID == "" {
		t.Fatal("could not find player with distinct start shard")
	}
	var locked int
	if err := tx.QueryRow(ctx, `
		SELECT count FROM tournament_registration_shards
		WHERE tournament_id=$1 AND shard_id=$2 FOR UPDATE
	`, tid, holdShard).Scan(&locked); err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() {
		cctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		uow, err := ts.BeginRegisterPlayer(cctx, tid, otherPID, "other-cmd")
		if err != nil {
			done <- err
			return
		}
		defer func() { _ = uow.Rollback() }()
		_, _, err = uow.ReserveRegistration()
		if err != nil {
			done <- err
			return
		}
		res := envelope.Accepted("other-cmd", "RegisterPlayer", nil, json.RawMessage(`{"facts":[]}`))
		done <- uow.FinalizeRegister(store.RegistrationCommitRequest{
			Op: store.RegistrationOpRegister, TournamentID: tid,
			CommandID: "other-cmd", CommandType: "RegisterPlayer", Outcome: res,
			Decision: domain.RegistrationDecision{Kind: domain.RegistrationReserve},
		})
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("other shard register failed under timeout: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("other shard register blocked by unrelated shard lock")
	}
	_ = tx.Rollback(ctx)
}

func TestIntegration_TReg_RegisterVsClose(t *testing.T) {
	_, ts := openStore(t)
	ctx := context.Background()
	tid := "treg-reg-vs-close"
	createTournamentTReg(t, ts, tid, 10)

	// Seed one registration so close is valid.
	registerOneTReg(t, ts, tid, "seed-p", "seed-cmd")

	t.Run("closeThenRegister", func(t *testing.T) {
		cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		uow, err := ts.BeginCloseRegistration(cctx, tid, "close-1")
		if err != nil {
			t.Fatal(err)
		}
		decision := domain.DecideCloseRegistration(uow.CloseContext(), domain.CloseRegistrationCommand{CommandID: "close-1"})
		res := envelope.Accepted("close-1", "CloseRegistration", nil, json.RawMessage(`{"facts":[]}`))
		if err := uow.Commit(store.RegistrationCommitRequest{
			Op: store.RegistrationOpClose, TournamentID: tid, CommandID: "close-1",
			CommandType: "CloseRegistration", Outcome: res, Decision: decision, BumpBaseProjection: true,
		}); err != nil {
			t.Fatal(err)
		}
		uow2, err := ts.BeginRegisterPlayer(cctx, tid, "late-p", "late-cmd")
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = uow2.Rollback() }()
		decision2 := domain.DecideRegisterPlayer(uow2.RegisterContext(), domain.RegisterPlayerCommand{
			CommandID: "late-cmd", PlayerID: "late-p",
		})
		if decision2.Kind != domain.RegistrationReject {
			t.Fatalf("want reject after close, got %s", decision2.Kind)
		}
	})
}

func TestIntegration_TReg_LastSlotAutoClose(t *testing.T) {
	pool, ts := openStore(t)
	ctx := context.Background()
	tid := "treg-autoclose"
	capacity := 2
	createTournamentTReg(t, ts, tid, capacity)
	registerOneTReg(t, ts, tid, "p0", "c0")

	uow, err := ts.BeginRegisterPlayer(ctx, tid, "p1", "c1")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = uow.Rollback() }()
	_, autoClosed, err := uow.ReserveRegistration()
	if err != nil {
		t.Fatal(err)
	}
	if !autoClosed {
		t.Fatal("expected auto-close on last slot")
	}
	facts := []domain.Fact{
		domain.PlayerRegisteredFact(domain.TournamentID(tid), "p1"),
		domain.RegistrationClosedFact(domain.TournamentID(tid), capacity),
	}
	payload, _ := json.Marshal(map[string]any{"facts": facts})
	res := envelope.Accepted("c1", "RegisterPlayer", nil, payload)
	if err := uow.FinalizeRegister(store.RegistrationCommitRequest{
		Op: store.RegistrationOpRegister, TournamentID: tid,
		CommandID: "c1", CommandType: "RegisterPlayer", Outcome: res,
		Decision: domain.RegistrationDecision{Kind: domain.RegistrationReserve, Outcome: domain.AcceptedWithFacts("c1", facts)},
	}); err != nil {
		t.Fatal(err)
	}

	var phase string
	var regCount int
	if err := pool.QueryRow(ctx, `SELECT phase, registered_count FROM tournaments WHERE tournament_id=$1`, tid).Scan(&phase, &regCount); err != nil {
		t.Fatal(err)
	}
	if phase != string(domain.PhaseSeeding) {
		t.Fatalf("phase=%s", phase)
	}
	if regCount != capacity {
		t.Fatalf("registered_count snapshot=%d want %d", regCount, capacity)
	}
	// Second close is noop.
	uow2, err := ts.BeginCloseRegistration(ctx, tid, "close-again")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = uow2.Rollback() }()
	d := domain.DecideCloseRegistration(uow2.CloseContext(), domain.CloseRegistrationCommand{CommandID: "close-again"})
	if d.Kind != domain.RegistrationCloseNoop {
		t.Fatalf("want close noop, got %s", d.Kind)
	}
}

func TestIntegration_TReg_ProjectionShardBumpAndBracketSum(t *testing.T) {
	pool, ts := openStore(t)
	ctx := context.Background()
	tid := "treg-proj"
	createTournamentTReg(t, ts, tid, 10)

	var baseBefore int64
	_ = pool.QueryRow(ctx, `SELECT projection_version FROM bracket_projection_versions WHERE tournament_id=$1`, tid).Scan(&baseBefore)

	registerOneTReg(t, ts, tid, "proj-p", "proj-cmd")

	var baseAfter int64
	_ = pool.QueryRow(ctx, `SELECT projection_version FROM bracket_projection_versions WHERE tournament_id=$1`, tid).Scan(&baseAfter)
	if baseAfter != baseBefore {
		t.Fatalf("base projection changed on register without auto-close: %d -> %d", baseBefore, baseAfter)
	}
	var shardSum int64
	_ = pool.QueryRow(ctx, `SELECT COALESCE(SUM(version),0) FROM bracket_projection_shards WHERE tournament_id=$1`, tid).Scan(&shardSum)
	if shardSum < 1 {
		t.Fatal("allocated projection shard not bumped")
	}

	page, err := ts.LoadBracketPage(ctx, store.BracketPageQuery{TournamentID: tid, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	var sum int
	_ = pool.QueryRow(ctx, `SELECT COALESCE(SUM(count),0)::int FROM tournament_registration_shards WHERE tournament_id=$1`, tid).Scan(&sum)
	if page.Summary.RegisteredCount != sum {
		t.Fatalf("bracket registeredCount=%d shardSum=%d", page.Summary.RegisteredCount, sum)
	}
}

func TestIntegration_TReg_LegacySeedPreservesShards(t *testing.T) {
	pool, ts := openStore(t)
	ctx := context.Background()
	tid := "treg-legacy-seed"
	// Capacity > registrations so auto-close does not fire before legacy close/seed.
	createTournamentTReg(t, ts, tid, 8)
	for i := 0; i < 4; i++ {
		registerOneTReg(t, ts, tid, fmt.Sprintf("lp-%d", i), fmt.Sprintf("lc-%d", i))
	}
	before := map[string]int{}
	rows, err := pool.Query(ctx, `SELECT player_id, shard_id FROM tournament_registrations WHERE tournament_id=$1`, tid)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var pid string
		var shard int
		if err := rows.Scan(&pid, &shard); err != nil {
			t.Fatal(err)
		}
		before[pid] = shard
	}
	rows.Close()

	// Close + seed via legacy persist path.
	uow, err := ts.BeginExisting(ctx, domain.TournamentID(tid))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = uow.Rollback() }()
	tr := uow.Loaded()
	_ = tr.CloseRegistration(domain.CloseRegistrationCommand{CommandID: "legacy-close"})
	_ = tr.SeedRound(domain.SeedRoundCommand{CommandID: "legacy-seed", RoundNumber: 1})
	if err := uow.Commit(store.CommitRequest{
		Tournament: tr, CommandID: "legacy-seed",
		Outcome:           envelope.Accepted("legacy-seed", "SeedRound", nil, json.RawMessage(`{}`)),
		ProjectionChanged: true,
	}); err != nil {
		t.Fatal(err)
	}

	after := map[string]int{}
	rows2, err := pool.Query(ctx, `SELECT player_id, shard_id FROM tournament_registrations WHERE tournament_id=$1`, tid)
	if err != nil {
		t.Fatal(err)
	}
	for rows2.Next() {
		var pid string
		var shard int
		if err := rows2.Scan(&pid, &shard); err != nil {
			t.Fatal(err)
		}
		after[pid] = shard
	}
	rows2.Close()
	for pid, shard := range before {
		if after[pid] != shard {
			t.Fatalf("player %s shard changed %d -> %d", pid, shard, after[pid])
		}
	}
	var sum int
	_ = pool.QueryRow(ctx, `SELECT COALESCE(SUM(count),0)::int FROM tournament_registration_shards WHERE tournament_id=$1`, tid).Scan(&sum)
	if sum != 4 {
		t.Fatalf("shard counts sum=%d", sum)
	}
}

func createTournamentTReg(t *testing.T, ts *store.TournamentStore, tid string, capacity int) {
	t.Helper()
	ctx := context.Background()
	cmdID := "create-" + tid
	uow, err := ts.BeginCreateTournament(ctx, tid, cmdID)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = uow.Rollback() }()
	cmd := domain.CreateTournamentCommand{
		CommandID: domain.CommandID(cmdID), TournamentID: domain.TournamentID(tid), Capacity: capacity,
	}
	decision := domain.DecideCreateTournament(cmd)
	res := envelope.Accepted(string(cmd.CommandID), "CreateTournament", nil, json.RawMessage(`{"facts":[]}`))
	if err := uow.Commit(store.RegistrationCommitRequest{
		Op: store.RegistrationOpCreate, TournamentID: tid, CommandID: string(cmd.CommandID),
		CommandType: "CreateTournament", Outcome: res, Decision: decision, CreateCmd: cmd,
		RetryBudget: 3, BatchSize: 100, BumpBaseProjection: true,
	}); err != nil {
		t.Fatal(err)
	}
}

func registerOneTReg(t *testing.T, ts *store.TournamentStore, tid, playerID, cmdID string) {
	t.Helper()
	ctx := context.Background()
	uow, err := ts.BeginRegisterPlayer(ctx, tid, playerID, cmdID)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = uow.Rollback() }()
	_, autoClosed, err := uow.ReserveRegistration()
	if err != nil {
		t.Fatal(err)
	}
	facts := []domain.Fact{domain.PlayerRegisteredFact(domain.TournamentID(tid), domain.PlayerID(playerID))}
	if autoClosed {
		facts = append(facts, domain.RegistrationClosedFact(domain.TournamentID(tid), uow.RegisterContext().Capacity))
	}
	payload, _ := json.Marshal(map[string]any{"facts": facts})
	res := envelope.Accepted(cmdID, "RegisterPlayer", nil, payload)
	if err := uow.FinalizeRegister(store.RegistrationCommitRequest{
		Op: store.RegistrationOpRegister, TournamentID: tid,
		CommandID: cmdID, CommandType: "RegisterPlayer", Outcome: res,
		Decision: domain.RegistrationDecision{Kind: domain.RegistrationReserve, Outcome: domain.AcceptedWithFacts(domain.CommandID(cmdID), facts)},
	}); err != nil {
		t.Fatal(err)
	}
}

func countCloseFactsInOutcomes(t *testing.T, pool *store.Pool, ctx context.Context, tid string) int {
	t.Helper()
	rows, err := pool.Query(ctx, `
		SELECT outcome_body FROM command_idempotency
		WHERE tournament_id = $1 AND command_type = 'RegisterPlayer'
	`, tid)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	n := 0
	for rows.Next() {
		var body []byte
		if err := rows.Scan(&body); err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(body), string(domain.FactTournamentRegistrationClosed)) {
			n++
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return n
}

func registerPlayerTRegOutcome(t *testing.T, ts *store.TournamentStore, tid, playerID, cmdID string) envelope.Result {
	t.Helper()
	ctx := context.Background()
	uow, err := ts.BeginRegisterPlayer(ctx, tid, playerID, cmdID)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = uow.Rollback() }()
	if prior, ok := uow.LookupOutcome(cmdID); ok {
		return prior
	}
	decision := domain.DecideRegisterPlayer(uow.RegisterContext(), domain.RegisterPlayerCommand{
		CommandID: domain.CommandID(cmdID), PlayerID: domain.PlayerID(playerID),
	})
	autoClosed := false
	if decision.Kind == domain.RegistrationReserve {
		var err error
		_, autoClosed, err = uow.ReserveRegistration()
		if store.IsRegistrationAlreadyPresent(err) {
			decision = domain.DecideRegisterPlayer(domain.RegistrationContext{
				TournamentID: domain.TournamentID(tid), Exists: true,
				Phase: domain.PhaseRegistration, PlayerRegistered: true,
			}, domain.RegisterPlayerCommand{CommandID: domain.CommandID(cmdID), PlayerID: domain.PlayerID(playerID)})
		} else if store.IsRegistrationCapacityExceeded(err) {
			decision = domain.RegistrationDecision{
				Kind: domain.RegistrationReject, Outcome: domain.CapacityExceededOutcome(domain.CommandID(cmdID)),
			}
		} else if err != nil {
			t.Fatal(err)
		} else {
			facts := []domain.Fact{domain.PlayerRegisteredFact(domain.TournamentID(tid), domain.PlayerID(playerID))}
			if autoClosed {
				facts = append(facts, domain.RegistrationClosedFact(domain.TournamentID(tid), uow.RegisterContext().Capacity))
			}
			decision = domain.RegistrationDecision{
				Kind: domain.RegistrationReserve, Outcome: domain.AcceptedWithFacts(domain.CommandID(cmdID), facts),
			}
		}
	}
	var res envelope.Result
	if decision.Outcome.Rejected() {
		res = envelope.Rejected(cmdID, "RegisterPlayer", string(decision.Outcome.Rejection.Code), nil)
	} else {
		payload, _ := json.Marshal(map[string]any{"facts": decision.Outcome.Facts})
		res = envelope.Accepted(cmdID, "RegisterPlayer", nil, payload)
	}
	if err := uow.FinalizeRegister(store.RegistrationCommitRequest{
		Op: store.RegistrationOpRegister, TournamentID: tid,
		CommandID: cmdID, CommandType: "RegisterPlayer", Outcome: res, Decision: decision,
	}); err != nil {
		if prior, ok := store.AsPriorCommandOutcome(err); ok {
			return prior
		}
		t.Fatal(err)
	}
	if stored, ok := ts.LookupOutcome(ctx, cmdID); ok {
		return stored
	}
	return res
}

func outcomesEqual(a, b envelope.Result) bool {
	if a.CommandID != b.CommandID || a.Type != b.Type || a.Status != b.Status || a.Reason != b.Reason {
		return false
	}
	if string(a.Payload) == string(b.Payload) {
		return true
	}
	// JSONB may reorder keys; compare decoded payloads.
	var pa, pb any
	if err := json.Unmarshal(a.Payload, &pa); err != nil {
		return false
	}
	if err := json.Unmarshal(b.Payload, &pb); err != nil {
		return false
	}
	ab, _ := json.Marshal(pa)
	bb, _ := json.Marshal(pb)
	return string(ab) == string(bb)
}

func TestIntegration_TReg_SameCommandIdDifferentPlayers(t *testing.T) {
	pool, ts := openStore(t)
	ctx := context.Background()
	tid := "treg-same-cmd-players"
	createTournamentTReg(t, ts, tid, 10)

	const cmdID = "shared-cmd-players"
	var wg sync.WaitGroup
	results := make([]envelope.Result, 2)
	errs := make([]error, 2)
	wg.Add(2)
	for i, pid := range []string{"alice", "bob"} {
		go func(i int, pid string) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					errs[i] = fmt.Errorf("panic: %v", r)
				}
			}()
			results[i] = registerPlayerTRegOutcome(t, ts, tid, pid, cmdID)
		}(i, pid)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("goroutine %d: %v", i, err)
		}
	}
	if !outcomesEqual(results[0], results[1]) {
		t.Fatalf("canonical mismatch:\n%#v\n%#v", results[0], results[1])
	}
	stored, ok := ts.LookupOutcome(ctx, cmdID)
	if !ok || !outcomesEqual(stored, results[0]) {
		t.Fatalf("stored outcome mismatch ok=%v", ok)
	}
	var rows int
	if err := pool.QueryRow(ctx, `SELECT count(*)::int FROM tournament_registrations WHERE tournament_id=$1`, tid).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 1 {
		t.Fatalf("want exactly one registration, got %d", rows)
	}
}

func TestIntegration_TReg_SameCommandIdDifferentTournaments(t *testing.T) {
	pool, ts := openStore(t)
	ctx := context.Background()
	createTournamentTReg(t, ts, "treg-cmd-t1", 10)
	createTournamentTReg(t, ts, "treg-cmd-t2", 10)

	const cmdID = "shared-cmd-cross-tid"
	var wg sync.WaitGroup
	results := make([]envelope.Result, 2)
	wg.Add(2)
	go func() {
		defer wg.Done()
		results[0] = registerPlayerTRegOutcome(t, ts, "treg-cmd-t1", "p1", cmdID)
	}()
	go func() {
		defer wg.Done()
		results[1] = registerPlayerTRegOutcome(t, ts, "treg-cmd-t2", "p2", cmdID)
	}()
	wg.Wait()
	if !outcomesEqual(results[0], results[1]) {
		t.Fatalf("canonical mismatch:\n%#v\n%#v", results[0], results[1])
	}
	var total int
	if err := pool.QueryRow(ctx, `
		SELECT count(*)::int FROM tournament_registrations
		WHERE tournament_id IN ('treg-cmd-t1','treg-cmd-t2')
	`).Scan(&total); err != nil {
		t.Fatal(err)
	}
	if total != 1 {
		t.Fatalf("want exactly one mutation globally, got %d registrations", total)
	}
}

func TestIntegration_TReg_SameCommandIdInvalidVsValid(t *testing.T) {
	pool, ts := openStore(t)
	ctx := context.Background()
	tid := "treg-invalid-vs-valid"
	createTournamentTReg(t, ts, tid, 10)

	const cmdID = "shared-cmd-invalid-valid"
	var wg sync.WaitGroup
	results := make([]envelope.Result, 2)
	wg.Add(2)
	go func() {
		defer wg.Done()
		uow, err := ts.BeginStandaloneCommand(ctx, cmdID)
		if err != nil {
			t.Errorf("standalone begin: %v", err)
			return
		}
		defer func() { _ = uow.Rollback() }()
		if prior, ok := uow.LookupOutcome(cmdID); ok {
			results[0] = prior
			return
		}
		res := envelope.Rejected(cmdID, "RegisterPlayer", string(domain.RejectInvalidIdentity), nil)
		if err := uow.Commit(store.RegistrationCommitRequest{
			Op: store.RegistrationOpStandalone, CommandID: cmdID, CommandType: "RegisterPlayer", Outcome: res,
		}); err != nil {
			if prior, ok := store.AsPriorCommandOutcome(err); ok {
				results[0] = prior
				return
			}
			t.Errorf("standalone commit: %v", err)
			return
		}
		if stored, ok := ts.LookupOutcome(ctx, cmdID); ok {
			results[0] = stored
			return
		}
		results[0] = res
	}()
	go func() {
		defer wg.Done()
		results[1] = registerPlayerTRegOutcome(t, ts, tid, "valid-p", cmdID)
	}()
	wg.Wait()
	if !outcomesEqual(results[0], results[1]) {
		t.Fatalf("canonical mismatch:\n%#v\n%#v", results[0], results[1])
	}
	stored, ok := ts.LookupOutcome(ctx, cmdID)
	if !ok || !outcomesEqual(stored, results[0]) {
		t.Fatalf("stored mismatch ok=%v", ok)
	}
	var rows int
	_ = pool.QueryRow(ctx, `SELECT count(*)::int FROM tournament_registrations WHERE tournament_id=$1`, tid).Scan(&rows)
	if stored.Status == envelope.StatusRejected {
		if rows != 0 {
			t.Fatalf("invalid won but registration leaked: rows=%d", rows)
		}
	} else if stored.Status == envelope.StatusAccepted {
		if rows != 1 {
			t.Fatalf("valid won but rows=%d", rows)
		}
	} else {
		t.Fatalf("unexpected status %s", stored.Status)
	}
}

func TestIntegration_TReg_SameCommandIdCreateVsCreate(t *testing.T) {
	pool, ts := openStore(t)
	ctx := context.Background()
	const cmdID = "shared-cmd-create"
	results := make([]envelope.Result, 2)
	var wg sync.WaitGroup
	wg.Add(2)
	for i, tid := range []string{"treg-create-a", "treg-create-b"} {
		go func(i int, tid string) {
			defer wg.Done()
			uow, err := ts.BeginCreateTournament(ctx, tid, cmdID)
			if err != nil {
				t.Errorf("begin: %v", err)
				return
			}
			defer func() { _ = uow.Rollback() }()
			if prior, ok := uow.LookupOutcome(cmdID); ok {
				results[i] = prior
				return
			}
			cmd := domain.CreateTournamentCommand{
				CommandID: domain.CommandID(cmdID), TournamentID: domain.TournamentID(tid), Capacity: 4,
			}
			decision := domain.DecideCreateTournament(cmd)
			res := envelope.Accepted(cmdID, "CreateTournament", nil, json.RawMessage(`{"facts":[]}`))
			if err := uow.Commit(store.RegistrationCommitRequest{
				Op: store.RegistrationOpCreate, TournamentID: tid, CommandID: cmdID,
				CommandType: "CreateTournament", Outcome: res, Decision: decision, CreateCmd: cmd,
				RetryBudget: 3, BatchSize: 100, BumpBaseProjection: true,
			}); err != nil {
				if prior, ok := store.AsPriorCommandOutcome(err); ok {
					results[i] = prior
					return
				}
				t.Errorf("commit: %v", err)
				return
			}
			if stored, ok := ts.LookupOutcome(ctx, cmdID); ok {
				results[i] = stored
				return
			}
			results[i] = res
		}(i, tid)
	}
	wg.Wait()
	if !outcomesEqual(results[0], results[1]) {
		t.Fatalf("canonical mismatch:\n%#v\n%#v", results[0], results[1])
	}
	var n int
	if err := pool.QueryRow(ctx, `
		SELECT count(*)::int FROM tournaments WHERE tournament_id IN ('treg-create-a','treg-create-b')
	`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("want exactly one tournament created, got %d", n)
	}
}

func TestIntegration_TReg_SameCommandIdCreateVsRegister(t *testing.T) {
	pool, ts := openStore(t)
	ctx := context.Background()
	createTournamentTReg(t, ts, "treg-cvr-existing", 8)
	const cmdID = "shared-cmd-create-reg"
	results := make([]envelope.Result, 2)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		uow, err := ts.BeginCreateTournament(ctx, "treg-cvr-new", cmdID)
		if err != nil {
			t.Errorf("create begin: %v", err)
			return
		}
		defer func() { _ = uow.Rollback() }()
		if prior, ok := uow.LookupOutcome(cmdID); ok {
			results[0] = prior
			return
		}
		cmd := domain.CreateTournamentCommand{
			CommandID: domain.CommandID(cmdID), TournamentID: "treg-cvr-new", Capacity: 4,
		}
		decision := domain.DecideCreateTournament(cmd)
		res := envelope.Accepted(cmdID, "CreateTournament", nil, json.RawMessage(`{"facts":[]}`))
		if err := uow.Commit(store.RegistrationCommitRequest{
			Op: store.RegistrationOpCreate, TournamentID: "treg-cvr-new", CommandID: cmdID,
			CommandType: "CreateTournament", Outcome: res, Decision: decision, CreateCmd: cmd,
			RetryBudget: 3, BatchSize: 100, BumpBaseProjection: true,
		}); err != nil {
			if prior, ok := store.AsPriorCommandOutcome(err); ok {
				results[0] = prior
				return
			}
			t.Errorf("create commit: %v", err)
			return
		}
		if stored, ok := ts.LookupOutcome(ctx, cmdID); ok {
			results[0] = stored
			return
		}
		results[0] = res
	}()
	go func() {
		defer wg.Done()
		results[1] = registerPlayerTRegOutcome(t, ts, "treg-cvr-existing", "p-cvr", cmdID)
	}()
	wg.Wait()
	if !outcomesEqual(results[0], results[1]) {
		t.Fatalf("canonical mismatch:\n%#v\n%#v", results[0], results[1])
	}
	// Loser must not leave a conflicting local success mutation.
	var created int
	_ = pool.QueryRow(ctx, `SELECT count(*)::int FROM tournaments WHERE tournament_id='treg-cvr-new'`).Scan(&created)
	var regs int
	_ = pool.QueryRow(ctx, `SELECT count(*)::int FROM tournament_registrations WHERE tournament_id='treg-cvr-existing'`).Scan(&regs)
	mutations := 0
	if created == 1 {
		mutations++
	}
	if regs == 1 {
		mutations++
	}
	if mutations != 1 {
		t.Fatalf("want exactly one winner mutation, created=%d regs=%d", created, regs)
	}
	if results[0].Status == envelope.StatusAccepted && results[0].Type == "CreateTournament" && created != 1 {
		t.Fatal("create won but tournament missing")
	}
	if results[0].Status == envelope.StatusAccepted && results[0].Type == "RegisterPlayer" && regs != 1 {
		t.Fatal("register won but registration missing")
	}
}

func TestIntegration_TReg_CrossShardAutoCloseElection(t *testing.T) {
	pool, ts := openStore(t)
	ctx := context.Background()
	tid := "treg-election-close"
	capacity := 2
	createTournamentTReg(t, ts, tid, capacity)

	var baseBefore int64
	_ = pool.QueryRow(ctx, `SELECT projection_version FROM bracket_projection_versions WHERE tournament_id=$1`, tid).Scan(&baseBefore)

	// Hold election lock so both final shard-fill txs block after local UPDATE.
	hold, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := hold.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, "tournament:registration-close:"+tid); err != nil {
		t.Fatal(err)
	}

	// Find two players that start on distinct quota shards (0 and 1 for capacity=2).
	var p0, p1 string
	for i := 0; i < 5000; i++ {
		pid := fmt.Sprintf("e-%d", i)
		s := domain.RegistrationStartShard(tid, pid)
		if s == 0 && p0 == "" {
			p0 = pid
		}
		if s == 1 && p1 == "" {
			p1 = pid
		}
		if p0 != "" && p1 != "" {
			break
		}
	}
	if p0 == "" || p1 == "" {
		_ = hold.Rollback(ctx)
		t.Fatal("could not find players for shards 0 and 1")
	}

	type outcome struct {
		res        envelope.Result
		autoClosed bool
		err        error
	}
	done := make(chan outcome, 2)
	started := make(chan struct{}, 2)
	run := func(pid, cmdID string) {
		cctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		uow, err := ts.BeginRegisterPlayer(cctx, tid, pid, cmdID)
		if err != nil {
			done <- outcome{err: err}
			return
		}
		defer func() { _ = uow.Rollback() }()
		started <- struct{}{}
		_, autoClosed, err := uow.ReserveRegistration()
		if err != nil {
			done <- outcome{err: err}
			return
		}
		facts := []domain.Fact{domain.PlayerRegisteredFact(domain.TournamentID(tid), domain.PlayerID(pid))}
		if autoClosed {
			facts = append(facts, domain.RegistrationClosedFact(domain.TournamentID(tid), capacity))
		}
		payload, _ := json.Marshal(map[string]any{"facts": facts})
		res := envelope.Accepted(cmdID, "RegisterPlayer", nil, payload)
		ferr := uow.FinalizeRegister(store.RegistrationCommitRequest{
			Op: store.RegistrationOpRegister, TournamentID: tid,
			CommandID: cmdID, CommandType: "RegisterPlayer", Outcome: res,
			Decision: domain.RegistrationDecision{Kind: domain.RegistrationReserve, Outcome: domain.AcceptedWithFacts(domain.CommandID(cmdID), facts)},
		})
		done <- outcome{res: res, autoClosed: autoClosed, err: ferr}
	}
	go run(p0, "election-c0")
	go run(p1, "election-c1")

	// Wait until both have begun (and are blocked on election or racing toward it).
	for i := 0; i < 2; i++ {
		select {
		case <-started:
		case <-time.After(5 * time.Second):
			_ = hold.Rollback(ctx)
			t.Fatal("registers did not start")
		}
	}
	// Give both time to reach election wait after local shard fill.
	time.Sleep(200 * time.Millisecond)
	if err := hold.Rollback(ctx); err != nil {
		t.Fatal(err)
	}

	var outs [2]outcome
	for i := 0; i < 2; i++ {
		select {
		case o := <-done:
			outs[i] = o
		case <-time.After(15 * time.Second):
			t.Fatal("registers timed out after election release")
		}
	}
	for _, o := range outs {
		if o.err != nil {
			t.Fatalf("register err: %v", o.err)
		}
	}
	closedCount := 0
	for _, o := range outs {
		if o.autoClosed {
			closedCount++
		}
	}
	if closedCount != 1 {
		t.Fatalf("want exactly one autoClosed election winner, got %d", closedCount)
	}

	var sum, rows int
	var phase string
	var regCount int
	_ = pool.QueryRow(ctx, `SELECT COALESCE(SUM(count),0)::int FROM tournament_registration_shards WHERE tournament_id=$1`, tid).Scan(&sum)
	_ = pool.QueryRow(ctx, `SELECT count(*)::int FROM tournament_registrations WHERE tournament_id=$1`, tid).Scan(&rows)
	_ = pool.QueryRow(ctx, `SELECT phase, registered_count FROM tournaments WHERE tournament_id=$1`, tid).Scan(&phase, &regCount)
	if sum != capacity || rows != capacity {
		t.Fatalf("capacity invariant sum=%d rows=%d", sum, rows)
	}
	if phase != string(domain.PhaseSeeding) || regCount != capacity {
		t.Fatalf("phase=%s registered_count=%d", phase, regCount)
	}
	if n := countCloseFactsInOutcomes(t, pool, ctx, tid); n != 1 {
		t.Fatalf("want one close fact in outcomes, got %d", n)
	}
	var baseAfter int64
	_ = pool.QueryRow(ctx, `SELECT projection_version FROM bracket_projection_versions WHERE tournament_id=$1`, tid).Scan(&baseAfter)
	if baseAfter != baseBefore+1 {
		t.Fatalf("base bump want +1 (%d→%d)", baseBefore, baseAfter)
	}
}
