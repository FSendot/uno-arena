package store

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"unoarena/services/analytics/domain"
)

// blockingCH is a process-local ClickHouse stand-in with deterministic Exec gating.
// It models the subset of SQL AnalyticsStore issues for Apply/Rebuild/Snapshot.
type blockingCH struct {
	mu sync.Mutex

	active string
	gens   map[string]genRow
	// processed[gen|eid] = outcome_json
	processed map[string]string
	gameplay  []gameplayRow

	// blockExec, when set, is invoked under no store locks before mutating.
	// Tests use it to hold Rebuild mid-flight while asserting Apply admission.
	blockExec func(query string, params map[string]string)
	// failExec returns a non-nil error to abort that Exec (failed rebuild).
	failExec func(query string, params map[string]string) error
}

type genRow struct {
	status   string
	accepted uint64
}

type gameplayRow struct {
	gen, eid, corr, room, game, tid, vis, mt string
}

func newBlockingCH() *blockingCH {
	return &blockingCH{
		gens:      map[string]genRow{},
		processed: map[string]string{},
	}
}

func (c *blockingCH) Exec(ctx context.Context, query string, params map[string]string) error {
	_ = ctx
	if c.failExec != nil {
		if err := c.failExec(query, params); err != nil {
			return err
		}
	}
	if c.blockExec != nil {
		c.blockExec(query, params)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	q := strings.ToLower(strings.Join(strings.Fields(query), " "))

	switch {
	case strings.Contains(q, "insert into projection_generations"):
		id := params["id"]
		st := params["st"]
		var ac uint64
		_, _ = fmt.Sscanf(params["ac"], "%d", &ac)
		c.gens[id] = genRow{status: st, accepted: ac}
		return nil
	case strings.Contains(q, "insert into active_generation"):
		c.active = params["id"]
		return nil
	case strings.Contains(q, "insert into processed_events"):
		key := params["gen"] + "|" + params["eid"]
		c.processed[key] = params["oj"]
		return nil
	case strings.Contains(q, "insert into gameplay_metrics"):
		c.gameplay = append(c.gameplay, gameplayRow{
			gen: params["gen"], eid: params["eid"], corr: params["cid"],
			room: params["room"], game: params["game"], tid: params["tid"],
			vis: params["vis"], mt: params["mt"],
		})
		return nil
	case strings.Contains(q, "insert into tournament_statistics"),
		strings.Contains(q, "insert into rating_statistics"):
		// Not needed for these race fixtures (gameplay-only events).
		return nil
	default:
		return fmt.Errorf("blockingCH: unsupported Exec: %s", truncate(q, 80))
	}
}

func (c *blockingCH) Query(ctx context.Context, query string, params ...map[string]string) ([][]string, error) {
	_ = ctx
	var p map[string]string
	if len(params) > 0 {
		p = params[0]
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	q := strings.ToLower(strings.Join(strings.Fields(query), " "))

	switch {
	case strings.Contains(q, "from active_generation"):
		if c.active == "" {
			return nil, nil
		}
		return [][]string{{c.active}}, nil
	case strings.Contains(q, "select status from projection_generations"):
		g, ok := c.gens[p["id"]]
		if !ok {
			return nil, nil
		}
		return [][]string{{g.status}}, nil
	case strings.Contains(q, "select outcome_json from processed_events"):
		key := p["gen"] + "|" + p["eid"]
		oj, ok := c.processed[key]
		if !ok {
			return nil, nil
		}
		return [][]string{{oj}}, nil
	case strings.Contains(q, "select count() from processed_events"):
		gen := p["gen"]
		disp := p["d"]
		n := 0
		for k, oj := range c.processed {
			if !strings.HasPrefix(k, gen+"|") {
				continue
			}
			if disp == dispositionApplied {
				if strings.Contains(oj, `"kind":"accepted"`) {
					n++
				}
				continue
			}
			n++
		}
		return [][]string{{fmt.Sprintf("%d", n)}}, nil
	case strings.Contains(q, "from gameplay_metrics"):
		gen := p["gen"]
		var rows [][]string
		for _, r := range c.gameplay {
			if r.gen != gen {
				continue
			}
			rows = append(rows, []string{
				r.eid, r.corr, r.room, r.game, r.tid, r.vis, r.mt,
				"", "", "0", "0", "", "",
			})
		}
		return rows, nil
	case strings.Contains(q, "from tournament_statistics"),
		strings.Contains(q, "from rating_statistics"):
		return nil, nil
	default:
		return nil, fmt.Errorf("blockingCH: unsupported Query: %s", truncate(q, 80))
	}
}

func (c *blockingCH) QueryCell(ctx context.Context, query string, params map[string]string) (string, error) {
	rows, err := c.Query(ctx, query, params)
	if err != nil {
		return "", err
	}
	if len(rows) == 0 || len(rows[0]) == 0 {
		return "", fmt.Errorf("clickhouse: empty result")
	}
	return rows[0][0], nil
}

func (c *blockingCH) seedInitial() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.gens[initialGenerationID] = genRow{status: genStatusComplete, accepted: 0}
	c.active = initialGenerationID
}

func (c *blockingCH) activeUnlocked() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.active
}

func (c *blockingCH) gameplayIn(gen string) []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	var ids []string
	for _, r := range c.gameplay {
		if r.gen == gen {
			ids = append(ids, r.eid)
		}
	}
	return ids
}

func raceGameplay(id string) domain.UpstreamEvent {
	return domain.UpstreamEvent{
		EventID: domain.EventID(id), EventType: domain.EventGameplayMetric,
		Source: domain.SourceRoomGameplayMetrics, SchemaVersion: domain.CurrentSchemaVersion,
		CorrelationID: "c-" + id, OccurredAt: time.Date(2026, 7, 11, 12, 0, 0, 0, time.UTC),
		Payload: map[string]any{
			"visibility": "anonymized_adhoc", "metricType": "turn_advanced", "roomId": "room_1",
		},
	}
}

func TestApplyDuringRebuild_LandsInNewlyActiveGeneration(t *testing.T) {
	ch := newBlockingCH()
	ch.seedInitial()
	s := &AnalyticsStore{client: ch}

	ctx := context.Background()
	if _, err := s.Apply(ctx, raceGameplay("seed1")); err != nil {
		t.Fatalf("seed apply: %v", err)
	}
	oldGen := ch.activeUnlocked()
	if oldGen == "" {
		t.Fatal("expected active generation after seed")
	}

	// Gate Rebuild after the building generation row exists and at least one
	// rebuild event write starts — Apply must not complete into oldGen meanwhile.
	enteredBuild := make(chan struct{})
	releaseBuild := make(chan struct{})
	var once sync.Once
	ch.blockExec = func(query string, params map[string]string) {
		q := strings.ToLower(query)
		if strings.Contains(q, "insert into gameplay_metrics") && params["eid"] == "rebuild1" {
			once.Do(func() { close(enteredBuild) })
			<-releaseBuild
		}
	}

	rebuildDone := make(chan error, 1)
	go func() {
		_, err := s.Rebuild(ctx, []domain.UpstreamEvent{raceGameplay("rebuild1")})
		rebuildDone <- err
	}()

	select {
	case <-enteredBuild:
	case <-time.After(3 * time.Second):
		t.Fatal("rebuild did not reach blocked gameplay insert")
	}

	applyDone := make(chan error, 1)
	go func() {
		_, err := s.Apply(ctx, raceGameplay("concurrent1"))
		applyDone <- err
	}()

	// Apply must not finish into the old generation while rebuild holds exclusive admission.
	select {
	case err := <-applyDone:
		t.Fatalf("Apply completed during rebuild (admission race); err=%v oldGenRows=%v", err, ch.gameplayIn(oldGen))
	case <-time.After(100 * time.Millisecond):
		// expected: still blocked on shared admission
	}

	close(releaseBuild)

	select {
	case err := <-rebuildDone:
		if err != nil {
			t.Fatalf("rebuild: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("rebuild timed out")
	}

	select {
	case err := <-applyDone:
		if err != nil {
			t.Fatalf("apply after rebuild: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("apply timed out after rebuild release")
	}

	newGen := ch.activeUnlocked()
	if newGen == "" || newGen == oldGen {
		t.Fatalf("expected new active generation, old=%s new=%s", oldGen, newGen)
	}
	if got := ch.gameplayIn(oldGen); containsID(got, "concurrent1") {
		t.Fatalf("concurrent Apply wrote superseded generation %s: %v", oldGen, got)
	}
	if got := ch.gameplayIn(newGen); !containsID(got, "rebuild1") || !containsID(got, "concurrent1") {
		t.Fatalf("new active %s missing events: %v", newGen, got)
	}

	snap, err := s.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var ids []string
	for _, m := range snap.GameplayMetrics {
		ids = append(ids, string(m.EventID))
	}
	if !containsID(ids, "rebuild1") || !containsID(ids, "concurrent1") {
		t.Fatalf("public snapshot missing events after activation: %v", ids)
	}
	if containsID(ids, "seed1") {
		t.Fatalf("rebuild should have replaced seed1 visibility; snap=%v", ids)
	}
}

func TestFailedRebuild_UnblocksApplyAndKeepsOldGeneration(t *testing.T) {
	ch := newBlockingCH()
	ch.seedInitial()
	s := &AnalyticsStore{client: ch}
	ctx := context.Background()

	if _, err := s.Apply(ctx, raceGameplay("keep1")); err != nil {
		t.Fatalf("seed: %v", err)
	}
	oldGen := ch.activeUnlocked()

	enteredBuild := make(chan struct{})
	releaseBuild := make(chan struct{})
	var once sync.Once
	ch.blockExec = func(query string, params map[string]string) {
		q := strings.ToLower(query)
		if strings.Contains(q, "insert into gameplay_metrics") && params["eid"] == "fail_rebuild" {
			once.Do(func() { close(enteredBuild) })
			<-releaseBuild
		}
	}
	ch.failExec = func(query string, params map[string]string) error {
		q := strings.ToLower(query)
		// Fail when marking the building generation complete (after event writes).
		if strings.Contains(q, "insert into projection_generations") &&
			params["st"] == genStatusComplete &&
			params["id"] != initialGenerationID &&
			params["id"] != oldGen {
			return fmt.Errorf("injected rebuild failure")
		}
		return nil
	}

	rebuildDone := make(chan error, 1)
	go func() {
		_, err := s.Rebuild(ctx, []domain.UpstreamEvent{raceGameplay("fail_rebuild")})
		rebuildDone <- err
	}()

	select {
	case <-enteredBuild:
	case <-time.After(3 * time.Second):
		t.Fatal("rebuild did not enter blocked write")
	}

	applyDone := make(chan error, 1)
	go func() {
		_, err := s.Apply(ctx, raceGameplay("after_fail"))
		applyDone <- err
	}()

	select {
	case err := <-applyDone:
		t.Fatalf("Apply must wait out exclusive rebuild; finished early err=%v", err)
	case <-time.After(100 * time.Millisecond):
	}

	close(releaseBuild)

	select {
	case err := <-rebuildDone:
		if err == nil {
			t.Fatal("expected injected rebuild failure")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("rebuild timed out")
	}

	select {
	case err := <-applyDone:
		if err != nil {
			t.Fatalf("Apply should proceed after failed rebuild: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Apply did not unblock after failed rebuild")
	}

	if got := ch.activeUnlocked(); got != oldGen {
		t.Fatalf("failed rebuild must leave old generation active: want %s got %s", oldGen, got)
	}
	if got := ch.gameplayIn(oldGen); !containsID(got, "keep1") || !containsID(got, "after_fail") {
		t.Fatalf("old generation rows=%v", got)
	}

	snap, err := s.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var ids []string
	for _, m := range snap.GameplayMetrics {
		ids = append(ids, string(m.EventID))
	}
	if !containsID(ids, "keep1") || !containsID(ids, "after_fail") {
		t.Fatalf("snapshot=%v", ids)
	}
	if containsID(ids, "fail_rebuild") {
		t.Fatalf("failed rebuild rows must not be publicly visible: %v", ids)
	}
}

func containsID(ids []string, want string) bool {
	for _, id := range ids {
		if id == want {
			return true
		}
	}
	return false
}
