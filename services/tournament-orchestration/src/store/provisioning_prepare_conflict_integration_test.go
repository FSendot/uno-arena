//go:build integration

package store_test

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"unoarena/services/tournament-orchestration/domain"
	"unoarena/services/tournament-orchestration/store"
)

// clearLegacyKickoffAssignments removes whole-rewrite kickoff assignments so differential
// prepare is forced to mutate assigned_matches/slots (the savepoint rollback path under test).
func clearLegacyKickoffAssignments(t *testing.T, ctx context.Context, pool *store.Pool, tid string) {
	t.Helper()
	if _, err := pool.Exec(ctx, `DELETE FROM assigned_matches WHERE tournament_id = $1`, tid); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		UPDATE bracket_slots SET status = 'pending', updated_at = now()
		WHERE tournament_id = $1
	`, tid); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		UPDATE round_progress_shards
		SET assigned_count = 0, resolved_count = 0, quarantined_count = 0
		WHERE tournament_id = $1
	`, tid); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		DELETE FROM outbox_events
		WHERE tournament_id = $1 AND event_type = $2
	`, tid, string(domain.FactTournamentMatchAssigned)); err != nil {
		t.Fatal(err)
	}
}

func TestIntegration_PrepareOutboxPayloadConflict_SavepointRollsBackMutations(t *testing.T) {
	ctx := context.Background()
	pool, ts := openStore(t)
	players := make([]string, 4)
	for i := range players {
		players[i] = "obx" + itoa(i)
	}
	seedProvisionedBatches(t, ts, "obx-conflict", players, 4)
	now := time.Now().UTC()
	claimed, err := ts.ClaimNextProvisioningBatch(ctx, "worker-obx", now, time.Minute)
	if err != nil || claimed == nil {
		t.Fatalf("claim: %v %+v", err, claimed)
	}
	clearLegacyKickoffAssignments(t, ctx, pool, claimed.TournamentID)

	slotID := claimed.SlotFrom
	eventID := domain.MatchAssignedEventID(domain.TournamentID(claimed.TournamentID), claimed.RoundNumber, domain.SlotID(slotID))
	wantRoom := string(domain.RoomIDForSlot(domain.TournamentID(claimed.TournamentID), claimed.RoundNumber, domain.SlotID(slotID)))
	// Canonical event_id with poisoned payload (wrong room) — prepare must conflict + roll back.
	poisonPayload, _ := json.Marshal(map[string]any{
		"schemaVersion": 1,
		"eventId":       eventID,
		"eventType":     string(domain.FactTournamentMatchAssigned),
		"tournamentId":  claimed.TournamentID,
		"roundNumber":   claimed.RoundNumber,
		"slotId":        slotID,
		"roomId":        wantRoom + "-POISON",
		"batchId":       claimed.BatchID,
	})
	if _, err := pool.Exec(ctx, `
		INSERT INTO outbox_events (
			event_id, event_type, tournament_id, topic, partition_key, schema_version, payload, created_at
		) VALUES ($1, $2, $3, 'tournament.match.assigned', $3, 1, $4::jsonb, $5)
	`, eventID, string(domain.FactTournamentMatchAssigned), claimed.TournamentID, poisonPayload, now); err != nil {
		t.Fatal(err)
	}

	var assignedBefore, pendingBefore, progressBefore int
	var projBefore int64
	if err := pool.QueryRow(ctx, `
		SELECT
			(SELECT count(*)::int FROM assigned_matches WHERE tournament_id=$1),
			(SELECT count(*)::int FROM bracket_slots WHERE tournament_id=$1 AND status='pending'),
			(SELECT COALESCE(SUM(assigned_count),0)::int FROM round_progress_shards WHERE tournament_id=$1),
			(SELECT COALESCE(projection_version,0) FROM bracket_projection_versions WHERE tournament_id=$1)
	`, claimed.TournamentID).Scan(&assignedBefore, &pendingBefore, &progressBefore, &projBefore); err != nil {
		t.Fatal(err)
	}
	if assignedBefore != 0 || progressBefore != 0 || pendingBefore == 0 {
		t.Fatalf("precondition: assigned=%d progress=%d pending=%d", assignedBefore, progressBefore, pendingBefore)
	}

	cmdID := "process:" + claimed.TournamentID + ":1:" + claimed.BatchID + ":0"
	res, err := ts.PrepareProvisioningBatch(ctx, store.PrepareProvisioningBatchInput{
		CommandID: cmdID, TournamentID: claimed.TournamentID, RoundNumber: claimed.RoundNumber,
		BatchID: claimed.BatchID, SlotFrom: claimed.SlotFrom, SlotTo: claimed.SlotTo,
		SlotSize: claimed.SlotSize, RetryAttempt: claimed.RetryAttempt,
		LeaseOwner: claimed.LeaseOwner, LeaseVersion: claimed.LeaseVersion,
	})
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	if res == nil || !res.Quarantined {
		t.Fatalf("want batch quarantine got %+v", res)
	}

	var batchStatus, roundStatus string
	if err := pool.QueryRow(ctx, `
		SELECT b.status, r.status
		FROM provisioning_batches b
		JOIN tournament_rounds r ON r.tournament_id=b.tournament_id AND r.round_number=b.round_number
		WHERE b.tournament_id=$1 AND b.batch_id=$2
	`, claimed.TournamentID, claimed.BatchID).Scan(&batchStatus, &roundStatus); err != nil {
		t.Fatal(err)
	}
	if batchStatus != "quarantined" || roundStatus != "blocked" {
		t.Fatalf("batch=%s round=%s want quarantined/blocked", batchStatus, roundStatus)
	}

	var assignedAfter, pendingAfter, progressAfter int
	var projAfter int64
	if err := pool.QueryRow(ctx, `
		SELECT
			(SELECT count(*)::int FROM assigned_matches WHERE tournament_id=$1),
			(SELECT count(*)::int FROM bracket_slots WHERE tournament_id=$1 AND status='pending'),
			(SELECT COALESCE(SUM(assigned_count),0)::int FROM round_progress_shards WHERE tournament_id=$1),
			(SELECT COALESCE(projection_version,0) FROM bracket_projection_versions WHERE tournament_id=$1)
	`, claimed.TournamentID).Scan(&assignedAfter, &pendingAfter, &progressAfter, &projAfter); err != nil {
		t.Fatal(err)
	}
	if assignedAfter != assignedBefore || assignedAfter != 0 {
		t.Fatalf("assigned_matches must remain empty after savepoint rollback: before=%d after=%d", assignedBefore, assignedAfter)
	}
	if pendingAfter != pendingBefore {
		t.Fatalf("bracket slot status mutated: pending before=%d after=%d", pendingBefore, pendingAfter)
	}
	if progressAfter != progressBefore || progressAfter != 0 {
		t.Fatalf("progress shards must remain unchanged: before=%d after=%d", progressBefore, progressAfter)
	}
	// Quarantine bumps once for round→blocked; prepare assignment bump must not apply.
	if projAfter != projBefore+1 {
		t.Fatalf("projection_version: before=%d after=%d want exactly +1 quarantine bump", projBefore, projAfter)
	}
}

func TestIntegration_PrepareOutboxPoison_WrongTopicSchemaPayload(t *testing.T) {
	ctx := context.Background()
	pool, ts := openStore(t)

	cases := []struct {
		name          string
		topic         string
		schemaVersion int
		mutatePayload func(map[string]any)
	}{
		{
			name: "wrong_topic", topic: "tournament.match.completed", schemaVersion: 1,
			mutatePayload: func(m map[string]any) {},
		},
		{
			name: "wrong_row_schema", topic: "tournament.match.assigned", schemaVersion: 99,
			mutatePayload: func(m map[string]any) {},
		},
		{
			name: "wrong_payload_schema", topic: "tournament.match.assigned", schemaVersion: 1,
			mutatePayload: func(m map[string]any) { m["schemaVersion"] = 7 },
		},
		{
			name: "wrong_payload_batch", topic: "tournament.match.assigned", schemaVersion: 1,
			mutatePayload: func(m map[string]any) { m["batchId"] = "other-batch" },
		},
	}

	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tid := "poison-" + tc.name
			players := make([]string, 4)
			for j := range players {
				players[j] = "pz" + itoa(i) + itoa(j)
			}
			seedProvisionedBatches(t, ts, tid, players, 4)
			now := time.Now().UTC()
			claimed, err := ts.ClaimNextProvisioningBatch(ctx, "worker-"+tc.name, now, time.Minute)
			if err != nil || claimed == nil {
				t.Fatalf("claim: %v %+v", err, claimed)
			}
			clearLegacyKickoffAssignments(t, ctx, pool, claimed.TournamentID)
			slotID := claimed.SlotFrom
			eventID := domain.MatchAssignedEventID(domain.TournamentID(claimed.TournamentID), claimed.RoundNumber, domain.SlotID(slotID))
			wantRoom := string(domain.RoomIDForSlot(domain.TournamentID(claimed.TournamentID), claimed.RoundNumber, domain.SlotID(slotID)))
			payload := map[string]any{
				"schemaVersion": 1,
				"eventId":       eventID,
				"eventType":     string(domain.FactTournamentMatchAssigned),
				"tournamentId":  claimed.TournamentID,
				"roundNumber":   claimed.RoundNumber,
				"slotId":        slotID,
				"roomId":        wantRoom,
				"batchId":       claimed.BatchID,
			}
			tc.mutatePayload(payload)
			raw, _ := json.Marshal(payload)
			if _, err := pool.Exec(ctx, `
				INSERT INTO outbox_events (
					event_id, event_type, tournament_id, topic, partition_key, schema_version, payload, created_at
				) VALUES ($1, $2, $3, $4, $3, $5, $6::jsonb, $7)
			`, eventID, string(domain.FactTournamentMatchAssigned), claimed.TournamentID, tc.topic, tc.schemaVersion, raw, now); err != nil {
				t.Fatal(err)
			}

			var assignedBefore int
			_ = pool.QueryRow(ctx, `SELECT count(*) FROM assigned_matches WHERE tournament_id=$1`, claimed.TournamentID).Scan(&assignedBefore)

			cmdID := "process:" + claimed.TournamentID + ":1:" + claimed.BatchID + ":0"
			res, err := ts.PrepareProvisioningBatch(ctx, store.PrepareProvisioningBatchInput{
				CommandID: cmdID, TournamentID: claimed.TournamentID, RoundNumber: claimed.RoundNumber,
				BatchID: claimed.BatchID, SlotFrom: claimed.SlotFrom, SlotTo: claimed.SlotTo,
				SlotSize: claimed.SlotSize, RetryAttempt: claimed.RetryAttempt,
				LeaseOwner: claimed.LeaseOwner, LeaseVersion: claimed.LeaseVersion,
			})
			if err != nil {
				t.Fatalf("prepare: %v", err)
			}
			if res == nil || !res.Quarantined {
				t.Fatalf("want quarantine for %s got %+v", tc.name, res)
			}
			var assignedAfter int
			_ = pool.QueryRow(ctx, `SELECT count(*) FROM assigned_matches WHERE tournament_id=$1`, claimed.TournamentID).Scan(&assignedAfter)
			if assignedAfter != assignedBefore {
				t.Fatalf("%s: assigned_matches leaked across poison conflict", tc.name)
			}
			var batchStatus string
			_ = pool.QueryRow(ctx, `SELECT status FROM provisioning_batches WHERE tournament_id=$1 AND batch_id=$2`,
				claimed.TournamentID, claimed.BatchID).Scan(&batchStatus)
			if batchStatus != "quarantined" {
				t.Fatalf("batch status=%s", batchStatus)
			}
			// Original accepted row preserved (no overwrite of topic/schema/payload).
			var topic string
			var schemaVersion int
			var storedPayload []byte
			if err := pool.QueryRow(ctx, `
				SELECT topic, schema_version, payload FROM outbox_events WHERE event_id=$1
			`, eventID).Scan(&topic, &schemaVersion, &storedPayload); err != nil {
				t.Fatal(err)
			}
			if topic != tc.topic || schemaVersion != tc.schemaVersion {
				t.Fatalf("poison row mutated: topic=%s schema=%d", topic, schemaVersion)
			}
		})
	}
}

func TestIntegration_PrepareUniqueViolation_QuarantinesWithoutAbortedTx(t *testing.T) {
	ctx := context.Background()
	pool, ts := openStore(t)

	players := make([]string, 4)
	for i := range players {
		players[i] = "uniq" + itoa(i)
	}
	seedProvisionedBatches(t, ts, "uniq-a", players, 4)
	now := time.Now().UTC()
	claimed, err := ts.ClaimNextProvisioningBatch(ctx, "worker-uniq", now, time.Minute)
	if err != nil || claimed == nil {
		t.Fatalf("claim: %v", err)
	}
	clearLegacyKickoffAssignments(t, ctx, pool, claimed.TournamentID)
	wantRoom := string(domain.RoomIDForSlot(domain.TournamentID(claimed.TournamentID), claimed.RoundNumber, domain.SlotID(claimed.SlotFrom)))

	// Second tournament owns a slot so we can park the colliding room_id under UNIQUE(room_id).
	otherPlayers := make([]string, 4)
	for i := range otherPlayers {
		otherPlayers[i] = "other" + itoa(i)
	}
	seedProvisionedBatches(t, ts, "uniq-b", otherPlayers, 4)
	clearLegacyKickoffAssignments(t, ctx, pool, "uniq-b")

	sideTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sideTx.Rollback(ctx) }()
	if _, err := sideTx.Exec(ctx, `
		INSERT INTO assigned_matches (tournament_id, round_number, slot_id, room_id, assigned_at, provisioning_batch_id)
		VALUES ('uniq-b', 1, 'slot_0', $1, $2, 'batch_0')
	`, wantRoom, now); err != nil {
		t.Fatal(err)
	}

	var (
		prepRes *store.PrepareProvisioningBatchResult
		prepErr error
		wg      sync.WaitGroup
	)
	wg.Add(1)
	go func() {
		defer wg.Done()
		cmdID := "process:" + claimed.TournamentID + ":1:" + claimed.BatchID + ":0"
		prepRes, prepErr = ts.PrepareProvisioningBatch(ctx, store.PrepareProvisioningBatchInput{
			CommandID: cmdID, TournamentID: claimed.TournamentID, RoundNumber: claimed.RoundNumber,
			BatchID: claimed.BatchID, SlotFrom: claimed.SlotFrom, SlotTo: claimed.SlotTo,
			SlotSize: claimed.SlotSize, RetryAttempt: claimed.RetryAttempt,
			LeaseOwner: claimed.LeaseOwner, LeaseVersion: claimed.LeaseVersion,
		})
	}()

	deadline := time.Now().Add(3 * time.Second)
	for {
		var waiting int
		_ = pool.QueryRow(ctx, `
			SELECT count(*)::int FROM pg_locks
			WHERE NOT granted AND locktype = 'transactionid'
		`).Scan(&waiting)
		if waiting > 0 || time.Now().After(deadline) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err := sideTx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	wg.Wait()

	if prepErr != nil {
		t.Fatalf("unique conflict must quarantine not aborted-tx fail: %v", prepErr)
	}
	if prepRes == nil || !prepRes.Quarantined {
		t.Fatalf("want quarantined got %+v", prepRes)
	}
	var assignedOnA int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM assigned_matches WHERE tournament_id=$1`, claimed.TournamentID).Scan(&assignedOnA); err != nil {
		t.Fatal(err)
	}
	if assignedOnA != 0 {
		t.Fatalf("savepoint must roll back A's assignments, got %d", assignedOnA)
	}
}
