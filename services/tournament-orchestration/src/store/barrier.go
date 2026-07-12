package store

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// Rewrite barrier coordinates transitional legacy whole-rewrite vs differential MatchCompleted.
//
// Namespace: "tournament:rewrite:" + tournamentId
//   - Differential hot/worker paths take pg_advisory_xact_lock_shared (many coexist per tournament).
//   - Terminal lifecycle Complete/Cancel take exclusive pg_advisory_xact_lock (fences in-flight
//     shared work; blocks new shared acquires) while remaining O(1).
//   - Legacy BeginExisting / BeginCreate also take exclusive (excludes differentials).
//
// Keys use hashtextextended(..., 0) for a full 64-bit advisory lock space (not 32-bit hashtext).
// Acquire the barrier BEFORE any child row lock (tournaments FOR UPDATE, shards, slots, etc.).
// Shared and exclusive calls use the identical namespace string and acquisition order.
const rewriteBarrierPrefix = "tournament:rewrite:"

// rewriteBarrierLockSQL is the exclusive advisory lock statement (structural tests assert this).
const rewriteBarrierLockSQL = `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`

// rewriteBarrierLockSharedSQL is the shared advisory lock statement (structural tests assert this).
const rewriteBarrierLockSharedSQL = `SELECT pg_advisory_xact_lock_shared(hashtextextended($1, 0))`

func rewriteBarrierKey(tournamentID string) string {
	return rewriteBarrierPrefix + tournamentID
}

func acquireRewriteBarrierExclusive(ctx context.Context, tx pgx.Tx, tournamentID string) error {
	if tournamentID == "" {
		return fmt.Errorf("tournamentId required for rewrite barrier")
	}
	_, err := tx.Exec(ctx, rewriteBarrierLockSQL, rewriteBarrierKey(tournamentID))
	return err
}

func acquireRewriteBarrierShared(ctx context.Context, tx pgx.Tx, tournamentID string) error {
	if tournamentID == "" {
		return fmt.Errorf("tournamentId required for rewrite barrier")
	}
	_, err := tx.Exec(ctx, rewriteBarrierLockSharedSQL, rewriteBarrierKey(tournamentID))
	return err
}

// RewriteBarrierUsesHashtextextended reports whether barrier SQL uses 64-bit hashtextextended.
func RewriteBarrierUsesHashtextextended() bool {
	return rewriteBarrierLockSQL == `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))` &&
		rewriteBarrierLockSharedSQL == `SELECT pg_advisory_xact_lock_shared(hashtextextended($1, 0))` &&
		rewriteBarrierPrefix == "tournament:rewrite:"
}
