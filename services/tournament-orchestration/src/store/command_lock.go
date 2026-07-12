package store

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// Global command idempotency advisory lock.
//
// Namespace: "tournament:command:" + commandId
// Exclusive pg_advisory_xact_lock(hashtextextended(..., 0)) serializes all durable
// writers that share a commandId before any decision, reservation, or outcome insert.
//
// Deadlock-safe acquisition order for T-Reg (and reusable by later durable UoWs):
//  1. Rewrite/lifecycle barrier (when a valid tournament id exists)
//  2. Global command lock (this helper)
//  3. Per-player registration lock (register only)
//  4. Row locks / mutations
//
// Standalone invalid/outcome-only paths take only the command lock.
const commandLockPrefix = "tournament:command:"

// commandLockSQL is the exclusive advisory lock statement (structural tests assert this).
const commandLockSQL = `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`

func commandLockKey(commandID string) string {
	return commandLockPrefix + commandID
}

// AcquireCommandLock takes the transaction-scoped global command advisory lock.
// Call after any tournament rewrite barrier and before per-player or row locks.
func AcquireCommandLock(ctx context.Context, tx pgx.Tx, commandID string) error {
	if commandID == "" {
		return fmt.Errorf("commandId required for command lock")
	}
	_, err := tx.Exec(ctx, commandLockSQL, commandLockKey(commandID))
	return err
}

// CommandLockUsesHashtextextended reports whether command lock SQL uses 64-bit hashtextextended.
func CommandLockUsesHashtextextended() bool {
	return commandLockSQL == `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))` &&
		commandLockPrefix == "tournament:command:"
}
