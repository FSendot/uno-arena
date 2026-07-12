package store

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// Registration auto-close election lock.
//
// Namespace: "tournament:registration-close:" + tournamentId
// Taken only when a local shard UPDATE returns newCount==quota (at most once per
// shard reaching full). After waiting, a fresh statement snapshot decides whether
// all shards are full; the conditional phase UPDATE registration→seeding elects
// exactly one closer (rowsAffected=1).
//
// Manual CloseRegistration uses the exclusive rewrite barrier and never takes
// this election lock (no deadlock with shared-barrier + election holders).
const registrationCloseElectionPrefix = "tournament:registration-close:"

const registrationCloseElectionLockSQL = `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`

func registrationCloseElectionKey(tournamentID string) string {
	return registrationCloseElectionPrefix + tournamentID
}

func acquireRegistrationCloseElectionLock(ctx context.Context, tx pgx.Tx, tournamentID string) error {
	if tournamentID == "" {
		return fmt.Errorf("tournamentId required for registration-close election")
	}
	_, err := tx.Exec(ctx, registrationCloseElectionLockSQL, registrationCloseElectionKey(tournamentID))
	return err
}

// RegistrationCloseElectionUsesHashtextextended reports 64-bit election lock SQL.
func RegistrationCloseElectionUsesHashtextextended() bool {
	return registrationCloseElectionLockSQL == `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))` &&
		registrationCloseElectionPrefix == "tournament:registration-close:"
}
