package store

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"unoarena/services/tournament-orchestration/domain"
)

const (
	// DefaultProvisioningLease is the visibility window for one claimed batch.
	DefaultProvisioningLease = 5 * time.Minute
	// DefaultProvisioningReapLimit bounds expired-lease resets per reap call.
	DefaultProvisioningReapLimit = 64
)

// ClaimedProvisioningBatch is one row exclusively claimed for worker processing.
type ClaimedProvisioningBatch struct {
	TournamentID   string
	RoundNumber    int
	BatchID        string
	RetryAttempt   int
	SlotFrom       string
	SlotTo         string
	SlotSize       int
	LeaseOwner     string
	LeaseExpiresAt time.Time
	LeaseVersion   int64
}

// ErrProvisioningBatchTooLarge is returned when a claimed row exceeds MaxProvisioningBatchSize.
var ErrProvisioningBatchTooLarge = errors.New("provisioning batch exceeds max size")

// ReapExpiredProvisioningLeases resets a bounded set of expired in_progress rows.
// Tournament-first FOR UPDATE SKIP LOCKED matches BeginExisting/persist lock order so
// reaping cannot race an in-flight aggregate rewrite on the same tournament.
// retry_attempt==0 → pending; otherwise → retried. Clears lease columns.
func (s *TournamentStore) ReapExpiredProvisioningLeases(ctx context.Context, now time.Time) (int64, error) {
	return s.ReapExpiredProvisioningLeasesBounded(ctx, now, DefaultProvisioningReapLimit)
}

// ReapExpiredProvisioningLeasesBounded is the testable reap entry with an explicit row limit.
func (s *TournamentStore) ReapExpiredProvisioningLeasesBounded(ctx context.Context, now time.Time, limit int) (int64, error) {
	if s == nil || s.pool == nil {
		return 0, fmt.Errorf("nil store")
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	now = now.UTC()
	if limit <= 0 {
		limit = DefaultProvisioningReapLimit
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var tid string
	err = tx.QueryRow(ctx, `
		SELECT t.tournament_id
		FROM tournaments t
		WHERE EXISTS (
			SELECT 1
			FROM provisioning_batches b
			WHERE b.tournament_id = t.tournament_id
			  AND b.status = 'in_progress'
			  AND b.lease_expires_at IS NOT NULL
			  AND b.lease_expires_at < $1
		)
		ORDER BY t.tournament_id ASC
		FOR UPDATE OF t SKIP LOCKED
		LIMIT 1
	`, now).Scan(&tid)
	if errors.Is(err, pgx.ErrNoRows) {
		if err := tx.Commit(ctx); err != nil {
			return 0, err
		}
		return 0, nil
	}
	if err != nil {
		return 0, err
	}

	tag, err := tx.Exec(ctx, `
		WITH picked AS (
			SELECT tournament_id, round_number, batch_id
			FROM provisioning_batches
			WHERE tournament_id = $2
			  AND status = 'in_progress'
			  AND lease_expires_at IS NOT NULL
			  AND lease_expires_at < $1
			ORDER BY lease_expires_at ASC, round_number ASC, batch_id ASC
			FOR UPDATE
			LIMIT $3
		)
		UPDATE provisioning_batches b
		SET status = CASE WHEN b.retry_attempt > 0 THEN 'retried' ELSE 'pending' END,
		    lease_owner = NULL,
		    lease_expires_at = NULL,
		    lease_heartbeat_at = NULL,
		    updated_at = $1
		FROM picked p
		WHERE b.tournament_id = p.tournament_id
		  AND b.round_number = p.round_number
		  AND b.batch_id = p.batch_id
	`, now, tid, limit)
	if err != nil {
		return 0, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

// ClaimNextProvisioningBatch claims at most one claimable batch with Tournament-first locking.
// Lock order matches BeginExisting/persist: select one eligible nonterminal tournament
// (provisioning round + claimable batch) FOR UPDATE SKIP LOCKED, then select+claim one batch.
// Claimable: pending, retried, or expired in_progress. Terminal completed/quarantined/cancelled never match.
// The claim transaction commits before the caller runs ProcessProvisioningBatch.
func (s *TournamentStore) ClaimNextProvisioningBatch(ctx context.Context, owner string, now time.Time, leaseTTL time.Duration) (*ClaimedProvisioningBatch, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("nil store")
	}
	owner = strings.TrimSpace(owner)
	if owner == "" {
		return nil, fmt.Errorf("lease owner is required")
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	now = now.UTC()
	if leaseTTL <= 0 {
		leaseTTL = DefaultProvisioningLease
	}
	expires := now.Add(leaseTTL)

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var lockedTID string
	err = tx.QueryRow(ctx, `
		SELECT t.tournament_id
		FROM tournaments t
		WHERE t.phase NOT IN ('completed', 'cancelled')
		  AND EXISTS (
			SELECT 1
			FROM tournament_rounds r
			INNER JOIN provisioning_batches b
			  ON b.tournament_id = r.tournament_id
			 AND b.round_number = r.round_number
			WHERE r.tournament_id = t.tournament_id
			  AND r.status = 'provisioning'
			  AND (
				b.status IN ('pending', 'retried')
				OR (
					b.status = 'in_progress'
					AND b.lease_expires_at IS NOT NULL
					AND b.lease_expires_at < $1
				)
			  )
		  )
		ORDER BY t.created_at ASC, t.tournament_id ASC
		FOR UPDATE OF t SKIP LOCKED
		LIMIT 1
	`, now).Scan(&lockedTID)
	if errors.Is(err, pgx.ErrNoRows) {
		if err := tx.Commit(ctx); err != nil {
			return nil, err
		}
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	var (
		tid, batchID, slotFrom, slotTo string
		roundNumber, retryAttempt      int
		leaseOwner                     string
		leaseExpires                   time.Time
		leaseVersion                   int64
	)
	err = tx.QueryRow(ctx, `
		WITH candidate AS (
			SELECT b.tournament_id, b.round_number, b.batch_id
			FROM provisioning_batches b
			INNER JOIN tournament_rounds r
			  ON r.tournament_id = b.tournament_id
			 AND r.round_number = b.round_number
			WHERE b.tournament_id = $1
			  AND r.status = 'provisioning'
			  AND (
				b.status IN ('pending', 'retried')
				OR (
					b.status = 'in_progress'
					AND b.lease_expires_at IS NOT NULL
					AND b.lease_expires_at < $2
				)
			  )
			ORDER BY b.created_at ASC, b.round_number ASC,
			         (regexp_replace(b.slot_id_from, '^slot_', ''))::int ASC
			FOR UPDATE OF b SKIP LOCKED
			LIMIT 1
		)
		UPDATE provisioning_batches b
		SET status = 'in_progress',
		    lease_owner = $3,
		    lease_expires_at = $4,
		    lease_version = b.lease_version + 1,
		    updated_at = $2
		FROM candidate c
		WHERE b.tournament_id = c.tournament_id
		  AND b.round_number = c.round_number
		  AND b.batch_id = c.batch_id
		RETURNING b.tournament_id, b.round_number, b.batch_id, b.retry_attempt,
		          b.slot_id_from, b.slot_id_to, b.lease_owner, b.lease_expires_at,
		          b.lease_version
	`, lockedTID, now, owner, expires).Scan(
		&tid, &roundNumber, &batchID, &retryAttempt,
		&slotFrom, &slotTo, &leaseOwner, &leaseExpires, &leaseVersion,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		if err := tx.Commit(ctx); err != nil {
			return nil, err
		}
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	slotSize, err := SlotSizeFromRange(slotFrom, slotTo)
	if err != nil {
		return nil, err
	}
	if slotSize > domain.MaxProvisioningBatchSize {
		// Leave the row claimed until lease expiry so a poison oversized row does not spin.
		if err := tx.Commit(ctx); err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("%w: slotSize=%d max=%d batch=%s", ErrProvisioningBatchTooLarge, slotSize, domain.MaxProvisioningBatchSize, batchID)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return &ClaimedProvisioningBatch{
		TournamentID:   tid,
		RoundNumber:    roundNumber,
		BatchID:        batchID,
		RetryAttempt:   retryAttempt,
		SlotFrom:       slotFrom,
		SlotTo:         slotTo,
		SlotSize:       slotSize,
		LeaseOwner:     leaseOwner,
		LeaseExpiresAt: leaseExpires,
		LeaseVersion:   leaseVersion,
	}, nil
}

// SlotSizeFromRange derives inclusive slot count from deterministic slot_{n} identities.
func SlotSizeFromRange(slotFrom, slotTo string) (int, error) {
	from, err := parseSlotIndex(slotFrom)
	if err != nil {
		return 0, fmt.Errorf("slotFrom: %w", err)
	}
	to, err := parseSlotIndex(slotTo)
	if err != nil {
		return 0, fmt.Errorf("slotTo: %w", err)
	}
	if to < from {
		return 0, fmt.Errorf("slot range inverted: %s..%s", slotFrom, slotTo)
	}
	return to - from + 1, nil
}

func parseSlotIndex(slotID string) (int, error) {
	slotID = strings.TrimSpace(slotID)
	const prefix = "slot_"
	if !strings.HasPrefix(slotID, prefix) {
		return 0, fmt.Errorf("invalid slot id %q", slotID)
	}
	n, err := strconv.Atoi(slotID[len(prefix):])
	if err != nil || n < 0 {
		return 0, fmt.Errorf("invalid slot id %q", slotID)
	}
	return n, nil
}
