package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"unoarena/platform/telemetry"
	"unoarena/services/room-gameplay/app"
	"unoarena/services/room-gameplay/domain"
)

// RuntimeDesiredState is the controller-owned desired lifecycle. It deliberately
// has no Kubernetes object identity: Postgres is the only room lifecycle source.
type RuntimeDesiredState string

const (
	RuntimeDesiredRunning  RuntimeDesiredState = "running"
	RuntimeDesiredTerminal RuntimeDesiredState = "terminal"
)

func (s RuntimeDesiredState) Valid() bool {
	return s == RuntimeDesiredRunning || s == RuntimeDesiredTerminal
}

// RuntimeObservedState is controller observation, never application authority.
type RuntimeObservedState string

const (
	RuntimeObservedPending  RuntimeObservedState = "pending"
	RuntimeObservedCreating RuntimeObservedState = "creating"
	RuntimeObservedReady    RuntimeObservedState = "ready"
	RuntimeObservedDeleting RuntimeObservedState = "deleting"
	RuntimeObservedDeleted  RuntimeObservedState = "deleted"
)

// RuntimeAssignment is the bounded controller work item for one room generation.
type RuntimeAssignment struct {
	RoomID        string
	Generation    int64
	Desired       RuntimeDesiredState
	Observed      RuntimeObservedState
	PodName       string
	PodIP         string
	LeaseOwner    string
	LeaseUntil    *time.Time
	Attempts      int
	NextAttemptAt time.Time
}

// RuntimePodName produces a deterministic DNS-1123 name without exposing a
// potentially unsafe room identifier in Kubernetes metadata.
func RuntimePodName(roomID string, generation int64) string {
	sum := sha256.Sum256([]byte(roomID + ":" + fmt.Sprint(generation)))
	return "room-" + hex.EncodeToString(sum[:])[:20] + "-g" + fmt.Sprint(generation)
}

// ensureRuntimeAssignmentTx is called while the room aggregate transaction is
// open. Creation and terminal intent therefore cannot be separated from Room
// state/outbox persistence by a process crash.
func ensureRuntimeAssignmentTx(ctx context.Context, tx pgx.Tx, roomID string, terminal bool) error {
	desired := RuntimeDesiredRunning
	if terminal {
		desired = RuntimeDesiredTerminal
	}
	_, err := tx.Exec(ctx, `
		INSERT INTO room_runtime_assignments (
			room_id, generation, desired_state, observed_state, pod_name, next_attempt_at, updated_at
		) VALUES ($1, 1, $2, 'pending', $3, now(), now())
		ON CONFLICT (room_id) DO UPDATE SET
			desired_state = CASE WHEN $2 = 'terminal' THEN 'terminal' ELSE room_runtime_assignments.desired_state END,
			updated_at = now()
	`, roomID, desired, RuntimePodName(roomID, 1))
	return err
}

// BeginExistingGeneration fences a runtime Pod under the same authoritative
// transaction that later holds rooms FOR UPDATE for GI/outbox mutation.
func (s *SessionStore) BeginExistingGeneration(ctx context.Context, roomID domain.RoomID, generation int64) (app.SessionUnitOfWork, error) {
	uow, err := s.BeginExisting(ctx, roomID)
	if err != nil || !uow.Exists() {
		return uow, err
	}
	concrete := uow.(*sessionUoW)
	var current int64
	var desired RuntimeDesiredState
	err = concrete.tx.QueryRow(ctx, `
		SELECT generation, desired_state
		FROM room_runtime_assignments
		WHERE room_id = $1
		FOR UPDATE
	`, string(roomID)).Scan(&current, &desired)
	if err != nil || current != generation || desired != RuntimeDesiredRunning {
		_ = concrete.Rollback()
		if err != nil && err != pgx.ErrNoRows {
			return nil, wrapUnavailable(err)
		}
		return nil, app.ErrRuntimeGenerationStale
	}
	return concrete, nil
}

// ClaimRuntimeAssignments leases a small work batch. SKIP LOCKED makes
// controller replicas leaderless while the row lock serializes generation changes.
func (s *SessionStore) ClaimRuntimeAssignments(ctx context.Context, owner string, limit int, lease time.Duration) ([]RuntimeAssignment, error) {
	if strings.TrimSpace(owner) == "" || limit < 1 {
		return nil, fmt.Errorf("owner and positive limit required")
	}
	if lease <= 0 {
		lease = 20 * time.Second
	}
	rows, err := s.pool.Query(ctx, `
		WITH claimed AS (
			SELECT room_id FROM room_runtime_assignments
			WHERE next_attempt_at <= now()
			  AND (lease_until IS NULL OR lease_until < now())
			  AND NOT (desired_state = 'terminal' AND observed_state = 'deleted')
			ORDER BY next_attempt_at, room_id
			FOR UPDATE SKIP LOCKED
			LIMIT $1
		)
		UPDATE room_runtime_assignments a
		SET lease_owner = $2, lease_until = now() + $3::interval,
			attempts = a.attempts + 1, next_attempt_at = now() + interval '5 seconds', updated_at = now()
		FROM claimed
		WHERE a.room_id = claimed.room_id
		RETURNING a.room_id, a.generation, a.desired_state, a.observed_state,
			a.pod_name, COALESCE(a.pod_ip::text, ''), COALESCE(a.lease_owner, ''),
			a.lease_until, a.attempts, a.next_attempt_at
	`, limit, owner, fmt.Sprintf("%f seconds", lease.Seconds()))
	if err != nil {
		return nil, wrapUnavailable(err)
	}
	defer rows.Close()
	var out []RuntimeAssignment
	for rows.Next() {
		var a RuntimeAssignment
		if err := rows.Scan(&a.RoomID, &a.Generation, &a.Desired, &a.Observed, &a.PodName, &a.PodIP, &a.LeaseOwner, &a.LeaseUntil, &a.Attempts, &a.NextAttemptAt); err != nil {
			return nil, wrapUnavailable(err)
		}
		out = append(out, a)
	}
	return out, wrapUnavailable(rows.Err())
}

func (s *SessionStore) MarkRuntimePodReady(ctx context.Context, roomID string, generation int64, ip string) error {
	traceparent, tracestate := telemetry.TraceContextHeaders(ctx)
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return wrapUnavailable(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	tag, err := tx.Exec(ctx, `
		UPDATE room_runtime_assignments
		SET observed_state = 'ready', pod_ip = $3, lease_owner = NULL, lease_until = NULL, updated_at = now()
		WHERE room_id = $1 AND generation = $2 AND desired_state = 'running'
	`, roomID, generation, ip)
	if err != nil {
		return wrapUnavailable(err)
	}
	if tag.RowsAffected() == 0 {
		return wrapUnavailable(tx.Commit(ctx))
	}
	// The stable event identity makes first readiness durable and deduplicated
	// across repeated observations and every later runtime generation.
	_, err = tx.Exec(ctx, `
		INSERT INTO integration_outbox_events (
			event_id, event_type, topic, partition_key, schema_version, room_id,
			payload, correlation_id, traceparent, tracestate, occurred_at
		)
		SELECT
			'room-runtime-ready:' || r.room_id,
			'RoomRuntimeReady', 'room.runtime.ready', r.room_id, 1, r.room_id,
			jsonb_build_object(
				'schemaVersion', 1,
				'eventId', 'room-runtime-ready:' || r.room_id,
				'eventType', 'RoomRuntimeReady',
				'correlationId', 'room-runtime-ready:' || r.room_id,
				'occurredAt', to_char(now() AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
				'roomId', r.room_id,
				'tournamentId', r.tournament_id,
				'roundNumber', r.round_number,
				'slotId', r.slot_id,
				'generation', $2::bigint
			),
			'room-runtime-ready:' || r.room_id, $3, $4, now()
		FROM rooms r
		WHERE r.room_id = $1 AND r.room_type = 'tournament'
		ON CONFLICT (event_id) DO NOTHING
	`, roomID, generation, nullIfEmpty(traceparent), nullIfEmpty(tracestate))
	if err != nil {
		return wrapUnavailable(err)
	}
	return wrapUnavailable(tx.Commit(ctx))
}

func (s *SessionStore) MarkRuntimePodCreating(ctx context.Context, roomID string, generation int64) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE room_runtime_assignments
		SET observed_state = 'creating', lease_owner = NULL, lease_until = NULL,
			next_attempt_at = now() + interval '2 seconds', updated_at = now()
		WHERE room_id = $1 AND generation = $2 AND desired_state = 'running'
	`, roomID, generation)
	return wrapUnavailable(err)
}

func (s *SessionStore) MarkRuntimePodDeleted(ctx context.Context, roomID string, generation int64) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE room_runtime_assignments
		SET observed_state = 'deleted', pod_ip = NULL, lease_owner = NULL, lease_until = NULL, updated_at = now()
		WHERE room_id = $1 AND generation = $2
	`, roomID, generation)
	return wrapUnavailable(err)
}

// AdvanceRuntimeGeneration is only for controller-owned replacement after a
// missing/unhealthy Pod. It is conditional so a late observer cannot fence a
// newer pod it did not create.
func (s *SessionStore) AdvanceRuntimeGeneration(ctx context.Context, roomID string, generation int64) (RuntimeAssignment, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return RuntimeAssignment{}, wrapUnavailable(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var current int64
	var desired RuntimeDesiredState
	if err := tx.QueryRow(ctx, `SELECT generation, desired_state FROM room_runtime_assignments WHERE room_id = $1 FOR UPDATE`, roomID).Scan(&current, &desired); err != nil {
		if err == pgx.ErrNoRows {
			return RuntimeAssignment{}, app.ErrRuntimeGenerationStale
		}
		return RuntimeAssignment{}, wrapUnavailable(err)
	}
	if current != generation || desired != RuntimeDesiredRunning {
		return RuntimeAssignment{}, app.ErrRuntimeGenerationStale
	}
	next := current + 1
	name := RuntimePodName(roomID, next)
	var a RuntimeAssignment
	err = tx.QueryRow(ctx, `
		UPDATE room_runtime_assignments
		SET generation = $3, observed_state = 'pending', pod_ip = NULL, pod_name = $4,
			lease_owner = NULL, lease_until = NULL, next_attempt_at = now(), updated_at = now()
		WHERE room_id = $1 AND generation = $2
		RETURNING room_id, generation, desired_state, observed_state, pod_name, COALESCE(pod_ip::text, ''),
			COALESCE(lease_owner, ''), lease_until, attempts, next_attempt_at
	`, roomID, generation, next, name).Scan(&a.RoomID, &a.Generation, &a.Desired, &a.Observed, &a.PodName, &a.PodIP, &a.LeaseOwner, &a.LeaseUntil, &a.Attempts, &a.NextAttemptAt)
	if err != nil {
		return RuntimeAssignment{}, wrapUnavailable(err)
	}
	if err := tx.Commit(ctx); err != nil {
		return RuntimeAssignment{}, wrapUnavailable(err)
	}
	return a, nil
}
