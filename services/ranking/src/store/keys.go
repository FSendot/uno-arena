package store

import (
	"context"
	"encoding/json"
	"errors"
	"sort"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// Lock key prefixes establish a globally fixed advisory-lock order when sorted:
// ranking:casual:event < ranking:casual:game < ranking:perf:biz < ranking:perf:event
// < ranking:placement:biz < ranking:placement:event
const (
	lockPrefixCasualEvent    = "ranking:casual:event:"
	lockPrefixCasualGame     = "ranking:casual:game:"
	lockPrefixPerfBiz        = "ranking:perf:biz:"
	lockPrefixPerfEvent      = "ranking:perf:event:"
	lockPrefixPlacementBiz   = "ranking:placement:biz:"
	lockPrefixPlacementEvent = "ranking:placement:event:"
)

// placementDedupeKey returns a PostgreSQL-safe injective encoding of the
// tournament placement business key as a canonical JSON string array.
// NUL / control bytes and delimiter-like characters are JSON-escaped, so the
// value is valid TEXT and distinct components cannot collide.
func placementDedupeKey(playerID, tournamentID, placementEventID string) (string, error) {
	b, err := json.Marshal([]string{playerID, tournamentID, placementEventID})
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func acquireXactLocks(ctx context.Context, tx pgx.Tx, keys ...string) error {
	cleaned := make([]string, 0, len(keys))
	seen := make(map[string]struct{}, len(keys))
	for _, k := range keys {
		if k == "" {
			continue
		}
		if _, ok := seen[k]; ok {
			continue
		}
		seen[k] = struct{}{}
		cleaned = append(cleaned, k)
	}
	sort.Strings(cleaned)
	for _, k := range cleaned {
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext($1))`, k); err != nil {
			return wrapUnavailable(err)
		}
	}
	return nil
}

func casualIngestLockKeys(eventID, gameID string) []string {
	var keys []string
	if eventID != "" {
		keys = append(keys, lockPrefixCasualEvent+eventID)
	}
	if gameID != "" {
		keys = append(keys, lockPrefixCasualGame+gameID)
	}
	return keys
}

func placementIngestLockKeys(placementKey, eventID string) []string {
	var keys []string
	if placementKey != "" {
		keys = append(keys, lockPrefixPlacementBiz+placementKey)
	}
	if eventID != "" {
		keys = append(keys, lockPrefixPlacementEvent+eventID)
	}
	return keys
}

func performanceIngestLockKeys(sourceTopic, businessKey, eventID string) []string {
	var keys []string
	if businessKey != "" {
		keys = append(keys, lockPrefixPerfBiz+sourceTopic+":"+businessKey)
	}
	if eventID != "" {
		keys = append(keys, lockPrefixPerfEvent+eventID)
	}
	return keys
}

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}
