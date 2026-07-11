package store

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	timerFamilyUno       = "uno"
	timerFamilyReconnect = "reconnect"
	timerBucketCount     = 64
	defaultLeaseTTL      = 30 * time.Second
)

// claimDueScript atomically moves due members from a due ZSET into an inflight ZSET with lease score.
var claimDueScript = redis.NewScript(`
local due = KEYS[1]
local inflight = KEYS[2]
local now = tonumber(ARGV[1])
local lease_until = tonumber(ARGV[2])
local limit = tonumber(ARGV[3])
local members = redis.call('ZRANGEBYSCORE', due, '-inf', now, 'LIMIT', 0, limit)
local claimed = {}
for i, member in ipairs(members) do
  redis.call('ZREM', due, member)
  redis.call('ZADD', inflight, lease_until, member)
  table.insert(claimed, member)
end
return claimed
`)

// reaperScript moves expired inflight leases back to due with score=now.
var reaperScript = redis.NewScript(`
local due = KEYS[1]
local inflight = KEYS[2]
local now = tonumber(ARGV[1])
local members = redis.call('ZRANGEBYSCORE', inflight, '-inf', now)
local n = 0
for i, member in ipairs(members) do
  redis.call('ZREM', inflight, member)
  redis.call('ZADD', due, now, member)
  n = n + 1
end
return n
`)

// TimerIndex is a non-authoritative Redis sorted-set dispatch index.
// Postgres deadlines remain authority; Redis may be rebuilt from open deadlines.
type TimerIndex struct {
	rdb       redis.UniversalClient
	leaseTTL  time.Duration
	keyPrefix string
}

// NewTimerIndex constructs a Redis timer index.
func NewTimerIndex(rdb redis.UniversalClient) *TimerIndex {
	return &TimerIndex{rdb: rdb, leaseTTL: defaultLeaseTTL}
}

// WithKeyPrefix returns a copy that namespaces all timer keys (integration isolation).
func (t *TimerIndex) WithKeyPrefix(prefix string) *TimerIndex {
	cp := *t
	cp.keyPrefix = prefix
	return &cp
}

// WithLeaseTTL returns a copy with a custom inflight lease duration (tests).
func (t *TimerIndex) WithLeaseTTL(d time.Duration) *TimerIndex {
	cp := *t
	if d > 0 {
		cp.leaseTTL = d
	}
	return &cp
}

// KeyPrefix returns the configured key namespace (may be empty).
func (t *TimerIndex) KeyPrefix() string { return t.keyPrefix }

// Ping verifies Redis connectivity.
func (t *TimerIndex) Ping(ctx context.Context) error {
	return t.rdb.Ping(ctx).Err()
}

// LoadScripts ensures Lua scripts are cached on the server.
func (t *TimerIndex) LoadScripts(ctx context.Context) error {
	if err := claimDueScript.Load(ctx, t.rdb).Err(); err != nil {
		return err
	}
	return reaperScript.Load(ctx, t.rdb).Err()
}

// FlushPrefixedKeys deletes due/inflight keys for both families under this prefix only.
func (t *TimerIndex) FlushPrefixedKeys(ctx context.Context) error {
	for _, family := range []string{timerFamilyUno, timerFamilyReconnect} {
		for b := 0; b < timerBucketCount; b++ {
			if err := t.rdb.Del(ctx, t.dueKey(b, family), t.inflightKey(b, family)).Err(); err != nil {
				return err
			}
		}
	}
	return nil
}

// TimerID is a stable timer identity for claim/ack.
type TimerID struct {
	Family     string
	RoomID     string
	PlayerID   string
	GameID     string
	Trigger    string
	Version    int64
	OpeningSeq int64
	ExpiresAt  time.Time
}

type timerIDWire struct {
	Family      string `json:"family"`
	RoomID      string `json:"roomId"`
	PlayerID    string `json:"playerId"`
	GameID      string `json:"gameId"`
	Trigger     string `json:"trigger"`
	Version     int64  `json:"version"`
	OpeningSeq  int64  `json:"openingSeq"`
	ExpiresAtMs int64  `json:"expiresAtMs"`
}

func (id TimerID) String() string {
	b, err := json.Marshal(timerIDWire{
		Family: id.Family, RoomID: id.RoomID, PlayerID: id.PlayerID, GameID: id.GameID,
		Trigger: id.Trigger, Version: id.Version, OpeningSeq: id.OpeningSeq,
		ExpiresAtMs: id.ExpiresAt.UTC().UnixMilli(),
	})
	if err != nil {
		// Deterministic identity must always encode; panic is unacceptable in hot path.
		return ""
	}
	return string(b)
}

func ParseTimerID(s string) (TimerID, error) {
	s = strings.TrimSpace(s)
	if s == "" || !strings.HasPrefix(s, "{") {
		return TimerID{}, fmt.Errorf("invalid timer id")
	}
	dec := json.NewDecoder(strings.NewReader(s))
	dec.DisallowUnknownFields()
	var wire timerIDWire
	if err := dec.Decode(&wire); err != nil {
		return TimerID{}, fmt.Errorf("invalid timer id: %w", err)
	}
	switch wire.Family {
	case timerFamilyUno, timerFamilyReconnect:
	default:
		return TimerID{}, fmt.Errorf("invalid timer id: unknown family %q", wire.Family)
	}
	if strings.TrimSpace(wire.RoomID) == "" {
		return TimerID{}, fmt.Errorf("invalid timer id: blank room")
	}
	if wire.ExpiresAtMs <= 0 {
		return TimerID{}, fmt.Errorf("invalid timer id: invalid expiry")
	}
	return TimerID{
		Family: wire.Family, RoomID: wire.RoomID, PlayerID: wire.PlayerID, GameID: wire.GameID,
		Trigger: wire.Trigger, Version: wire.Version, OpeningSeq: wire.OpeningSeq,
		ExpiresAt: time.UnixMilli(wire.ExpiresAtMs).UTC(),
	}, nil
}

func bucketKey(roomID, family string) int {
	sum := sha256.Sum256([]byte(family + ":" + roomID))
	n := binary.BigEndian.Uint64(sum[:8])
	return int(n % timerBucketCount)
}

func (t *TimerIndex) dueKey(bucket int, family string) string {
	return t.keyPrefix + fmt.Sprintf("room:timers:%s:due:%d", family, bucket)
}

func (t *TimerIndex) inflightKey(bucket int, family string) string {
	return t.keyPrefix + fmt.Sprintf("room:timers:%s:inflight:%d", family, bucket)
}

// Schedule upserts a due member (score = expiresAt unix ms).
func (t *TimerIndex) Schedule(ctx context.Context, id TimerID) error {
	b := bucketKey(id.RoomID, id.Family)
	return t.rdb.ZAdd(ctx, t.dueKey(b, id.Family), redis.Z{
		Score:  float64(id.ExpiresAt.UTC().UnixMilli()),
		Member: id.String(),
	}).Err()
}

// ClaimDue claims up to limit due timers across buckets for a family.
func (t *TimerIndex) ClaimDue(ctx context.Context, family string, now time.Time, limit int) ([]TimerID, error) {
	if limit <= 0 {
		limit = 16
	}
	nowMs := now.UTC().UnixMilli()
	leaseUntil := now.Add(t.leaseTTL).UTC().UnixMilli()
	var out []TimerID
	for b := 0; b < timerBucketCount && len(out) < limit; b++ {
		remain := limit - len(out)
		res, err := claimDueScript.Run(ctx, t.rdb, []string{t.dueKey(b, family), t.inflightKey(b, family)}, nowMs, leaseUntil, remain).StringSlice()
		if err != nil {
			return out, err
		}
		for _, m := range res {
			id, err := ParseTimerID(m)
			if err != nil {
				continue
			}
			out = append(out, id)
		}
	}
	return out, nil
}

// Ack removes an inflight timer after successful/stale-terminal handling.
func (t *TimerIndex) Ack(ctx context.Context, id TimerID) error {
	b := bucketKey(id.RoomID, id.Family)
	return t.rdb.ZRem(ctx, t.inflightKey(b, id.Family), id.String()).Err()
}

// ReapExpiredLeases returns expired inflight members to due for retry.
func (t *TimerIndex) ReapExpiredLeases(ctx context.Context, family string, now time.Time) (int, error) {
	nowMs := now.UTC().UnixMilli()
	total := 0
	for b := 0; b < timerBucketCount; b++ {
		n, err := reaperScript.Run(ctx, t.rdb, []string{t.dueKey(b, family), t.inflightKey(b, family)}, nowMs).Int()
		if err != nil {
			return total, err
		}
		total += n
	}
	return total, nil
}

// RebuildFromPostgres replaces Redis indexes from open Postgres deadlines (authority).
func (t *TimerIndex) RebuildFromPostgres(ctx context.Context, pool *pgxpool.Pool) error {
	if err := t.FlushPrefixedKeys(ctx); err != nil {
		return err
	}
	rows, err := pool.Query(ctx, `
		SELECT room_id, game_id, player_id, triggering_game_event_id, expires_at, opening_room_sequence
		FROM uno_deadlines WHERE status = 'open'
	`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var roomID, gameID, playerID, trigger string
		var expires time.Time
		var opening int64
		if err := rows.Scan(&roomID, &gameID, &playerID, &trigger, &expires, &opening); err != nil {
			return err
		}
		id := TimerID{
			Family: timerFamilyUno, RoomID: roomID, PlayerID: playerID, GameID: gameID,
			Trigger: trigger, OpeningSeq: opening, ExpiresAt: expires.UTC(),
		}
		if err := t.Schedule(ctx, id); err != nil {
			return err
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}

	rows2, err := pool.Query(ctx, `
		SELECT room_id, player_id, disconnect_version, expires_at
		FROM reconnect_deadlines WHERE status = 'open'
	`)
	if err != nil {
		return err
	}
	defer rows2.Close()
	for rows2.Next() {
		var roomID, playerID string
		var ver int64
		var expires time.Time
		if err := rows2.Scan(&roomID, &playerID, &ver, &expires); err != nil {
			return err
		}
		id := TimerID{
			Family: timerFamilyReconnect, RoomID: roomID, PlayerID: playerID,
			Version: ver, ExpiresAt: expires.UTC(),
		}
		if err := t.Schedule(ctx, id); err != nil {
			return err
		}
	}
	return rows2.Err()
}

// NewRedisFromURL opens a go-redis client.
func NewRedisFromURL(url string) (*redis.Client, error) {
	opt, err := redis.ParseURL(url)
	if err != nil {
		return nil, err
	}
	return redis.NewClient(opt), nil
}
