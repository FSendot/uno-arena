package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"unoarena/services/room-gameplay/app"
)

const (
	defaultPlayerStreamMaxLen           = int64(1024)
	defaultPlayerStreamCompactorPage    = 100
	defaultPlayerStreamTrimLimit        = int64(4096)
	defaultPlayerStreamCompactorCadence = 30 * time.Second
	defaultPlayerStreamOperationTimeout = 5 * time.Second
)

type playerStreamCatalog interface {
	ListPlayerStreamRoomIDs(context.Context, string, int) ([]string, error)
}

type playerStreamTrimmer interface {
	TrimApprox(context.Context, string, int64, int64) error
}

type redisPlayerStreamTrimmer struct{ client redis.UniversalClient }

func (t redisPlayerStreamTrimmer) TrimApprox(ctx context.Context, stream string, maxLen, limit int64) error {
	return t.client.XTrimMaxLenApprox(ctx, stream, maxLen, limit).Err()
}

// PlayerStreamCompactor bounds disposable Redis player-feed history. Postgres
// remains authoritative, Debezium keeps appending in CDC order, and Gateway
// requests a Room snapshot when a resume marker has been trimmed.
type PlayerStreamCompactor struct {
	catalog          playerStreamCatalog
	trimmer          playerStreamTrimmer
	pageSize         int
	maxLen           int64
	trimLimit        int64
	cadence          time.Duration
	operationTimeout time.Duration
	jitter           func(time.Duration) time.Duration
	afterRoomID      string
}

func NewPlayerStreamCompactor(catalog playerStreamCatalog, trimmer playerStreamTrimmer) *PlayerStreamCompactor {
	return &PlayerStreamCompactor{
		catalog: catalog, trimmer: trimmer,
		pageSize: defaultPlayerStreamCompactorPage, maxLen: defaultPlayerStreamMaxLen,
		trimLimit: defaultPlayerStreamTrimLimit, cadence: defaultPlayerStreamCompactorCadence,
		operationTimeout: defaultPlayerStreamOperationTimeout, jitter: randomJitterDuration,
	}
}

func (w *PlayerStreamCompactor) Configure(pageSize int, maxLen, trimLimit int64, cadence, operationTimeout time.Duration) {
	w.pageSize = boundedPlayerStreamPageSize(pageSize)
	w.maxLen = boundedPlayerStreamMaxLen(maxLen)
	w.trimLimit = boundedPlayerStreamTrimLimit(trimLimit)
	w.cadence = boundedPlayerStreamCadence(cadence)
	w.operationTimeout = boundedPlayerStreamOperationTimeout(operationTimeout)
}

// Run performs one bounded page immediately, then repeats until ctx owns the
// worker's shutdown. Cleanup failures are observable but never gate Room API
// readiness or stop later compaction attempts.
func (w *PlayerStreamCompactor) Run(ctx context.Context) {
	for {
		if err := w.RunOnce(ctx); err != nil && ctx.Err() == nil {
			slog.WarnContext(ctx, "player stream compaction failed", "event", "room_player_stream_compaction_failed", "error", err.Error())
		}
		delay := w.cadence
		if w.jitter != nil {
			delay = w.jitter(delay)
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

// RunOnce compacts at most one keyset page. Approximate XTRIM may leave a small
// radix-node overage, and trimLimit bounds work Redis performs per invocation.
func (w *PlayerStreamCompactor) RunOnce(ctx context.Context) error {
	if w.catalog == nil || w.trimmer == nil {
		return errors.New("player stream compactor dependencies are required")
	}
	opCtx, cancel := context.WithTimeout(ctx, w.operationTimeout)
	defer cancel()
	roomIDs, err := w.catalog.ListPlayerStreamRoomIDs(opCtx, w.afterRoomID, w.pageSize)
	if err != nil {
		return err
	}
	var combined error
	for _, roomID := range roomIDs {
		stream, streamErr := app.PlayerFeedTargetStream(strings.TrimSpace(roomID))
		if streamErr != nil {
			combined = errors.Join(combined, fmt.Errorf("room %q stream key: %w", roomID, streamErr))
			continue
		}
		if trimErr := w.trimmer.TrimApprox(opCtx, stream, w.maxLen, w.trimLimit); trimErr != nil {
			combined = errors.Join(combined, fmt.Errorf("trim %s: %w", stream, trimErr))
		}
	}
	if len(roomIDs) == w.pageSize {
		w.afterRoomID = roomIDs[len(roomIDs)-1]
	} else {
		w.afterRoomID = ""
	}
	return combined
}

func boundedPlayerStreamPageSize(value int) int {
	if value < 1 {
		return defaultPlayerStreamCompactorPage
	}
	if value > 500 {
		return 500
	}
	return value
}

func boundedPlayerStreamMaxLen(value int64) int64 {
	if value < 1 {
		return defaultPlayerStreamMaxLen
	}
	if value > 1_000_000 {
		return 1_000_000
	}
	return value
}

func boundedPlayerStreamTrimLimit(value int64) int64 {
	if value < 1 {
		return defaultPlayerStreamTrimLimit
	}
	if value > 100_000 {
		return 100_000
	}
	return value
}

func boundedPlayerStreamCadence(value time.Duration) time.Duration {
	if value <= 0 {
		return defaultPlayerStreamCompactorCadence
	}
	if value < time.Second {
		return time.Second
	}
	if value > time.Hour {
		return time.Hour
	}
	return value
}

func boundedPlayerStreamOperationTimeout(value time.Duration) time.Duration {
	if value <= 0 {
		return defaultPlayerStreamOperationTimeout
	}
	if value < 250*time.Millisecond {
		return 250 * time.Millisecond
	}
	if value > 30*time.Second {
		return 30 * time.Second
	}
	return value
}
