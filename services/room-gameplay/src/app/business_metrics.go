package app

import (
	"context"

	"go.opentelemetry.io/otel"
	otelmetric "go.opentelemetry.io/otel/metric"
)

const gamesCompletedDescription = "Number of authoritative game completion transitions committed by Room Gameplay."

// GamesCompletedDescription exposes the context-owned public metric contract
// without moving the instrument definition into platform code.
func GamesCompletedDescription() string { return gamesCompletedDescription }

var gamesCompletedCounter, _ = otel.Meter("unoarena/services/room-gameplay").Int64Counter(
	"unoarena.games.completed",
	otelmetric.WithUnit("1"),
	otelmetric.WithDescription(gamesCompletedDescription),
)

// InitializeBusinessMetrics materializes the counter descriptor at process
// startup without claiming a completed game.
func InitializeBusinessMetrics(ctx context.Context) {
	gamesCompletedCounter.Add(ctx, 0)
}

// RecordCommittedGameCompletion records the post-commit transition represented
// by a context-owned outbox. It is also called by the reconciliation adapter
// after its authoritative recovery transaction commits.
func RecordCommittedGameCompletion(ctx context.Context, outbox OutboxEntry) {
	for _, event := range outbox.Events {
		if event.Topic == TopicGameCompleted {
			gamesCompletedCounter.Add(ctx, 1)
			return
		}
	}
}
