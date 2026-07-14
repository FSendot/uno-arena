package main

import (
	"context"

	"go.opentelemetry.io/otel"
	otelmetric "go.opentelemetry.io/otel/metric"

	"unoarena/services/tournament-orchestration/domain"
)

const tournamentCompletionMetricDescription = "Number of authoritative tournament completion transitions committed by Tournament Orchestration."

var tournamentCompletionCounter, _ = otel.Meter("unoarena/services/tournament-orchestration").Int64Counter(
	"unoarena.tournaments.completed",
	otelmetric.WithUnit("1"),
	otelmetric.WithDescription(tournamentCompletionMetricDescription),
)

func initializeBusinessMetrics(ctx context.Context) {
	// Materialize the descriptor before the single completion flow so
	// deployment acceptance can establish a real zero baseline.
	tournamentCompletionCounter.Add(ctx, 0)
}

func recordCommittedTournamentCompletion(ctx context.Context, out domain.CommandOutcome) {
	for _, fact := range out.Facts {
		if fact.Name == domain.FactTournamentCompleted {
			tournamentCompletionCounter.Add(ctx, 1)
			return
		}
	}
}
