package main

import (
	"context"

	"unoarena/services/analytics/domain"
)

// AnalyticsApplication is the deep application/store seam for Analytics handlers.
// Durable ClickHouse and capability memory adapters share this contract.
// Store/DB failures return error so HTTP maps them to 503; domain dispositions
// (accepted/duplicate/quarantined) remain ApplyOutcome values with nil error.
type AnalyticsApplication interface {
	Apply(ctx context.Context, evt domain.UpstreamEvent) (domain.ApplyOutcome, error)
	Rebuild(ctx context.Context, events []domain.UpstreamEvent) ([]domain.ApplyOutcome, error)
	Snapshot(ctx context.Context) (domain.AnalyticsSnapshot, error)
	SnapshotJSON(ctx context.Context) ([]byte, error)
	ProjectionVersion(ctx context.Context) (domain.ProjectionVersion, error)
	Ready(ctx context.Context) error
}
