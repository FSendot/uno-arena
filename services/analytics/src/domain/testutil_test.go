package domain

import "testing"

func gameplayEvent(id EventID, payload map[string]any) UpstreamEvent {
	return UpstreamEvent{
		EventID:       id,
		EventType:     EventGameplayMetric,
		Source:        SourceRoomGameplayMetrics,
		SchemaVersion: CurrentSchemaVersion,
		CorrelationID: "corr_" + string(id),
		Payload:       payload,
	}
}

func tournamentEvent(id EventID, payload map[string]any) UpstreamEvent {
	return UpstreamEvent{
		EventID:       id,
		EventType:     EventTournamentStatistic,
		Source:        SourceTournamentRoundCompleted,
		SchemaVersion: CurrentSchemaVersion,
		CorrelationID: "corr_" + string(id),
		Payload:       payload,
	}
}

func ratingEvent(id EventID, payload map[string]any) UpstreamEvent {
	return UpstreamEvent{
		EventID:       id,
		EventType:     EventRatingStatistic,
		Source:        SourceRankingPlayerRatingUpdated,
		SchemaVersion: CurrentSchemaVersion,
		CorrelationID: "corr_" + string(id),
		Payload:       payload,
	}
}

func mustApply(t *testing.T, p *PublicAnalyticsProjection, evt UpstreamEvent) ApplyOutcome {
	t.Helper()
	out := p.Apply(evt)
	if out.Kind != OutcomeAccepted {
		t.Fatalf("apply %s: %+v", evt.EventID, out)
	}
	return out
}

func hasFact(facts []Fact, name FactName) bool {
	for _, f := range facts {
		if f.Name == name {
			return true
		}
	}
	return false
}

func factNames(facts []Fact) []FactName {
	out := make([]FactName, len(facts))
	for i, f := range facts {
		out[i] = f.Name
	}
	return out
}
