package main

import (
	"testing"

	"unoarena/services/tournament-orchestration/domain"
	"unoarena/shared/envelope"
)

func TestProjectionChangedFromOutcome_PublicVisibilitySemantics(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		out  domain.CommandOutcome
		want bool
	}{
		{
			name: "reject never bumps",
			out: domain.CommandOutcome{
				Kind:      domain.OutcomeRejected,
				CommandID: "c",
				Rejection: &domain.Rejection{Code: domain.RejectInvalidCommand, Message: "bad"},
			},
			want: false,
		},
		{
			name: "factless accept stable",
			out:  domain.CommandOutcome{Kind: domain.OutcomeAccepted, CommandID: "c", Facts: nil},
			want: false,
		},
		{
			name: "retry hidden batch state only",
			out: domain.CommandOutcome{
				Kind: domain.OutcomeAccepted, CommandID: "c",
				Facts: []domain.Fact{
					{Name: domain.FactTournamentProvisioningBatchRetried, Data: map[string]string{"batchId": "b0"}},
				},
			},
			want: false,
		},
		{
			name: "non-last batch complete hidden only",
			out: domain.CommandOutcome{
				Kind: domain.OutcomeAccepted, CommandID: "c",
				Facts: []domain.Fact{
					{Name: domain.FactTournamentProvisioningBatchCompleted, Data: map[string]string{"batchId": "b0"}},
				},
			},
			want: false,
		},
		{
			name: "last batch complete public round status",
			out: domain.CommandOutcome{
				Kind: domain.OutcomeAccepted, CommandID: "c",
				Facts: []domain.Fact{
					{Name: domain.FactTournamentProvisioningBatchCompleted, Data: map[string]string{
						"batchId":                           "b1",
						domain.FactDataPublicBracketVisible: "true",
					}},
				},
			},
			want: true,
		},
		{
			name: "quarantine batch public blocked",
			out: domain.CommandOutcome{
				Kind: domain.OutcomeAccepted, CommandID: "c",
				Facts: []domain.Fact{
					{Name: domain.FactTournamentProvisioningBatchQuarantined, Data: map[string]string{
						"batchId":                           "b0",
						domain.FactDataPublicBracketVisible: "true",
					}},
				},
			},
			want: true,
		},
		{
			name: "quarantine batch already blocked hidden",
			out: domain.CommandOutcome{
				Kind: domain.OutcomeAccepted, CommandID: "c",
				Facts: []domain.Fact{
					{Name: domain.FactTournamentProvisioningBatchQuarantined, Data: map[string]string{"batchId": "b0"}},
				},
			},
			want: false,
		},
		{
			name: "phase create bumps",
			out: domain.CommandOutcome{
				Kind: domain.OutcomeAccepted, CommandID: "c",
				Facts: []domain.Fact{
					{Name: domain.FactTournamentCreated, Data: map[string]string{"tournamentId": "t"}},
				},
			},
			want: true,
		},
		{
			name: "registeredCount bumps",
			out: domain.CommandOutcome{
				Kind: domain.OutcomeAccepted, CommandID: "c",
				Facts: []domain.Fact{
					{Name: domain.FactPlayerRegisteredInTournament, Data: map[string]string{"playerId": "p"}},
				},
			},
			want: true,
		},
		{
			name: "round seed bumps",
			out: domain.CommandOutcome{
				Kind: domain.OutcomeAccepted, CommandID: "c",
				Facts: []domain.Fact{
					{Name: domain.FactTournamentRoundSeeded, Data: map[string]string{"roundNumber": "1"}},
				},
			},
			want: true,
		},
		{
			name: "slot assignment bumps",
			out: domain.CommandOutcome{
				Kind: domain.OutcomeAccepted, CommandID: "c",
				Facts: []domain.Fact{
					{Name: domain.FactTournamentMatchAssigned, Data: map[string]string{"slotId": "slot_0"}},
				},
			},
			want: true,
		},
		{
			name: "result bumps",
			out: domain.CommandOutcome{
				Kind: domain.OutcomeAccepted, CommandID: "c",
				Facts: []domain.Fact{
					{Name: domain.FactTournamentMatchResultRecorded, Data: map[string]string{"slotId": "slot_0"}},
				},
			},
			want: true,
		},
		{
			name: "advancement bumps",
			out: domain.CommandOutcome{
				Kind: domain.OutcomeAccepted, CommandID: "c",
				Facts: []domain.Fact{
					{Name: domain.FactPlayersAdvanced, Data: map[string]string{"slotId": "slot_0"}},
				},
			},
			want: true,
		},
		{
			name: "result quarantine bumps",
			out: domain.CommandOutcome{
				Kind: domain.OutcomeAccepted, CommandID: "c",
				Facts: []domain.Fact{
					{Name: domain.FactTournamentResultQuarantined, Data: map[string]string{"slotId": "slot_0"}},
				},
			},
			want: true,
		},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := projectionChangedFromOutcome(tc.out); got != tc.want {
				t.Fatalf("projectionChangedFromOutcome=%v want %v facts=%+v", got, tc.want, tc.out.Facts)
			}
		})
	}
}

func TestProjectionVersion_ProvisioningPublicVisibility(t *testing.T) {
	h := newTestHarness(t)
	mux := h.srv.Routes()
	corr := map[string]string{"X-Correlation-Id": "corr-proj-vis"}

	postJSON(t, mux, "/internal/v1/commands", testCred, commandBody("vis-create", "CreateTournament", map[string]any{
		"tournamentId": "t-proj-vis",
		"capacity":     25,
		"batchSize":    2,
	}, "op", "s"), corr)
	for i := 1; i <= 25; i++ {
		p := "p" + itoa(i)
		postJSON(t, mux, "/internal/v1/commands", testCred, commandBody("vis-reg-"+p, "RegisterPlayer", map[string]any{
			"tournamentId": "t-proj-vis", "playerId": p,
		}, p, "s"), corr)
	}
	postJSON(t, mux, "/internal/v1/commands", testCred, commandBody("vis-close", "CloseRegistration", map[string]any{
		"tournamentId": "t-proj-vis",
	}, "op", "s"), corr)
	postJSON(t, mux, "/internal/v1/commands", testCred, commandBody("vis-seed", "SeedRound", map[string]any{
		"tournamentId": "t-proj-vis", "roundNumber": 1,
	}, "op", "s"), corr)
	postJSON(t, mux, "/internal/v1/commands", testCred, commandBody("vis-prov", "ProvisionRoundMatches", map[string]any{
		"tournamentId": "t-proj-vis", "roundNumber": 1,
	}, "op", "s"), corr)

	batches := bracketBatches(t, h, "t-proj-vis", 1)
	if len(batches) < 2 {
		t.Fatalf("need >=2 batches, got %d", len(batches))
	}

	verBase, _ := h.repo.ProjectionCheckpoint(domain.TournamentID("t-proj-vis"))

	// Retry changes hidden batch status only.
	w := postJSON(t, mux, "/internal/v1/commands", testCred, commandBody("vis-retry", "RetryTournamentProvisioningBatch", map[string]any{
		"tournamentId": "t-proj-vis",
		"roundNumber":  1,
		"batchId":      string(batches[0].BatchID),
		"retryAttempt": 1,
	}, "op", "s"), corr)
	if decodeResult(t, w).Status != envelope.StatusAccepted {
		t.Fatalf("retry: %s", w.Body.String())
	}
	verAfterRetry, _ := h.repo.ProjectionCheckpoint(domain.TournamentID("t-proj-vis"))
	if verAfterRetry != verBase {
		t.Fatalf("retry bumped version %d -> %d", verBase, verAfterRetry)
	}

	// Non-last batch completion: hidden only.
	w = postJSON(t, mux, "/internal/v1/tournaments/t-proj-vis/rounds/1/provisioning-batches", testCred, map[string]any{
		"commandId":     "vis-worker-0",
		"schemaVersion": 1,
		"batchId":       string(batches[0].BatchID),
		"slotFrom":      string(batches[0].SlotFrom),
		"slotTo":        string(batches[0].SlotTo),
		"slotSize":      len(batches[0].SlotIndexes),
	}, corr)
	if decodeResult(t, w).Status != envelope.StatusAccepted {
		t.Fatalf("non-last complete: %s", w.Body.String())
	}
	verAfterNonLast, _ := h.repo.ProjectionCheckpoint(domain.TournamentID("t-proj-vis"))
	if verAfterNonLast != verBase {
		t.Fatalf("non-last complete bumped %d -> %d", verBase, verAfterNonLast)
	}

	// Last batch completion: public round status → in_progress.
	w = postJSON(t, mux, "/internal/v1/tournaments/t-proj-vis/rounds/1/provisioning-batches", testCred, map[string]any{
		"commandId":     "vis-worker-1",
		"schemaVersion": 1,
		"batchId":       string(batches[1].BatchID),
		"slotFrom":      string(batches[1].SlotFrom),
		"slotTo":        string(batches[1].SlotTo),
		"slotSize":      len(batches[1].SlotIndexes),
	}, corr)
	if decodeResult(t, w).Status != envelope.StatusAccepted {
		t.Fatalf("last complete: %s", w.Body.String())
	}
	verAfterLast, _ := h.repo.ProjectionCheckpoint(domain.TournamentID("t-proj-vis"))
	if verAfterLast <= verBase {
		t.Fatalf("last complete should bump: before=%d after=%d", verBase, verAfterLast)
	}

	// Quarantine on a fresh tournament: public round status → blocked.
	postJSON(t, mux, "/internal/v1/commands", testCred, commandBody("visq-create", "CreateTournament", map[string]any{
		"tournamentId": "t-proj-vis-q",
		"capacity":     10,
		"batchSize":    1,
	}, "op", "s"), corr)
	for i := 1; i <= 10; i++ {
		p := "q" + itoa(i)
		postJSON(t, mux, "/internal/v1/commands", testCred, commandBody("visq-reg-"+p, "RegisterPlayer", map[string]any{
			"tournamentId": "t-proj-vis-q", "playerId": p,
		}, p, "s"), corr)
	}
	postJSON(t, mux, "/internal/v1/commands", testCred, commandBody("visq-close", "CloseRegistration", map[string]any{
		"tournamentId": "t-proj-vis-q",
	}, "op", "s"), corr)
	postJSON(t, mux, "/internal/v1/commands", testCred, commandBody("visq-seed", "SeedRound", map[string]any{
		"tournamentId": "t-proj-vis-q", "roundNumber": 1,
	}, "op", "s"), corr)
	postJSON(t, mux, "/internal/v1/commands", testCred, commandBody("visq-prov", "ProvisionRoundMatches", map[string]any{
		"tournamentId": "t-proj-vis-q", "roundNumber": 1,
	}, "op", "s"), corr)
	qBatches := bracketBatches(t, h, "t-proj-vis-q", 1)
	verQBase, _ := h.repo.ProjectionCheckpoint(domain.TournamentID("t-proj-vis-q"))
	w = postJSON(t, mux, "/internal/v1/commands", testCred, commandBody("visq-q", "QuarantineTournamentProvisioningBatch", map[string]any{
		"tournamentId": "t-proj-vis-q",
		"roundNumber":  1,
		"batchId":      string(qBatches[0].BatchID),
		"reason":       "operator",
	}, "op", "s"), corr)
	if decodeResult(t, w).Status != envelope.StatusAccepted {
		t.Fatalf("quarantine: %s", w.Body.String())
	}
	verQAfter, _ := h.repo.ProjectionCheckpoint(domain.TournamentID("t-proj-vis-q"))
	if verQAfter <= verQBase {
		t.Fatalf("quarantine should bump: before=%d after=%d", verQBase, verQAfter)
	}
}
