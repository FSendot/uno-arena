package main

import (
	"os"
	"strings"
	"testing"
)

func TestWireProvisioningWorkerRuntime_WiresProvisioningDifferential(t *testing.T) {
	body := extractFuncBody(t, "runtime.go", "wireProvisioningWorkerRuntime")
	if !strings.Contains(body, "Provisioning:") {
		t.Fatal("wireProvisioningWorkerRuntime must inject Provisioning port")
	}
	if !strings.Contains(body, "durableProvisioningRepo") {
		t.Fatal("wireProvisioningWorkerRuntime must use durableProvisioningRepo")
	}
}

func TestProductionDurableRouteCompleteness(t *testing.T) {
	// Every command / worker ingress → required differential port (or store-claim worker).
	routes := []struct {
		ingress string
		port    string
	}{
		{CmdCreateTournament, "Registrations"},
		{CmdRegisterPlayer, "Registrations"},
		{CmdCloseRegistration, "Registrations"},
		{CmdSeedRound, "Seeding"},
		{CmdProvisionRoundMatches, "Provisioning"},
		{CmdRetryProvisioning, "Provisioning"},
		{CmdQuarantineBatch, "Provisioning"},
		{"ProcessProvisioningBatch", "Provisioning"},
		{CmdRecordMatchResult, "RoundMatches"},
		{"IngestMatchCompleted", "RoundMatches"},
		{CmdCompleteRound, "CompleteRounds"},
		{"completion worker TryCompleteReadyRound", "CompleteRounds"},
		{CmdCompleteTournament, "Lifecycle"},
		{CmdCancelTournament, "Lifecycle"},
		{CmdQuarantineResult, "QuarantineResults"},
	}

	api := extractFuncBody(t, "runtime.go", "wireTournamentRuntime")
	requiredAPI := []struct {
		field string
		typ   string
	}{
		{"Registrations:", "durableRegistrationRepo"},
		{"Seeding:", "durableSeedingRepo"},
		{"Provisioning:", "durableProvisioningRepo"},
		{"RoundMatches:", "durableRoundMatchRepo"},
		{"CompleteRounds:", "durableCompleteRoundRepo"},
		{"Lifecycle:", "durableLifecycleRepo"},
		{"QuarantineResults:", "durableQuarantineResultRepo"},
	}
	for _, req := range requiredAPI {
		if !strings.Contains(api, req.field) || !strings.Contains(api, req.typ) {
			t.Fatalf("public durable API must wire %s (%s)", req.field, req.typ)
		}
	}

	provWorker := extractFuncBody(t, "runtime.go", "wireProvisioningWorkerRuntime")
	if !strings.Contains(provWorker, "Provisioning:") || !strings.Contains(provWorker, "durableProvisioningRepo") {
		t.Fatal("provisioning worker must wire Provisioning")
	}

	completion := extractFuncBody(t, "runtime.go", "wireCompletionWorkerRuntime")
	if !strings.Contains(completion, "CompleteRounds:") || !strings.Contains(completion, "durableCompleteRoundRepo") {
		t.Fatal("completion worker must wire CompleteRounds")
	}

	seeding := extractFuncBody(t, "runtime.go", "wireSeedingWorkerRuntime")
	if strings.Contains(seeding, "Seeding:") {
		t.Fatal("seeding worker must not wire Service Seeding port for chunk mutation")
	}
	mainSrc, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	main := string(mainSrc)
	if !strings.Contains(main, "NewSeedingWorker") {
		t.Fatal("seeding worker must start NewSeedingWorker on store claims")
	}
	if !strings.Contains(main, "workerRoleTournamentSeeding") {
		t.Fatal("main must branch seeding worker role")
	}

	submit := extractFuncBody(t, "service.go", "SubmitCommand")
	regGate := extractFuncBody(t, "service.go", "isRegistrationDifferentialCommand")
	provGate := extractFuncBody(t, "service_provisioning.go", "isProvisioningDifferentialCommand")
	ingest := extractFuncBody(t, "service.go", "IngestMatchCompleted")
	processBatch := extractFuncBody(t, "service.go", "ProcessProvisioningBatch")
	tryComplete := extractFuncBody(t, "service_complete_round.go", "TryCompleteReadyRound")

	for _, r := range routes {
		switch r.port {
		case "Registrations":
			constName := map[string]string{
				CmdCreateTournament:  "CmdCreateTournament",
				CmdRegisterPlayer:    "CmdRegisterPlayer",
				CmdCloseRegistration: "CmdCloseRegistration",
			}[r.ingress]
			if !strings.Contains(submit, "registrations") || constName == "" || !strings.Contains(regGate, constName) {
				t.Fatalf("%s must route via %s", r.ingress, r.port)
			}
		case "Seeding":
			if !strings.Contains(submit, "seeding") || !strings.Contains(submit, "CmdSeedRound") {
				t.Fatalf("%s must route via %s", r.ingress, r.port)
			}
		case "Provisioning":
			if r.ingress == "ProcessProvisioningBatch" {
				if !strings.Contains(processBatch, "provisioning") {
					t.Fatalf("%s must route via %s", r.ingress, r.port)
				}
				continue
			}
			constName := map[string]string{
				CmdProvisionRoundMatches: "CmdProvisionRoundMatches",
				CmdRetryProvisioning:     "CmdRetryProvisioning",
				CmdQuarantineBatch:       "CmdQuarantineBatch",
			}[r.ingress]
			if !strings.Contains(submit, "provisioning") || constName == "" || !strings.Contains(provGate, constName) {
				t.Fatalf("%s must route via %s", r.ingress, r.port)
			}
		case "RoundMatches":
			if r.ingress == "IngestMatchCompleted" {
				if !strings.Contains(ingest, "roundMatches") {
					t.Fatalf("%s must route via %s", r.ingress, r.port)
				}
				continue
			}
			if !strings.Contains(submit, "roundMatches") || !strings.Contains(submit, "CmdRecordMatchResult") {
				t.Fatalf("%s must route via %s", r.ingress, r.port)
			}
		case "CompleteRounds":
			if strings.Contains(r.ingress, "completion worker") {
				if !strings.Contains(tryComplete, "completeRounds") {
					t.Fatalf("%s must use %s", r.ingress, r.port)
				}
				continue
			}
			if !strings.Contains(submit, "completeRounds") || !strings.Contains(submit, "CmdCompleteRound") {
				t.Fatalf("%s must route via %s", r.ingress, r.port)
			}
		case "Lifecycle":
			if !strings.Contains(submit, "lifecycle") {
				t.Fatalf("%s must route via %s", r.ingress, r.port)
			}
			if r.ingress == CmdCompleteTournament && !strings.Contains(submit, "CmdCompleteTournament") {
				t.Fatalf("%s must route via %s", r.ingress, r.port)
			}
			if r.ingress == CmdCancelTournament && !strings.Contains(submit, "CmdCancelTournament") {
				t.Fatalf("%s must route via %s", r.ingress, r.port)
			}
		case "QuarantineResults":
			if !strings.Contains(submit, "quarantineResults") || !strings.Contains(submit, "CmdQuarantineResult") {
				t.Fatalf("%s must route via %s", r.ingress, r.port)
			}
		default:
			t.Fatalf("unknown port %s", r.port)
		}
	}
}
