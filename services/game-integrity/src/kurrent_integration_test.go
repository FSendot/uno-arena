//go:build integration

package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/kurrent-io/KurrentDB-Client-Go/kurrentdb"

	"unoarena/services/game-integrity/domain"
)

const (
	integrationMasterKey = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	integrationAltKey    = "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
)

func integrationURL(t *testing.T) string {
	t.Helper()
	u := os.Getenv("KURRENTDB_INTEGRATION_URL")
	if u == "" {
		t.Skip("KURRENTDB_INTEGRATION_URL not set")
	}
	return u
}

func integrationKeyring(t *testing.T) string {
	t.Helper()
	if v := os.Getenv("GAME_INTEGRITY_ENVELOPE_DEV_KEYS"); v != "" {
		return v
	}
	return "1:" + integrationMasterKey
}

func newIntegrationServer(t *testing.T) (*Server, *KurrentStreamRepository) {
	t.Helper()
	return newIntegrationServerWithKeys(t, integrationKeyring(t), 1)
}

func newIntegrationServerWithKeys(t *testing.T, keyring string, version int) (*Server, *KurrentStreamRepository) {
	t.Helper()
	url := integrationURL(t)
	t.Setenv("KURRENTDB_URL", url)
	t.Setenv("DEPLOYMENT_ENV", "test")
	t.Setenv("GAME_INTEGRITY_ENVELOPE_PROVIDER", "dev")
	t.Setenv("GAME_INTEGRITY_ENVELOPE_DEV_KEYS", keyring)
	t.Setenv("GAME_INTEGRITY_ENVELOPE_DEV_MASTER_KEY", "")
	t.Setenv("GAME_INTEGRITY_ENVELOPE_KEY_VERSION", fmt.Sprintf("%d", version))
	t.Setenv("GAME_INTEGRITY_ALLOW_MEMORY", "")
	if os.Getenv("GAME_INTEGRITY_READINESS_STREAM_SUFFIX") == "" {
		t.Setenv("GAME_INTEGRITY_READINESS_STREAM_SUFFIX", "it-"+uniqueSuffix(t))
	}

	repo, audit, mode, reason := resolveRuntime()
	if reason != "" || repo == nil || mode != "durable" {
		t.Fatalf("resolveRuntime: repo=%v mode=%q reason=%q", repo != nil, mode, reason)
	}
	krepo, ok := repo.(*KurrentStreamRepository)
	if !ok {
		t.Fatalf("expected *KurrentStreamRepository, got %T", repo)
	}
	t.Cleanup(func() { _ = krepo.Close() })
	srv := NewServerWithAudit(repo, audit, testRoomCredential, testAuditCredential, mode, reason)
	return srv, krepo
}

func uniqueSuffix(t *testing.T) string {
	t.Helper()
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return fmt.Sprintf("%x", b)
}

func uniqueRoomID(t *testing.T) string {
	t.Helper()
	return fmt.Sprintf("room-int-%s-%s", uniqueSuffix(t), sanitizeName(t.Name()))
}

func uniqueGameID(t *testing.T) string {
	t.Helper()
	return fmt.Sprintf("game-int-%s", uniqueSuffix(t))
}

func uniqueEventID(t *testing.T, label string) string {
	t.Helper()
	return fmt.Sprintf("evt-%s-%s", label, uniqueSuffix(t))
}

func sanitizeName(name string) string {
	out := make([]rune, 0, len(name))
	for _, r := range name {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') {
			out = append(out, r)
		} else {
			out = append(out, '-')
		}
	}
	return string(out)
}

func openIndependentRepo(t *testing.T, keyring string, version int, pageSize uint64) *KurrentStreamRepository {
	t.Helper()
	return openIndependentRepoWithSuffix(t, keyring, version, pageSize, "indep-"+uniqueSuffix(t))
}

func openIndependentRepoWithSuffix(t *testing.T, keyring string, version int, pageSize uint64, readinessSuffix string) *KurrentStreamRepository {
	t.Helper()
	url := integrationURL(t)
	t.Setenv("DEPLOYMENT_ENV", "test")
	t.Setenv("GAME_INTEGRITY_READINESS_STREAM_SUFFIX", readinessSuffix)
	keys, err := ParseDevKeyring(keyring)
	if err != nil {
		t.Fatal(err)
	}
	p, err := NewDevKeyProviderFromKeyring(keys, version)
	if err != nil {
		t.Fatal(err)
	}
	client, err := openKurrentClient(url)
	if err != nil {
		t.Fatal(err)
	}
	repo := NewKurrentStreamRepositoryWithPageSize(client, p, pageSize)
	t.Cleanup(func() { _ = repo.Close() })
	return repo
}

func TestIntegration_ReadyAndRoomAppendReplay(t *testing.T) {
	srv, krepo := newIntegrationServer(t)
	mux := srv.routes()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := krepo.Ready(ctx); err != nil {
		t.Fatalf("ready: %v", err)
	}

	rw := httptest.NewRecorder()
	mux.ServeHTTP(rw, httptest.NewRequest(http.MethodGet, "/ready", nil))
	if rw.Code != http.StatusOK {
		t.Fatalf("ready http: %d %s", rw.Code, rw.Body.String())
	}
	ready := decodeJSON(t, rw)
	if ready["status"] != "ready" || ready["mode"] != "durable" {
		t.Fatalf("ready body: %+v", ready)
	}

	roomID := uniqueRoomID(t)
	payload := map[string]any{"marker": "integration-plaintext-token-alpha", "n": 1}
	w := doJSON(t, mux, http.MethodPost, "/internal/v1/game-logs/"+roomID+"/append", map[string]any{
		"eventId": "evt-int-1", "expectedRevision": 0, "eventType": "PlayCard",
		"gameId": "game-int-1", "payload": payload,
	}, withRoomCred(nil))
	if w.Code != http.StatusOK {
		t.Fatalf("append: %d %s", w.Code, w.Body.String())
	}
	out := decodeJSON(t, w)
	if out["kind"] != "accepted" || out["revision"] != float64(1) || out["logOffset"] != float64(0) {
		t.Fatalf("append body: %+v", out)
	}

	rw = doJSON(t, mux, http.MethodGet, "/internal/v1/game-logs/"+roomID+"/replay?from=0", nil, withAuditCred(nil))
	if rw.Code != http.StatusOK {
		t.Fatalf("replay: %d %s", rw.Code, rw.Body.String())
	}
	rep := decodeJSON(t, rw)
	if rep["revision"] != float64(1) {
		t.Fatalf("replay rev: %+v", rep)
	}
	entries, ok := rep["entries"].([]any)
	if !ok || len(entries) != 1 {
		t.Fatalf("entries: %+v", rep["entries"])
	}
	entry := entries[0].(map[string]any)
	if entry["eventId"] != "evt-int-1" || entry["eventType"] != "PlayCard" {
		t.Fatalf("entry: %+v", entry)
	}
	raw, _ := json.Marshal(entry["payload"])
	if !bytes.Contains(raw, []byte("integration-plaintext-token-alpha")) {
		t.Fatalf("authorized replay missing plaintext payload: %s", raw)
	}
}

func TestIntegration_StaleDuplicateConflictAndConcurrent(t *testing.T) {
	srv, _ := newIntegrationServer(t)
	mux := srv.routes()
	roomID := uniqueRoomID(t)

	first := doJSON(t, mux, http.MethodPost, "/internal/v1/game-logs/"+roomID+"/append", map[string]any{
		"eventId": "dup-base", "expectedRevision": 0, "eventType": "CreateRoom",
		"payload": map[string]any{"room": "base"},
	}, withRoomCred(nil))
	if first.Code != http.StatusOK {
		t.Fatalf("first: %d %s", first.Code, first.Body.String())
	}

	stale := doJSON(t, mux, http.MethodPost, "/internal/v1/game-logs/"+roomID+"/append", map[string]any{
		"eventId": "stale-1", "expectedRevision": 0, "eventType": "JoinRoom",
		"payload": map[string]any{"seat": 1},
	}, withRoomCred(nil))
	if stale.Code != http.StatusConflict || decodeJSON(t, stale)["code"] != "revision_mismatch" {
		t.Fatalf("stale: %d %s", stale.Code, stale.Body.String())
	}

	payload := map[string]any{"card": "r5"}
	accepted := doJSON(t, mux, http.MethodPost, "/internal/v1/game-logs/"+roomID+"/append", map[string]any{
		"eventId": "same-id-1", "expectedRevision": 1, "eventType": "PlayCard", "payload": payload,
	}, withRoomCred(nil))
	if accepted.Code != http.StatusOK || decodeJSON(t, accepted)["kind"] != "accepted" {
		t.Fatalf("accepted: %d %s", accepted.Code, accepted.Body.String())
	}
	dup := doJSON(t, mux, http.MethodPost, "/internal/v1/game-logs/"+roomID+"/append", map[string]any{
		"eventId": "same-id-1", "expectedRevision": 99, "eventType": "PlayCard", "payload": payload,
	}, withRoomCred(nil))
	if dup.Code != http.StatusOK || decodeJSON(t, dup)["kind"] != "duplicate" {
		t.Fatalf("duplicate: %d %s", dup.Code, dup.Body.String())
	}
	conflict := doJSON(t, mux, http.MethodPost, "/internal/v1/game-logs/"+roomID+"/append", map[string]any{
		"eventId": "same-id-1", "expectedRevision": 2, "eventType": "PlayCard",
		"payload": map[string]any{"card": "OTHER"},
	}, withRoomCred(nil))
	if conflict.Code != http.StatusConflict || decodeJSON(t, conflict)["code"] != "conflicting_duplicate" {
		t.Fatalf("conflict: %d %s", conflict.Code, conflict.Body.String())
	}

	roomRace := uniqueRoomID(t)
	seed := doJSON(t, mux, http.MethodPost, "/internal/v1/game-logs/"+roomRace+"/append", map[string]any{
		"eventId": "race-seed", "expectedRevision": 0, "eventType": "CreateRoom", "payload": map[string]any{},
	}, withRoomCred(nil))
	if seed.Code != http.StatusOK {
		t.Fatalf("race seed: %d %s", seed.Code, seed.Body.String())
	}

	type result struct {
		code int
		kind string
		c    string
	}
	ch := make(chan result, 2)
	for i := 0; i < 2; i++ {
		i := i
		go func() {
			w := doJSON(t, mux, http.MethodPost, "/internal/v1/game-logs/"+roomRace+"/append", map[string]any{
				"eventId": fmt.Sprintf("race-%d", i), "expectedRevision": 1, "eventType": "JoinRoom",
				"payload": map[string]any{"i": i},
			}, withRoomCred(nil))
			body := decodeJSON(t, w)
			kind, _ := body["kind"].(string)
			code, _ := body["code"].(string)
			ch <- result{code: w.Code, kind: kind, c: code}
		}()
	}
	var acceptedN, mismatchN int
	for i := 0; i < 2; i++ {
		r := <-ch
		switch {
		case r.code == http.StatusOK && r.kind == "accepted":
			acceptedN++
		case r.code == http.StatusConflict && r.c == "revision_mismatch":
			mismatchN++
		default:
			t.Fatalf("unexpected race result: %+v", r)
		}
	}
	if acceptedN != 1 || mismatchN != 1 {
		t.Fatalf("race accepted=%d mismatch=%d", acceptedN, mismatchN)
	}
	rep := doJSON(t, mux, http.MethodGet, "/internal/v1/game-logs/"+roomRace+"/replay?from=0", nil, withAuditCred(nil))
	if decodeJSON(t, rep)["revision"] != float64(2) {
		t.Fatalf("race revision: %s", rep.Body.String())
	}
}

func TestIntegration_DeckLifecycleAndNewRepoRestore(t *testing.T) {
	srv, krepo := newIntegrationServer(t)
	mux := srv.routes()
	roomID := uniqueRoomID(t)
	gameID := fmt.Sprintf("game-%d", time.Now().UnixNano())

	seedRoom := doJSON(t, mux, http.MethodPost, "/internal/v1/game-logs/"+roomID+"/append", map[string]any{
		"eventId": "deck-room-seed", "expectedRevision": 0, "eventType": "CreateRoom", "payload": map[string]any{},
	}, withRoomCred(nil))
	if seedRoom.Code != http.StatusOK {
		t.Fatalf("room seed: %d %s", seedRoom.Code, seedRoom.Body.String())
	}

	init := doJSON(t, mux, http.MethodPost, "/internal/v1/game-logs/"+roomID+"/deck-operations", map[string]any{
		"operation": "initialize", "gameId": gameID,
	}, withRoomCred(nil))
	initBody := decodeJSON(t, init)
	if init.Code != http.StatusOK || initBody["kind"] != "accepted" {
		t.Fatalf("init: %d %s", init.Code, init.Body.String())
	}
	seedCommit, _ := initBody["seedCommitment"].(string)
	if seedCommit == "" {
		t.Fatal("missing seedCommitment")
	}

	deal := doJSON(t, mux, http.MethodPost, "/internal/v1/game-logs/"+roomID+"/deck-operations", map[string]any{
		"operation": "reserve_deal", "gameId": gameID, "operationId": "op-deal-1",
		"seats": []string{"p1", "p2"}, "cardsPerHand": 7,
	}, withRoomCred(nil))
	dealBody := decodeJSON(t, deal)
	if deal.Code != http.StatusOK {
		t.Fatalf("deal: %d %s", deal.Code, deal.Body.String())
	}
	resID, _ := dealBody["reservationId"].(string)
	if resID == "" {
		t.Fatal("missing reservationId")
	}
	confirm := doJSON(t, mux, http.MethodPost, "/internal/v1/game-logs/"+roomID+"/deck-operations", map[string]any{
		"operation": "confirm", "gameId": gameID, "reservationId": resID,
	}, withRoomCred(nil))
	if confirm.Code != http.StatusOK {
		t.Fatalf("confirm: %d %s", confirm.Code, confirm.Body.String())
	}

	draw := doJSON(t, mux, http.MethodPost, "/internal/v1/game-logs/"+roomID+"/deck-operations", map[string]any{
		"operation": "reserve_draw", "gameId": gameID, "operationId": "op-draw-1", "count": 2,
	}, withRoomCred(nil))
	drawBody := decodeJSON(t, draw)
	if draw.Code != http.StatusOK {
		t.Fatalf("draw: %d %s", draw.Code, draw.Body.String())
	}
	drawRes, _ := drawBody["reservationId"].(string)
	cancel := doJSON(t, mux, http.MethodPost, "/internal/v1/game-logs/"+roomID+"/deck-operations", map[string]any{
		"operation": "cancel", "gameId": gameID, "reservationId": drawRes,
	}, withRoomCred(nil))
	if cancel.Code != http.StatusOK {
		t.Fatalf("cancel: %d %s", cancel.Code, cancel.Body.String())
	}
	draw2 := doJSON(t, mux, http.MethodPost, "/internal/v1/game-logs/"+roomID+"/deck-operations", map[string]any{
		"operation": "reserve_draw", "gameId": gameID, "operationId": "op-draw-2", "count": 2,
	}, withRoomCred(nil))
	draw2Body := decodeJSON(t, draw2)
	if draw2.Code != http.StatusOK {
		t.Fatalf("draw2: %d %s", draw2.Code, draw2.Body.String())
	}
	draw2Res, _ := draw2Body["reservationId"].(string)
	confirm2 := doJSON(t, mux, http.MethodPost, "/internal/v1/game-logs/"+roomID+"/deck-operations", map[string]any{
		"operation": "confirm", "gameId": gameID, "reservationId": draw2Res,
	}, withRoomCred(nil))
	if confirm2.Code != http.StatusOK {
		t.Fatalf("confirm2: %d %s", confirm2.Code, confirm2.Body.String())
	}

	// Idempotent re-confirm
	reconfirm := doJSON(t, mux, http.MethodPost, "/internal/v1/game-logs/"+roomID+"/deck-operations", map[string]any{
		"operation": "confirm", "gameId": gameID, "reservationId": draw2Res,
	}, withRoomCred(nil))
	reconfirmBody := decodeJSON(t, reconfirm)
	if reconfirm.Code != http.StatusOK || reconfirmBody["kind"] != "duplicate" {
		t.Fatalf("reconfirm: %d %s", reconfirm.Code, reconfirm.Body.String())
	}

	_ = krepo.Close()
	srv2, krepo2 := newIntegrationServer(t)
	defer krepo2.Close()
	mux2 := srv2.routes()

	export := doJSON(t, mux2, http.MethodGet, "/internal/v1/audit/exports/"+gameID+"?roomId="+roomID, nil, withAuditCred(nil))
	if export.Code != http.StatusOK {
		t.Fatalf("export restore: %d %s", export.Code, export.Body.String())
	}
	body := decodeJSON(t, export)
	deck, ok := body["deck"].(map[string]any)
	if !ok {
		t.Fatalf("missing deck: %s", export.Body.String())
	}
	if deck["seedCommitment"] != seedCommit {
		t.Fatalf("seedCommitment mismatch: %v vs %s", deck["seedCommitment"], seedCommit)
	}
	if deck["pointer"] != float64(7*2+1+2) { // deal 15 + draw 2
		t.Fatalf("pointer=%v", deck["pointer"])
	}

	// Conflicting re-initialize must remain rejected after restore
	reinit := doJSON(t, mux2, http.MethodPost, "/internal/v1/game-logs/"+roomID+"/deck-operations", map[string]any{
		"operation": "initialize", "gameId": gameID,
	}, withRoomCred(nil))
	if reinit.Code != http.StatusConflict || decodeJSON(t, reinit)["code"] != "conflicting_duplicate" {
		t.Fatalf("reinit: %d %s", reinit.Code, reinit.Body.String())
	}
}

func TestIntegration_AtRestCiphertextHidesPlaintext(t *testing.T) {
	srv, krepo := newIntegrationServer(t)
	mux := srv.routes()
	roomID := uniqueRoomID(t)
	gameID := fmt.Sprintf("game-cipher-%d", time.Now().UnixNano())
	secret := "distinctive-seed-token-zeta-999"
	cardMarker := "card-face-marker-omega"

	appendW := doJSON(t, mux, http.MethodPost, "/internal/v1/game-logs/"+roomID+"/append", map[string]any{
		"eventId": "cipher-evt-1", "expectedRevision": 0, "eventType": "PlayCard",
		"gameId": gameID, "payload": map[string]any{"secret": secret, "card": cardMarker},
	}, withRoomCred(nil))
	if appendW.Code != http.StatusOK {
		t.Fatalf("append: %d %s", appendW.Code, appendW.Body.String())
	}
	init := doJSON(t, mux, http.MethodPost, "/internal/v1/game-logs/"+roomID+"/deck-operations", map[string]any{
		"operation": "initialize", "gameId": gameID,
	}, withRoomCred(nil))
	if init.Code != http.StatusOK {
		t.Fatalf("init: %d %s", init.Code, init.Body.String())
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, stream := range []string{roomStreamName(domain.RoomID(roomID)), deckStreamName(domain.RoomID(roomID), domain.GameID(gameID))} {
		events, err := krepo.readAllEvents(ctx, stream)
		if err != nil || len(events) == 0 {
			t.Fatalf("raw read %s: err=%v n=%d", stream, err, len(events))
		}
		for _, ev := range events {
			meta := string(ev.Event.UserMetadata)
			if bytes.Contains(ev.Event.Data, []byte(secret)) || bytes.Contains(ev.Event.Data, []byte(cardMarker)) {
				t.Fatalf("plaintext leaked in Data of %s", stream)
			}
			if bytes.Contains(ev.Event.Data, []byte(`"seedHex"`)) || bytes.Contains(ev.Event.Data, []byte(`"order"`)) {
				t.Fatalf("deck plaintext fields present in Data of %s", stream)
			}
			if !bytes.Contains(ev.Event.UserMetadata, []byte(`"wrappedDek"`)) ||
				!bytes.Contains(ev.Event.UserMetadata, []byte(`"payloadNonce"`)) ||
				!bytes.Contains(ev.Event.UserMetadata, []byte(`"keyVersion"`)) ||
				!bytes.Contains(ev.Event.UserMetadata, []byte(`"originalEventId"`)) {
				t.Fatalf("readable metadata missing fields: %s", meta)
			}
			if bytes.Contains(ev.Event.UserMetadata, []byte(secret)) {
				t.Fatalf("secret leaked in metadata")
			}
		}
	}

	rep := doJSON(t, mux, http.MethodGet, "/internal/v1/game-logs/"+roomID+"/replay?from=0", nil, withAuditCred(nil))
	if rep.Code != http.StatusOK || !bytes.Contains(rep.Body.Bytes(), []byte(secret)) {
		t.Fatalf("authorized replay must decrypt: %d %s", rep.Code, rep.Body.String())
	}
}

func TestIntegration_ReadyFailClosedAndCloseReopen(t *testing.T) {
	url := integrationURL(t)
	t.Setenv("KURRENTDB_URL", url)
	t.Setenv("DEPLOYMENT_ENV", "local")
	t.Setenv("GAME_INTEGRITY_ENVELOPE_PROVIDER", "dev")
	t.Setenv("GAME_INTEGRITY_ENVELOPE_KEY_VERSION", "1")
	t.Setenv("GAME_INTEGRITY_ENVELOPE_DEV_KEYS", "1:00") // invalid
	t.Setenv("GAME_INTEGRITY_ENVELOPE_DEV_MASTER_KEY", "")
	repo, _, mode, reason := resolveRuntime()
	if repo != nil || mode != "" || reason != "envelope_dev_keyring_invalid" {
		t.Fatalf("bad key resolve: repo=%v mode=%q reason=%q", repo != nil, mode, reason)
	}

	t.Setenv("GAME_INTEGRITY_ENVELOPE_PROVIDER", "kms")
	t.Setenv("GAME_INTEGRITY_ENVELOPE_DEV_KEYS", "1:"+integrationMasterKey)
	repo, _, _, reason = resolveRuntime()
	if repo != nil || reason != "envelope_provider_kms_unsupported" {
		t.Fatalf("kms resolve: repo=%v reason=%q", repo != nil, reason)
	}

	srv, krepo := newIntegrationServer(t)
	mux := srv.routes()
	roomID := uniqueRoomID(t)
	_ = doJSON(t, mux, http.MethodPost, "/internal/v1/game-logs/"+roomID+"/append", map[string]any{
		"eventId": uniqueEventID(t, "close"), "expectedRevision": 0, "eventType": "CreateRoom", "payload": map[string]any{},
	}, withRoomCred(nil))
	if err := krepo.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	// Reopen via fresh client/server must restore.
	srv2, krepo2 := newIntegrationServer(t)
	defer krepo2.Close()
	rep := doJSON(t, mux2routes(srv2), http.MethodGet, "/internal/v1/game-logs/"+roomID+"/replay?from=0", nil, withAuditCred(nil))
	if rep.Code != http.StatusOK {
		t.Fatalf("reopen replay: %d %s", rep.Code, rep.Body.String())
	}
}

func mux2routes(s *Server) http.Handler { return s.routes() }

func TestIntegration_PaginationAbovePageBoundary(t *testing.T) {
	repo := openIndependentRepo(t, integrationKeyring(t), 1, 3)
	roomID := domain.RoomID(uniqueRoomID(t))
	ctx := context.Background()
	svc := NewService(repo)
	var lastRev int64
	for i := 0; i < 7; i++ {
		res, rej, err := svc.Append(ctx, AppendRequest{
			RoomID: string(roomID), EventID: uniqueEventID(t, fmt.Sprintf("page-%d", i)),
			ExpectedRevision: lastRev, EventType: "PlayCard",
			Payload: []byte(fmt.Sprintf(`{"n":%d}`, i)),
		})
		if err != nil || rej != nil {
			t.Fatalf("append %d: err=%v rej=%v", i, err, rej)
		}
		lastRev = res.Revision
	}
	rep, rej, err := svc.Replay(ctx, string(roomID), 0)
	if err != nil || rej != nil {
		t.Fatalf("replay: err=%v rej=%v", err, rej)
	}
	if int64(len(rep.Entries)) != 7 || rep.Revision != 7 {
		t.Fatalf("truncated risk: entries=%d rev=%d", len(rep.Entries), rep.Revision)
	}
	dup, rej, err := svc.Append(ctx, AppendRequest{
		RoomID: string(roomID), EventID: uniqueEventID(t, "after-page"),
		ExpectedRevision: 7, EventType: "PlayCard", Payload: []byte(`{"ok":true}`),
	})
	if err != nil || rej != nil || dup.Revision != 8 {
		t.Fatalf("append after full restore failed: %+v rej=%v err=%v", dup, rej, err)
	}
}

func TestIntegration_FirstWriteDEKRaceTwoRepos(t *testing.T) {
	keyring := integrationKeyring(t)
	roomID := uniqueRoomID(t)
	a := openIndependentRepo(t, keyring, 1, defaultReadPageSize)
	b := openIndependentRepo(t, keyring, 1, defaultReadPageSize)
	svcA := NewService(a)
	svcB := NewService(b)
	type out struct {
		kind domain.OutcomeKind
		err  error
		rej  *domain.Rejection
	}
	ch := make(chan out, 2)
	evt := uniqueEventID(t, "race-first")
	appendOnce := func(svc *Service) out {
		var last out
		for attempt := 0; attempt < 8; attempt++ {
			res, rej, err := svc.Append(context.Background(), AppendRequest{
				RoomID: roomID, EventID: evt, ExpectedRevision: 0, EventType: "CreateRoom",
				Payload: []byte(`{"race":true}`),
			})
			last = out{kind: res.Kind, err: err, rej: rej}
			if err == nil && rej == nil {
				return last
			}
			if err != nil && strings.Contains(strings.ToLower(err.Error()), "decrypt") {
				return last
			}
			time.Sleep(50 * time.Millisecond)
		}
		return last
	}
	for _, svc := range []*Service{svcA, svcB} {
		svc := svc
		go func() { ch <- appendOnce(svc) }()
	}
	o1, o2 := <-ch, <-ch
	for _, o := range []out{o1, o2} {
		if o.err != nil {
			t.Fatalf("append failure: %v", o.err)
		}
		if o.rej != nil {
			t.Fatalf("unexpected rejection: %+v", o.rej)
		}
		if o.kind != domain.OutcomeAccepted && o.kind != domain.OutcomeDuplicate {
			t.Fatalf("kind=%v", o.kind)
		}
	}
	rep, rej, err := svcA.Replay(context.Background(), roomID, 0)
	if err != nil || rej != nil || len(rep.Entries) != 1 {
		t.Fatalf("replay after race: err=%v rej=%v entries=%d", err, rej, len(rep.Entries))
	}
}

func TestIntegration_BindingRepairAfterInjectedFailure(t *testing.T) {
	repo := openIndependentRepo(t, integrationKeyring(t), 1, defaultReadPageSize)
	roomID := uniqueRoomID(t)
	gameID := uniqueGameID(t)
	svc := NewService(repo)
	ctx := context.Background()
	_, _, err := svc.Append(ctx, AppendRequest{
		RoomID: roomID, EventID: uniqueEventID(t, "bind-seed"), ExpectedRevision: 0, EventType: "CreateRoom",
	})
	if err != nil {
		t.Fatal(err)
	}
	// Claim happens immediately before deck persist; injected binding failure must not leave a deck or binding.
	repo.failNextBindingAppend = true
	_, _, err = svc.InitializeDeck(ctx, InitializeDeckRequest{RoomID: roomID, GameID: gameID})
	if err == nil {
		t.Fatal("expected injected binding failure")
	}
	_, ok, err := repo.FindByGameID(ctx, domain.GameID(gameID))
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("phantom binding must not exist after failed claim")
	}
	events, readErr := repo.readAllEvents(ctx, deckStreamName(domain.RoomID(roomID), domain.GameID(gameID)))
	if readErr != nil {
		t.Fatal(readErr)
	}
	if len(events) != 0 {
		t.Fatalf("deck must not exist after failed claim, got %d events", len(events))
	}
	// Retry creates claim+deck+finalize.
	if _, rej, err := svc.InitializeDeck(ctx, InitializeDeckRequest{RoomID: roomID, GameID: gameID}); err != nil || rej != nil {
		t.Fatalf("retry init: rej=%v err=%v", rej, err)
	}
	found, ok, err := repo.FindByGameID(ctx, domain.GameID(gameID))
	if err != nil || !ok || string(found) != roomID {
		t.Fatalf("binding not established: found=%q ok=%v err=%v", found, ok, err)
	}
}

func TestIntegration_FinalizeRepairAfterInjectedFailure(t *testing.T) {
	repo := openIndependentRepo(t, integrationKeyring(t), 1, defaultReadPageSize)
	roomID := uniqueRoomID(t)
	gameID := uniqueGameID(t)
	svc := NewService(repo)
	ctx := context.Background()
	if _, rej, err := svc.Append(ctx, AppendRequest{
		RoomID: roomID, EventID: uniqueEventID(t, "fin-seed"), ExpectedRevision: 0, EventType: "CreateRoom",
	}); err != nil || rej != nil {
		t.Fatalf("seed: rej=%v err=%v", rej, err)
	}
	// Claim succeeds; fail the finalize append after deck persist.
	repo.failBindingAppendsRemaining = 2 // claim ok, finalize fails
	_, _, err := svc.InitializeDeck(ctx, InitializeDeckRequest{RoomID: roomID, GameID: gameID})
	if err == nil {
		t.Fatal("expected injected finalize failure")
	}
	events, readErr := repo.readAllEvents(ctx, deckStreamName(domain.RoomID(roomID), domain.GameID(gameID)))
	if readErr != nil {
		t.Fatal(readErr)
	}
	if len(events) == 0 {
		t.Fatal("deck must exist after claim+deck before failed finalize")
	}
	// Read/repair path finalizes the claim for the existing deck.
	_, rej, err := svc.InitializeDeck(ctx, InitializeDeckRequest{RoomID: roomID, GameID: gameID})
	if err != nil {
		t.Fatal(err)
	}
	if rej == nil || rej.Code != domain.RejectConflictingDuplicate {
		t.Fatalf("want conflicting duplicate after durable deck exists, got rej=%v", rej)
	}
	found, ok, err := repo.FindByGameID(ctx, domain.GameID(gameID))
	if err != nil || !ok || string(found) != roomID {
		t.Fatalf("binding not finalized: found=%q ok=%v err=%v", found, ok, err)
	}
}

func TestIntegration_WrongKeyReadinessAndHistoricalRotation(t *testing.T) {
	ctx := context.Background()
	suffix := "rot-" + uniqueSuffix(t)
	// Create sentinel with key 1 on an isolated readiness stream shared by this test.
	repo1 := openIndependentRepoWithSuffix(t, "1:"+integrationMasterKey, 1, defaultReadPageSize, suffix)
	if err := repo1.Ready(ctx); err != nil {
		t.Fatalf("ready key1: %v", err)
	}
	// Wrong-but-valid key must fail readiness against the same sentinel stream.
	wrong := openIndependentRepoWithSuffix(t, "1:"+integrationAltKey, 1, defaultReadPageSize, suffix)
	if err := wrong.Ready(ctx); err == nil {
		t.Fatal("wrong key must fail readiness sentinel decrypt")
	}
	// Historical rotation: old+new passes; new writes use current.
	rotated := openIndependentRepoWithSuffix(t, "1:"+integrationMasterKey+",2:"+integrationAltKey, 2, defaultReadPageSize, suffix)
	if err := rotated.Ready(ctx); err != nil {
		t.Fatalf("rotated ready: %v", err)
	}
	roomID := uniqueRoomID(t)
	svc := NewService(rotated)
	res, rej, err := svc.Append(ctx, AppendRequest{
		RoomID: roomID, EventID: uniqueEventID(t, "rot"), ExpectedRevision: 0, EventType: "CreateRoom",
	})
	if err != nil || rej != nil || res.Revision != 1 {
		t.Fatalf("append under current: %+v rej=%v err=%v", res, rej, err)
	}
	// Removing old fails if old sentinel/events exist.
	onlyNew := openIndependentRepoWithSuffix(t, "2:"+integrationAltKey, 2, defaultReadPageSize, suffix)
	if err := onlyNew.Ready(ctx); err == nil {
		t.Fatal("removing historical key must fail readiness against old sentinel")
	}
}

func TestIntegration_CancelTombstoneAndReopenDrawFidelity(t *testing.T) {
	srv, krepo := newIntegrationServer(t)
	mux := srv.routes()
	roomID := uniqueRoomID(t)
	gameID := uniqueGameID(t)
	path := "/internal/v1/game-logs/" + roomID + "/deck-operations"
	init := doJSON(t, mux, http.MethodPost, path, map[string]any{"operation": "initialize", "gameId": gameID}, withRoomCred(nil))
	if init.Code != http.StatusOK {
		t.Fatalf("init: %d %s", init.Code, init.Body.String())
	}
	res := doJSON(t, mux, http.MethodPost, path, map[string]any{
		"operation": "reserve_draw", "gameId": gameID, "operationId": "op-tomb:draw", "count": 2,
	}, withRoomCred(nil))
	id := decodeJSON(t, res)["reservationId"].(string)
	_ = doJSON(t, mux, http.MethodPost, path, map[string]any{
		"operation": "cancel", "gameId": gameID, "reservationId": id,
	}, withRoomCred(nil))
	reuse := doJSON(t, mux, http.MethodPost, path, map[string]any{
		"operation": "reserve_draw", "gameId": gameID, "operationId": "op-tomb:draw", "count": 2,
	}, withRoomCred(nil))
	if reuse.Code != http.StatusConflict {
		t.Fatalf("tombstone reuse: %d %s", reuse.Code, reuse.Body.String())
	}

	drawRes := doJSON(t, mux, http.MethodPost, path, map[string]any{
		"operation": "reserve_draw", "gameId": gameID, "operationId": "op-draw:draw", "count": 1,
	}, withRoomCred(nil))
	if drawRes.Code != http.StatusOK {
		t.Fatalf("draw reserve: %d %s", drawRes.Code, drawRes.Body.String())
	}
	drawBody := decodeJSON(t, drawRes)
	drawID := drawBody["reservationId"].(string)
	beforeCards := mustJSON(t, drawBody["cards"])
	confirm := doJSON(t, mux, http.MethodPost, path, map[string]any{
		"operation": "confirm", "gameId": gameID, "reservationId": drawID,
	}, withRoomCred(nil))
	if confirm.Code != http.StatusOK {
		t.Fatalf("confirm: %d %s", confirm.Code, confirm.Body.String())
	}
	_ = krepo.Close()
	srv2, krepo2 := newIntegrationServer(t)
	defer krepo2.Close()
	dup := doJSON(t, srv2.routes(), http.MethodPost, path, map[string]any{
		"operation": "reserve_draw", "gameId": gameID, "operationId": "op-draw:draw", "count": 1,
	}, withRoomCred(nil))
	if dup.Code != http.StatusOK {
		t.Fatalf("dup after reopen: %d %s", dup.Code, dup.Body.String())
	}
	body := decodeJSON(t, dup)
	if body["kind"] != "duplicate" && body["kind"] != "accepted" {
		t.Fatalf("kind=%v", body["kind"])
	}
	if mustJSON(t, body["cards"]) != beforeCards {
		t.Fatalf("cards mismatch after reopen")
	}
	reconfirm := doJSON(t, srv2.routes(), http.MethodPost, path, map[string]any{
		"operation": "confirm", "gameId": gameID, "reservationId": drawID,
	}, withRoomCred(nil))
	if reconfirm.Code != http.StatusOK {
		t.Fatalf("reconfirm: %d %s", reconfirm.Code, reconfirm.Body.String())
	}
}

func TestIntegration_ContextCancellationPreventsAppend(t *testing.T) {
	repo := openIndependentRepo(t, integrationKeyring(t), 1, defaultReadPageSize)
	svc := NewService(repo)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, err := svc.Append(ctx, AppendRequest{
		RoomID: uniqueRoomID(t), EventID: uniqueEventID(t, "cancel"), ExpectedRevision: 0, EventType: "CreateRoom",
	})
	if err == nil {
		t.Fatal("expected cancellation error")
	}
}

func TestIntegration_AuditEventRecorded(t *testing.T) {
	srv, _ := newIntegrationServer(t)
	mux := srv.routes()
	roomID := uniqueRoomID(t)
	_ = doJSON(t, mux, http.MethodPost, "/internal/v1/game-logs/"+roomID+"/append", map[string]any{
		"eventId": uniqueEventID(t, "aud"), "expectedRevision": 0, "eventType": "CreateRoom",
	}, withRoomCred(nil))
	rep := doJSON(t, mux, http.MethodGet, "/internal/v1/game-logs/"+roomID+"/replay?from=0", nil, withAuditCred(map[string]string{
		"X-Correlation-Id": "corr-int-audit",
	}))
	if rep.Code != http.StatusOK {
		t.Fatalf("replay: %d %s", rep.Code, rep.Body.String())
	}
}

type countingKeyProvider struct {
	inner       KeyProvider
	unwrapCalls int
	wrapCalls   int
}

func (c *countingKeyProvider) KeyVersion() int { return c.inner.KeyVersion() }
func (c *countingKeyProvider) Ready(ctx context.Context) error {
	return c.inner.Ready(ctx)
}
func (c *countingKeyProvider) WrapDEK(ctx context.Context, dek []byte) ([]byte, []byte, error) {
	c.wrapCalls++
	return c.inner.WrapDEK(ctx, dek)
}
func (c *countingKeyProvider) UnwrapDEK(ctx context.Context, keyVersion int, wrapNonce, wrapped []byte) ([]byte, error) {
	c.unwrapCalls++
	return c.inner.UnwrapDEK(ctx, keyVersion, wrapNonce, wrapped)
}

func TestIntegration_StableStreamWrappingReusesMetadata(t *testing.T) {
	url := integrationURL(t)
	t.Setenv("DEPLOYMENT_ENV", "test")
	t.Setenv("GAME_INTEGRITY_READINESS_STREAM_SUFFIX", "wrap-"+uniqueSuffix(t))
	keys, err := ParseDevKeyring("1:" + integrationMasterKey)
	if err != nil {
		t.Fatal(err)
	}
	base, err := NewDevKeyProviderFromKeyring(keys, 1)
	if err != nil {
		t.Fatal(err)
	}
	counting := &countingKeyProvider{inner: base}
	client, err := openKurrentClient(url)
	if err != nil {
		t.Fatal(err)
	}
	repo := NewKurrentStreamRepositoryWithPageSize(client, counting, defaultReadPageSize)
	t.Cleanup(func() { _ = repo.Close() })
	svc := NewService(repo)
	ctx := context.Background()
	roomID := uniqueRoomID(t)
	for i := 0; i < 3; i++ {
		res, rej, err := svc.Append(ctx, AppendRequest{
			RoomID: roomID, EventID: uniqueEventID(t, fmt.Sprintf("w%d", i)),
			ExpectedRevision: int64(i), EventType: "PlayCard",
		})
		if err != nil || rej != nil {
			t.Fatalf("append %d: res=%+v rej=%v err=%v", i, res, rej, err)
		}
	}
	if counting.wrapCalls != 1 {
		t.Fatalf("expected exactly one WrapDEK for stream lifetime, got %d", counting.wrapCalls)
	}
	stream := roomStreamName(domain.RoomID(roomID))
	events, err := repo.readAllEvents(ctx, stream)
	if err != nil || len(events) != 3 {
		t.Fatalf("events=%d err=%v", len(events), err)
	}
	firstMeta, err := parseEnvelopeMetadata(events[0].Event.UserMetadata)
	if err != nil {
		t.Fatal(err)
	}
	for i, ev := range events {
		meta, err := parseEnvelopeMetadata(ev.Event.UserMetadata)
		if err != nil {
			t.Fatal(err)
		}
		if meta.KeyVersion != firstMeta.KeyVersion || meta.WrappedDEK != firstMeta.WrappedDEK || meta.WrapNonce != firstMeta.WrapNonce {
			t.Fatalf("event %d wrapper metadata changed: %+v vs %+v", i, meta, firstMeta)
		}
	}
	// Fresh repo replay: unwrap once for the stream, then cache hits.
	freshClient, err := openKurrentClient(url)
	if err != nil {
		t.Fatal(err)
	}
	counting2 := &countingKeyProvider{inner: base}
	fresh := NewKurrentStreamRepositoryWithPageSize(freshClient, counting2, defaultReadPageSize)
	t.Cleanup(func() { _ = fresh.Close() })
	freshSvc := NewService(fresh)
	_, rej, err := freshSvc.Replay(ctx, roomID, 0)
	if err != nil || rej != nil {
		t.Fatalf("replay: rej=%v err=%v", rej, err)
	}
	if counting2.unwrapCalls != 1 {
		t.Fatalf("replay must unwrap once per stream, got %d", counting2.unwrapCalls)
	}
}

func TestIntegration_LoadRoomStateRejectsWrongRoomMetadata(t *testing.T) {
	repo := openIndependentRepo(t, integrationKeyring(t), 1, defaultReadPageSize)
	ctx := context.Background()
	requested := domain.RoomID(uniqueRoomID(t))
	stream := roomStreamName(requested)
	forgedRoom := "room-forged-" + uniqueSuffix(t)
	eventID := uniqueEventID(t, "forge")

	if err := appendEncryptedJSONWithRoomOverride(ctx, repo, stream, forgedRoom, "", eventID, "PlayCard", roomPlaintextV1{
		EventID:   eventID,
		EventType: "PlayCard",
		Payload:   json.RawMessage(`{"marker":"forged-room-meta"}`),
	}, 1, kurrentdb.NoStream{}); err != nil {
		t.Fatalf("forge append: %v", err)
	}

	_, _, err := repo.loadRoomState(ctx, requested, stream)
	if err == nil {
		t.Fatal("loadRoomState must fail closed for self-consistent wrong roomId metadata")
	}
	err2 := repo.WithExistingRoom(ctx, requested, func(*RoomState) error { return nil })
	if err2 == nil {
		t.Fatal("WithExistingRoom must fail closed for self-consistent wrong roomId metadata")
	}
}

func TestIntegration_DeckLoadRejectsChangedWrapperIdentity(t *testing.T) {
	repo := openIndependentRepo(t, integrationKeyring(t), 1, defaultReadPageSize)
	svc := NewService(repo)
	ctx := context.Background()
	roomID := uniqueRoomID(t)
	gameID := uniqueGameID(t)

	if _, rej, err := svc.Append(ctx, AppendRequest{
		RoomID: roomID, EventID: uniqueEventID(t, "deck-room"), ExpectedRevision: 0, EventType: "CreateRoom",
	}); err != nil || rej != nil {
		t.Fatalf("room seed: rej=%v err=%v", rej, err)
	}
	if _, rej, err := svc.InitializeDeck(ctx, InitializeDeckRequest{RoomID: roomID, GameID: gameID}); err != nil || rej != nil {
		t.Fatalf("init: rej=%v err=%v", rej, err)
	}

	stream := deckStreamName(domain.RoomID(roomID), domain.GameID(gameID))
	events, err := repo.readAllEvents(ctx, stream)
	if err != nil || len(events) != 1 {
		t.Fatalf("deck events=%d err=%v", len(events), err)
	}
	firstMeta, err := parseEnvelopeMetadata(events[0].Event.UserMetadata)
	if err != nil {
		t.Fatal(err)
	}
	// Decrypt the legitimate first snapshot so the tamper event is a valid later snapshot
	// under a different wrapper (proves fail-closed on identity, not on snapshot shape).
	plain, _, err := repo.decryptEvent(ctx, stream, 0, events[0])
	if err != nil {
		t.Fatalf("decrypt first: %v", err)
	}
	var snap DeckStateSnapshotV1
	if err := json.Unmarshal(plain, &snap); err != nil {
		t.Fatal(err)
	}

	// Clear cache and append a later snapshot under an independent wrapper identity.
	repo.mu.Lock()
	delete(repo.dekCache, stream)
	repo.mu.Unlock()

	if err := appendEncryptedJSONWithFreshDEK(ctx, repo, stream, domain.RoomID(roomID), domain.GameID(gameID),
		"deck-snap-tamper-2", deckSnapshotEventType, snap, 2, kurrentdb.StreamRevision{Value: 0}); err != nil {
		t.Fatalf("tamper append: %v", err)
	}

	fresh := openIndependentRepo(t, integrationKeyring(t), 1, defaultReadPageSize)
	_, _, err = fresh.loadDeckState(ctx, domain.RoomID(roomID), domain.GameID(gameID), stream)
	if err == nil {
		t.Fatal("loadDeckState must fail when latest wrapper identity differs from first")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "key identity") &&
		!strings.Contains(strings.ToLower(err.Error()), "stream key") {
		t.Fatalf("expected key identity error, got: %v", err)
	}

	// Sanity: first event wrapper is still the original identity.
	events2, err := fresh.readAllEvents(ctx, stream)
	if err != nil || len(events2) != 2 {
		t.Fatalf("after tamper events=%d err=%v", len(events2), err)
	}
	m0, _ := parseEnvelopeMetadata(events2[0].Event.UserMetadata)
	m1, _ := parseEnvelopeMetadata(events2[1].Event.UserMetadata)
	if m0.WrappedDEK != firstMeta.WrappedDEK {
		t.Fatal("first event wrap unexpectedly changed")
	}
	if m1.WrappedDEK == m0.WrappedDEK && m1.WrapNonce == m0.WrapNonce {
		t.Fatal("tamper append did not change wrapper identity")
	}
}

func TestIntegration_DeckStableWrapperIdentityReopen(t *testing.T) {
	repo := openIndependentRepo(t, integrationKeyring(t), 1, defaultReadPageSize)
	svc := NewService(repo)
	ctx := context.Background()
	roomID := uniqueRoomID(t)
	gameID := uniqueGameID(t)

	if _, rej, err := svc.Append(ctx, AppendRequest{
		RoomID: roomID, EventID: uniqueEventID(t, "stable-room"), ExpectedRevision: 0, EventType: "CreateRoom",
	}); err != nil || rej != nil {
		t.Fatalf("room seed: rej=%v err=%v", rej, err)
	}
	if _, rej, err := svc.InitializeDeck(ctx, InitializeDeckRequest{RoomID: roomID, GameID: gameID}); err != nil || rej != nil {
		t.Fatalf("init: rej=%v err=%v", rej, err)
	}
	deal, rej, err := svc.ReserveDeal(ctx, ReserveDealRequest{
		RoomID: roomID, GameID: gameID, OperationID: "op-stable-deal",
		Seats: []string{"a", "b"}, CardsPerHand: 7,
	})
	if err != nil || rej != nil {
		t.Fatalf("deal: rej=%v err=%v", rej, err)
	}
	if _, rej, err := svc.ConfirmReservation(ctx, roomID, gameID, deal.ReservationID); err != nil || rej != nil {
		t.Fatalf("confirm: rej=%v err=%v", rej, err)
	}

	stream := deckStreamName(domain.RoomID(roomID), domain.GameID(gameID))
	events, err := repo.readAllEvents(ctx, stream)
	if err != nil || len(events) < 2 {
		t.Fatalf("want >=2 deck snapshots, got %d err=%v", len(events), err)
	}
	firstMeta, err := parseEnvelopeMetadata(events[0].Event.UserMetadata)
	if err != nil {
		t.Fatal(err)
	}
	for i, ev := range events {
		meta, err := parseEnvelopeMetadata(ev.Event.UserMetadata)
		if err != nil {
			t.Fatal(err)
		}
		if meta.KeyVersion != firstMeta.KeyVersion || meta.WrappedDEK != firstMeta.WrappedDEK || meta.WrapNonce != firstMeta.WrapNonce {
			t.Fatalf("event %d wrapper identity changed", i)
		}
	}

	fresh := openIndependentRepo(t, integrationKeyring(t), 1, defaultReadPageSize)
	st, _, err := fresh.loadDeckState(ctx, domain.RoomID(roomID), domain.GameID(gameID), stream)
	if err != nil {
		t.Fatalf("reopen loadDeckState: %v", err)
	}
	if st == nil || st.Deck == nil {
		t.Fatal("expected restored deck")
	}
}

// appendEncryptedJSONWithRoomOverride seals a self-consistent envelope using forgedRoomID in metadata
// while writing to the given stream (used to prove loadRoomState fail-closed on room identity).
func appendEncryptedJSONWithRoomOverride(ctx context.Context, r *KurrentStreamRepository, stream, forgedRoomID, gameID, originalEventID, originalEventType string, body any, domainRev uint64, expected kurrentdb.StreamState) error {
	return appendEncryptedJSONForged(ctx, r, stream, forgedRoomID, gameID, originalEventID, originalEventType, body, domainRev, expected, false)
}

// appendEncryptedJSONWithFreshDEK forces a new DEK wrap even when the stream already has events.
func appendEncryptedJSONWithFreshDEK(ctx context.Context, r *KurrentStreamRepository, stream string, roomID domain.RoomID, gameID domain.GameID, originalEventID, originalEventType string, body any, domainRev uint64, expected kurrentdb.StreamState) error {
	return appendEncryptedJSONForged(ctx, r, stream, string(roomID), string(gameID), originalEventID, originalEventType, body, domainRev, expected, true)
}

func appendEncryptedJSONForged(ctx context.Context, r *KurrentStreamRepository, stream, roomID, gameID, originalEventID, originalEventType string, body any, domainRev uint64, expected kurrentdb.StreamState, forceFreshDEK bool) error {
	plain, err := json.Marshal(body)
	if err != nil {
		return err
	}
	var dek, wrapped, wrapNonce []byte
	var keyVersion int
	if forceFreshDEK {
		dek = make([]byte, dekSizeBytes)
		if _, err := io.ReadFull(rand.Reader, dek); err != nil {
			return err
		}
		wrapped, wrapNonce, err = r.provider.WrapDEK(ctx, dek)
		if err != nil {
			return err
		}
		keyVersion = r.provider.KeyVersion()
	} else {
		dek, wrapped, wrapNonce, keyVersion, err = r.streamDEK(ctx, stream, true)
		if err != nil {
			return err
		}
	}
	kurrentRev := uint64(0)
	switch s := expected.(type) {
	case kurrentdb.NoStream:
		kurrentRev = 0
	case kurrentdb.StreamRevision:
		kurrentRev = s.Value + 1
	}
	payloadNonce := make([]byte, gcmNonceSize)
	if _, err := io.ReadFull(rand.Reader, payloadNonce); err != nil {
		return err
	}
	eventUUID := deterministicEventUUID(stream, originalEventID)
	meta := envelopeMetadataV1{
		EnvelopeVersion:   envelopeVersionV1,
		KeyVersion:        keyVersion,
		WrappedDEK:        hexBytes(wrapped),
		WrapNonce:         hexBytes(wrapNonce),
		PayloadNonce:      hexBytes(payloadNonce),
		OriginalEventID:   originalEventID,
		OriginalEventType: originalEventType,
		Stream:            stream,
		RoomID:            roomID,
		GameID:            gameID,
		KurrentRevision:   kurrentRev,
		DomainRevision:    domainRev,
		EventUUID:         eventUUID.String(),
	}
	ct, err := SealPayloadWithNonce(dek, meta.canonicalAAD(), payloadNonce, plain)
	if err != nil {
		return err
	}
	metaBytes, err := meta.marshal()
	if err != nil {
		return err
	}
	event := kurrentdb.EventData{
		EventID:     eventUUID,
		EventType:   originalEventType,
		ContentType: kurrentdb.ContentTypeBinary,
		Data:        ct,
		Metadata:    metaBytes,
	}
	opCtx, cancel := context.WithTimeout(ctx, kurrentOpTimeout)
	defer cancel()
	_, err = r.client.AppendToStream(opCtx, stream, kurrentdb.AppendToStreamOptions{StreamState: expected}, event)
	return err
}

func TestIntegration_AuditRecordsActualKeyVersions(t *testing.T) {
	sink := &MemoryAuditRecorder{}
	url := integrationURL(t)
	t.Setenv("DEPLOYMENT_ENV", "test")
	t.Setenv("GAME_INTEGRITY_READINESS_STREAM_SUFFIX", "auditkv-"+uniqueSuffix(t))
	keys, err := ParseDevKeyring("1:" + integrationMasterKey)
	if err != nil {
		t.Fatal(err)
	}
	p, err := NewDevKeyProviderFromKeyring(keys, 1)
	if err != nil {
		t.Fatal(err)
	}
	client, err := openKurrentClient(url)
	if err != nil {
		t.Fatal(err)
	}
	repo := NewKurrentStreamRepositoryWithPageSize(client, p, defaultReadPageSize)
	t.Cleanup(func() { _ = repo.Close() })
	srv := NewServerWithAudit(repo, sink, testRoomCredential, testAuditCredential, "durable", "")
	mux := srv.routes()
	roomID := uniqueRoomID(t)
	_ = doJSON(t, mux, http.MethodPost, "/internal/v1/game-logs/"+roomID+"/append", map[string]any{
		"eventId": uniqueEventID(t, "kv"), "expectedRevision": 0, "eventType": "CreateRoom",
	}, withRoomCred(nil))
	rep := doJSON(t, mux, http.MethodGet, "/internal/v1/game-logs/"+roomID+"/replay?from=0", nil, withAuditCred(nil))
	if rep.Code != http.StatusOK {
		t.Fatalf("replay: %d %s", rep.Code, rep.Body.String())
	}
	recs := sink.Snapshot()
	if len(recs) != 1 {
		t.Fatalf("audit records=%d", len(recs))
	}
	if len(recs[0].KeyVersions) != 1 || recs[0].KeyVersions[0] != 1 || recs[0].KeyVersion != 1 {
		t.Fatalf("want keyVersions=[1] keyVersion=1, got %+v", recs[0])
	}
	rep2 := doJSON(t, mux, http.MethodGet, "/internal/v1/game-logs/"+roomID+"/replay?from=0", nil, withAuditCred(nil))
	if rep2.Code != http.StatusOK {
		t.Fatalf("replay2: %d", rep2.Code)
	}
	recs = sink.Snapshot()
	if len(recs) != 2 || recs[0].EventID == recs[1].EventID {
		t.Fatalf("repeated attempts must be unique durable records: %+v", recs)
	}
}

func TestIntegration_ConflictingRoomCannotCreateOrphanDeck(t *testing.T) {
	repo := openIndependentRepo(t, integrationKeyring(t), 1, defaultReadPageSize)
	svc := NewService(repo)
	ctx := context.Background()
	roomA := uniqueRoomID(t)
	roomB := uniqueRoomID(t)
	gameID := uniqueGameID(t)

	for _, room := range []string{roomA, roomB} {
		if _, rej, err := svc.Append(ctx, AppendRequest{
			RoomID: room, EventID: uniqueEventID(t, "orphan-seed"), ExpectedRevision: 0, EventType: "CreateRoom",
		}); err != nil || rej != nil {
			t.Fatalf("seed %s: rej=%v err=%v", room, rej, err)
		}
	}
	if _, rej, err := svc.InitializeDeck(ctx, InitializeDeckRequest{RoomID: roomA, GameID: gameID}); err != nil || rej != nil {
		t.Fatalf("init A: rej=%v err=%v", rej, err)
	}
	_, _, err := svc.InitializeDeck(ctx, InitializeDeckRequest{RoomID: roomB, GameID: gameID})
	if err == nil {
		t.Fatal("conflicting room must fail before creating a deck")
	}
	// Orphan check: room B deck stream must not exist.
	events, readErr := repo.readAllEvents(ctx, deckStreamName(domain.RoomID(roomB), domain.GameID(gameID)))
	if readErr != nil {
		t.Fatal(readErr)
	}
	if len(events) != 0 {
		t.Fatalf("conflicting room must not persist orphan deck events, got %d", len(events))
	}
	found, ok, err := repo.FindByGameID(ctx, domain.GameID(gameID))
	if err != nil || !ok || string(found) != roomA {
		t.Fatalf("binding must remain room A: found=%q ok=%v err=%v", found, ok, err)
	}
}

func TestIntegration_CallbackErrorLeavesBindingUnboundForOtherRoom(t *testing.T) {
	repo := openIndependentRepo(t, integrationKeyring(t), 1, defaultReadPageSize)
	svc := NewService(repo)
	ctx := context.Background()
	roomA := uniqueRoomID(t)
	roomB := uniqueRoomID(t)
	gameID := uniqueGameID(t)
	for _, room := range []string{roomA, roomB} {
		if _, rej, err := svc.Append(ctx, AppendRequest{
			RoomID: room, EventID: uniqueEventID(t, "cb-err-seed"), ExpectedRevision: 0, EventType: "CreateRoom",
		}); err != nil || rej != nil {
			t.Fatalf("seed %s: rej=%v err=%v", room, rej, err)
		}
	}
	err := repo.WithDeck(ctx, domain.RoomID(roomA), domain.GameID(gameID), true, func(*DeckState) error {
		return fmt.Errorf("injected callback failure")
	})
	if err == nil {
		t.Fatal("expected callback failure")
	}
	st, err := repo.loadBindingState(ctx, domain.GameID(gameID))
	if err != nil {
		t.Fatal(err)
	}
	if !st.IsEmptyOrReleased() {
		t.Fatalf("callback error must leave binding unbound/released, got %+v", st)
	}
	events, readErr := repo.readAllEvents(ctx, deckStreamName(domain.RoomID(roomA), domain.GameID(gameID)))
	if readErr != nil {
		t.Fatal(readErr)
	}
	if len(events) != 0 {
		t.Fatalf("callback error must not persist deck, got %d events", len(events))
	}
	if _, rej, err := svc.InitializeDeck(ctx, InitializeDeckRequest{RoomID: roomB, GameID: gameID}); err != nil || rej != nil {
		t.Fatalf("other room must initialize after unbound callback failure: rej=%v err=%v", rej, err)
	}
	found, ok, err := repo.FindByGameID(ctx, domain.GameID(gameID))
	if err != nil || !ok || string(found) != roomB {
		t.Fatalf("binding after other-room init: found=%q ok=%v err=%v", found, ok, err)
	}
}

func TestIntegration_NoOpCreateLeavesBindingUnboundForOtherRoom(t *testing.T) {
	repo := openIndependentRepo(t, integrationKeyring(t), 1, defaultReadPageSize)
	svc := NewService(repo)
	ctx := context.Background()
	roomA := uniqueRoomID(t)
	roomB := uniqueRoomID(t)
	gameID := uniqueGameID(t)
	for _, room := range []string{roomA, roomB} {
		if _, rej, err := svc.Append(ctx, AppendRequest{
			RoomID: room, EventID: uniqueEventID(t, "noop-seed"), ExpectedRevision: 0, EventType: "CreateRoom",
		}); err != nil || rej != nil {
			t.Fatalf("seed %s: rej=%v err=%v", room, rej, err)
		}
	}
	if err := repo.WithDeck(ctx, domain.RoomID(roomA), domain.GameID(gameID), true, func(*DeckState) error {
		return nil // no mutation
	}); err != nil {
		t.Fatalf("no-op create: %v", err)
	}
	st, err := repo.loadBindingState(ctx, domain.GameID(gameID))
	if err != nil {
		t.Fatal(err)
	}
	if !st.IsEmptyOrReleased() {
		t.Fatalf("no-op create must leave binding unbound/released, got %+v", st)
	}
	events, readErr := repo.readAllEvents(ctx, deckStreamName(domain.RoomID(roomA), domain.GameID(gameID)))
	if readErr != nil {
		t.Fatal(readErr)
	}
	if len(events) != 0 {
		t.Fatalf("no-op create must not persist deck, got %d events", len(events))
	}
	if _, rej, err := svc.InitializeDeck(ctx, InitializeDeckRequest{RoomID: roomB, GameID: gameID}); err != nil || rej != nil {
		t.Fatalf("other room must initialize after unbound no-op: rej=%v err=%v", rej, err)
	}
	found, ok, err := repo.FindByGameID(ctx, domain.GameID(gameID))
	if err != nil || !ok || string(found) != roomB {
		t.Fatalf("binding after other-room init: found=%q ok=%v err=%v", found, ok, err)
	}
}

func TestIntegration_DeckAppendFailureAllowsSameRoomRecovery(t *testing.T) {
	repo := openIndependentRepo(t, integrationKeyring(t), 1, defaultReadPageSize)
	svc := NewService(repo)
	ctx := context.Background()
	roomID := uniqueRoomID(t)
	gameID := uniqueGameID(t)
	if _, rej, err := svc.Append(ctx, AppendRequest{
		RoomID: roomID, EventID: uniqueEventID(t, "reclaim-seed"), ExpectedRevision: 0, EventType: "CreateRoom",
	}); err != nil || rej != nil {
		t.Fatalf("seed: rej=%v err=%v", rej, err)
	}
	repo.failNextDeckAppend = true
	_, _, err := svc.InitializeDeck(ctx, InitializeDeckRequest{RoomID: roomID, GameID: gameID})
	if err == nil {
		t.Fatal("expected injected deck append failure")
	}
	events, readErr := repo.readAllEvents(ctx, deckStreamName(domain.RoomID(roomID), domain.GameID(gameID)))
	if readErr != nil {
		t.Fatal(readErr)
	}
	if len(events) != 0 {
		t.Fatalf("deck must not exist after injected append failure, got %d events", len(events))
	}
	st, err := repo.loadBindingState(ctx, domain.GameID(gameID))
	if err != nil {
		t.Fatal(err)
	}
	if !st.IsEmptyOrReleased() {
		t.Fatalf("known append failure must release claim, got %+v", st)
	}
	// Same room recovers after release: new claim token, then active.
	if _, rej, err := svc.InitializeDeck(ctx, InitializeDeckRequest{RoomID: roomID, GameID: gameID}); err != nil || rej != nil {
		t.Fatalf("recovery init: rej=%v err=%v", rej, err)
	}
	found, ok, err := repo.FindByGameID(ctx, domain.GameID(gameID))
	if err != nil || !ok || string(found) != roomID {
		t.Fatalf("binding after recovery: found=%q ok=%v err=%v", found, ok, err)
	}
}

func TestIntegration_ConcurrentFirstDeckInitUsesBindingClaim(t *testing.T) {
	keyring := integrationKeyring(t)
	suffix := "bindrace-" + uniqueSuffix(t)
	a := openIndependentRepoWithSuffix(t, keyring, 1, defaultReadPageSize, suffix+"-a")
	b := openIndependentRepoWithSuffix(t, keyring, 1, defaultReadPageSize, suffix+"-b")
	svcA := NewService(a)
	svcB := NewService(b)
	ctx := context.Background()
	roomID := uniqueRoomID(t)
	gameID := uniqueGameID(t)
	if _, rej, err := svcA.Append(ctx, AppendRequest{
		RoomID: roomID, EventID: uniqueEventID(t, "race-bind"), ExpectedRevision: 0, EventType: "CreateRoom",
	}); err != nil || rej != nil {
		t.Fatalf("seed: rej=%v err=%v", rej, err)
	}
	type out struct {
		err error
		rej *domain.Rejection
	}
	initOnce := func(svc *Service) out {
		var last out
		for attempt := 0; attempt < 16; attempt++ {
			_, rej, err := svc.InitializeDeck(ctx, InitializeDeckRequest{RoomID: roomID, GameID: gameID})
			last = out{err: err, rej: rej}
			if err == nil {
				return last
			}
			msg := strings.ToLower(err.Error())
			// Transient cluster aborts, revision races, or losing an in-flight same-room claim.
			if strings.Contains(msg, "timeout") ||
				strings.Contains(msg, "aborted") ||
				strings.Contains(msg, "conflicting gameid binding") ||
				isRevisionConflict(err) {
				time.Sleep(50 * time.Millisecond)
				continue
			}
			return last
		}
		return last
	}
	ch := make(chan out, 2)
	go func() { ch <- initOnce(svcA) }()
	go func() { ch <- initOnce(svcB) }()
	o1, o2 := <-ch, <-ch
	accepted := 0
	dupes := 0
	for _, o := range []out{o1, o2} {
		if o.err != nil {
			t.Fatalf("same-room concurrent init must settle: %v", o.err)
		}
		if o.rej == nil {
			accepted++
			continue
		}
		if o.rej.Code != domain.RejectConflictingDuplicate {
			t.Fatalf("loser must be conflicting duplicate, got %+v", o.rej)
		}
		dupes++
	}
	events, err := a.readAllEvents(ctx, deckStreamName(domain.RoomID(roomID), domain.GameID(gameID)))
	if err != nil {
		t.Fatal(err)
	}
	if len(events) == 0 {
		t.Fatal("deck must exist after concurrent init")
	}
	// Exactly one accept, or both clients timed out after a durable write and only saw duplicates.
	if accepted != 1 && !(accepted == 0 && dupes == 2) {
		t.Fatalf("want one accept (or two post-success dupes), got accepted=%d dupes=%d o1=%+v o2=%+v", accepted, dupes, o1, o2)
	}
	if accepted > 1 {
		t.Fatalf("at most one concurrent init may accept, got %d", accepted)
	}
	found, ok, err := a.FindByGameID(ctx, domain.GameID(gameID))
	if err != nil || !ok || string(found) != roomID {
		t.Fatalf("binding=%q ok=%v err=%v", found, ok, err)
	}
}

func TestIntegration_KnownFailedAppendThenOtherRoomRecovery(t *testing.T) {
	repo := openIndependentRepo(t, integrationKeyring(t), 1, defaultReadPageSize)
	svc := NewService(repo)
	ctx := context.Background()
	roomA := uniqueRoomID(t)
	roomB := uniqueRoomID(t)
	gameID := uniqueGameID(t)
	for _, room := range []string{roomA, roomB} {
		if _, rej, err := svc.Append(ctx, AppendRequest{
			RoomID: room, EventID: uniqueEventID(t, "reclaim-room"), ExpectedRevision: 0, EventType: "CreateRoom",
		}); err != nil || rej != nil {
			t.Fatalf("seed %s: rej=%v err=%v", room, rej, err)
		}
	}
	repo.failNextDeckAppend = true
	if _, _, err := svc.InitializeDeck(ctx, InitializeDeckRequest{RoomID: roomA, GameID: gameID}); err == nil {
		t.Fatal("expected injected deck failure for room A claim")
	}
	st, err := repo.loadBindingState(ctx, domain.GameID(gameID))
	if err != nil {
		t.Fatal(err)
	}
	if !st.IsEmptyOrReleased() {
		t.Fatalf("known failure must release before other-room recovery, got %+v", st)
	}
	// After release, room B may claim with expected revision.
	if _, rej, err := svc.InitializeDeck(ctx, InitializeDeckRequest{RoomID: roomB, GameID: gameID}); err != nil || rej != nil {
		t.Fatalf("recovery by B: rej=%v err=%v", rej, err)
	}
	found, ok, err := repo.FindByGameID(ctx, domain.GameID(gameID))
	if err != nil || !ok || string(found) != roomB {
		t.Fatalf("binding after recovery: found=%q ok=%v err=%v", found, ok, err)
	}
	eventsA, err := repo.readAllEvents(ctx, deckStreamName(domain.RoomID(roomA), domain.GameID(gameID)))
	if err != nil {
		t.Fatal(err)
	}
	if len(eventsA) != 0 {
		t.Fatalf("room A must not have orphan deck after recovery, got %d", len(eventsA))
	}
}

func TestIntegration_ConcurrentConflictingRoomsOnlyOneDeck(t *testing.T) {
	keyring := integrationKeyring(t)
	suffix := "conflictrace-" + uniqueSuffix(t)
	a := openIndependentRepoWithSuffix(t, keyring, 1, defaultReadPageSize, suffix+"-a")
	b := openIndependentRepoWithSuffix(t, keyring, 1, defaultReadPageSize, suffix+"-b")
	svcA := NewService(a)
	svcB := NewService(b)
	ctx := context.Background()
	roomA := uniqueRoomID(t)
	roomB := uniqueRoomID(t)
	gameID := uniqueGameID(t)
	for _, pair := range []struct {
		svc  *Service
		room string
	}{{svcA, roomA}, {svcB, roomB}} {
		if _, rej, err := pair.svc.Append(ctx, AppendRequest{
			RoomID: pair.room, EventID: uniqueEventID(t, "conflict-seed"), ExpectedRevision: 0, EventType: "CreateRoom",
		}); err != nil || rej != nil {
			t.Fatalf("seed %s: rej=%v err=%v", pair.room, rej, err)
		}
	}
	type out struct {
		room string
		err  error
		rej  *domain.Rejection
	}
	initOnce := func(svc *Service, room string) out {
		_, rej, err := svc.InitializeDeck(ctx, InitializeDeckRequest{RoomID: room, GameID: gameID})
		return out{room: room, err: err, rej: rej}
	}
	ch := make(chan out, 2)
	go func() { ch <- initOnce(svcA, roomA) }()
	go func() { ch <- initOnce(svcB, roomB) }()
	o1, o2 := <-ch, <-ch
	wins := 0
	for _, o := range []out{o1, o2} {
		if o.err == nil && o.rej == nil {
			wins++
			continue
		}
		if o.err == nil {
			t.Fatalf("loser must not accept with rejection only: %+v", o)
		}
		if !strings.Contains(strings.ToLower(o.err.Error()), "conflicting gameid binding") {
			t.Fatalf("loser must fail closed on conflicting claim, got %+v", o)
		}
	}
	if wins != 1 {
		t.Fatalf("exactly one conflicting room may win, got wins=%d o1=%+v o2=%+v", wins, o1, o2)
	}
	winner := roomA
	if o1.err == nil && o1.rej == nil {
		winner = o1.room
	} else {
		winner = o2.room
	}
	loser := roomB
	if winner == roomB {
		loser = roomA
	}
	eventsWinner, err := a.readAllEvents(ctx, deckStreamName(domain.RoomID(winner), domain.GameID(gameID)))
	if err != nil {
		t.Fatal(err)
	}
	if len(eventsWinner) == 0 {
		t.Fatal("winner must persist a deck")
	}
	eventsLoser, err := a.readAllEvents(ctx, deckStreamName(domain.RoomID(loser), domain.GameID(gameID)))
	if err != nil {
		t.Fatal(err)
	}
	if len(eventsLoser) != 0 {
		t.Fatalf("loser must not persist orphan deck, got %d", len(eventsLoser))
	}
	found, ok, err := a.FindByGameID(ctx, domain.GameID(gameID))
	if err != nil || !ok || string(found) != winner {
		t.Fatalf("binding must be winner: found=%q ok=%v err=%v", found, ok, err)
	}
}

func TestIntegration_StaleOwnerCannotReleaseOrFinalize(t *testing.T) {
	repo := openIndependentRepo(t, integrationKeyring(t), 1, defaultReadPageSize)
	svc := NewService(repo)
	ctx := context.Background()
	roomID := uniqueRoomID(t)
	gameID := uniqueGameID(t)
	if _, rej, err := svc.Append(ctx, AppendRequest{
		RoomID: roomID, EventID: uniqueEventID(t, "stale-seed"), ExpectedRevision: 0, EventType: "CreateRoom",
	}); err != nil || rej != nil {
		t.Fatalf("seed: rej=%v err=%v", rej, err)
	}
	token, err := repo.claimGameBinding(ctx, domain.GameID(gameID), domain.RoomID(roomID))
	if err != nil {
		t.Fatal(err)
	}
	if err := repo.releaseGameBinding(ctx, domain.GameID(gameID), domain.RoomID(roomID), "stale-token"); err != nil {
		t.Fatalf("stale release must no-op without error: %v", err)
	}
	st, err := repo.loadBindingState(ctx, domain.GameID(gameID))
	if err != nil {
		t.Fatal(err)
	}
	if st.Phase != gameBindPhaseClaimed || st.OwnerToken != token {
		t.Fatalf("stale release must leave claim intact, got %+v", st)
	}
	if err := repo.finalizeGameBinding(ctx, domain.GameID(gameID), domain.RoomID(roomID), "stale-token"); err == nil {
		t.Fatal("stale owner must not finalize")
	}
	st, err = repo.loadBindingState(ctx, domain.GameID(gameID))
	if err != nil {
		t.Fatal(err)
	}
	if st.Phase != gameBindPhaseClaimed || st.OwnerToken != token {
		t.Fatalf("failed stale finalize must leave claim intact, got %+v", st)
	}
	// Real owner can still finalize after revalidation.
	if err := repo.finalizeGameBinding(ctx, domain.GameID(gameID), domain.RoomID(roomID), token); err != nil {
		t.Fatalf("owner finalize: %v", err)
	}
}

func TestIntegration_UncertainAppendErrorUsesDeckExistence(t *testing.T) {
	repo := openIndependentRepo(t, integrationKeyring(t), 1, defaultReadPageSize)
	svc := NewService(repo)
	ctx := context.Background()
	roomID := uniqueRoomID(t)
	gameID := uniqueGameID(t)
	if _, rej, err := svc.Append(ctx, AppendRequest{
		RoomID: roomID, EventID: uniqueEventID(t, "uncertain-seed"), ExpectedRevision: 0, EventType: "CreateRoom",
	}); err != nil || rej != nil {
		t.Fatalf("seed: rej=%v err=%v", rej, err)
	}

	// Uncertain with no write → release.
	repo.failNextDeckAppendUncertain = true
	repo.uncertainDeckAppendAfterWrite = false
	if _, _, err := svc.InitializeDeck(ctx, InitializeDeckRequest{RoomID: roomID, GameID: gameID}); err == nil {
		t.Fatal("expected uncertain append failure")
	}
	st, err := repo.loadBindingState(ctx, domain.GameID(gameID))
	if err != nil {
		t.Fatal(err)
	}
	if !st.IsEmptyOrReleased() {
		t.Fatalf("uncertain without deck must release, got %+v", st)
	}
	events, err := repo.readAllEvents(ctx, deckStreamName(domain.RoomID(roomID), domain.GameID(gameID)))
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 0 {
		t.Fatalf("no deck expected, got %d", len(events))
	}

	// Uncertain after durable write → finalize/repair.
	gameID2 := uniqueGameID(t)
	repo.failNextDeckAppendUncertain = true
	repo.uncertainDeckAppendAfterWrite = true
	if _, _, err := svc.InitializeDeck(ctx, InitializeDeckRequest{RoomID: roomID, GameID: gameID2}); err == nil {
		t.Fatal("expected uncertain-after-write failure")
	}
	events, err = repo.readAllEvents(ctx, deckStreamName(domain.RoomID(roomID), domain.GameID(gameID2)))
	if err != nil {
		t.Fatal(err)
	}
	if len(events) == 0 {
		t.Fatal("deck must exist after uncertain-after-write")
	}
	found, ok, err := repo.FindByGameID(ctx, domain.GameID(gameID2))
	if err != nil || !ok || string(found) != roomID {
		t.Fatalf("uncertain-after-write must finalize binding: found=%q ok=%v err=%v", found, ok, err)
	}
	st, err = repo.loadBindingState(ctx, domain.GameID(gameID2))
	if err != nil {
		t.Fatal(err)
	}
	if st.Phase != gameBindPhaseActive {
		t.Fatalf("want active after uncertain-after-write repair, got %+v", st)
	}
}

func TestIntegration_InFlightClaimNotStolenByForeignRoom(t *testing.T) {
	repo := openIndependentRepo(t, integrationKeyring(t), 1, defaultReadPageSize)
	svc := NewService(repo)
	ctx := context.Background()
	roomA := uniqueRoomID(t)
	roomB := uniqueRoomID(t)
	gameID := uniqueGameID(t)
	for _, room := range []string{roomA, roomB} {
		if _, rej, err := svc.Append(ctx, AppendRequest{
			RoomID: room, EventID: uniqueEventID(t, "inflight-seed"), ExpectedRevision: 0, EventType: "CreateRoom",
		}); err != nil || rej != nil {
			t.Fatalf("seed %s: rej=%v err=%v", room, rej, err)
		}
	}
	token, err := repo.claimGameBinding(ctx, domain.GameID(gameID), domain.RoomID(roomA))
	if err != nil {
		t.Fatal(err)
	}
	// Cross-stream deck absence must not authorize stealing an in-flight claim.
	_, _, err = svc.InitializeDeck(ctx, InitializeDeckRequest{RoomID: roomB, GameID: gameID})
	if err == nil {
		t.Fatal("foreign room must not steal in-flight claim merely because no deck exists")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "conflicting gameid binding") {
		t.Fatalf("want conflicting binding, got %v", err)
	}
	st, err := repo.loadBindingState(ctx, domain.GameID(gameID))
	if err != nil {
		t.Fatal(err)
	}
	if st.Phase != gameBindPhaseClaimed || st.RoomID != roomA || st.OwnerToken != token {
		t.Fatalf("in-flight claim must remain with A: %+v", st)
	}
	eventsB, err := repo.readAllEvents(ctx, deckStreamName(domain.RoomID(roomB), domain.GameID(gameID)))
	if err != nil {
		t.Fatal(err)
	}
	if len(eventsB) != 0 {
		t.Fatalf("foreign room must not persist deck, got %d", len(eventsB))
	}
}
