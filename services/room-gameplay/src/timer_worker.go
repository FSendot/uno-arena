package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"unoarena/services/room-gameplay/app"
	"unoarena/services/room-gameplay/store"
	"unoarena/shared/envelope"
)

// TimerWorker claims due Redis timers, POSTs internal timer commands, and acks
// success/stale-terminal outcomes. Infra failures leave leases for the reaper.
type TimerWorker struct {
	timers     *store.TimerIndex
	baseURL    string
	credential string
	client     *http.Client
	stopCh     chan struct{}
	doneCh     chan struct{}
}

func NewTimerWorker(timers *store.TimerIndex, baseURL, credential string) *TimerWorker {
	return &TimerWorker{
		timers:     timers,
		baseURL:    stringsTrimRightSlash(baseURL),
		credential: credential,
		client:     &http.Client{Timeout: 10 * time.Second},
		stopCh:     make(chan struct{}),
		doneCh:     make(chan struct{}),
	}
}

func (w *TimerWorker) Start() {
	go w.loop()
}

func (w *TimerWorker) Stop() {
	close(w.stopCh)
	<-w.doneCh
}

func (w *TimerWorker) loop() {
	defer close(w.doneCh)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-w.stopCh:
			return
		case <-ticker.C:
			w.tick(context.Background())
		}
	}
}

func (w *TimerWorker) tick(ctx context.Context) {
	now := time.Now().UTC()
	_, _ = w.timers.ReapExpiredLeases(ctx, "uno", now)
	_, _ = w.timers.ReapExpiredLeases(ctx, "reconnect", now)

	for _, family := range []string{"uno", "reconnect"} {
		claimed, err := w.timers.ClaimDue(ctx, family, now, 16)
		if err != nil {
			log.Printf(`{"level":"warn","event":"timer_claim_failed","family":%q,"error":%q}`, family, err.Error())
			continue
		}
		for _, id := range claimed {
			terminal, err := w.dispatch(ctx, id)
			if err != nil {
				log.Printf(`{"level":"warn","event":"timer_dispatch_failed","timer":%q,"error":%q}`, id.String(), err.Error())
				continue
			}
			if terminal {
				if err := w.timers.Ack(ctx, id); err != nil {
					log.Printf(`{"level":"warn","event":"timer_ack_failed","timer":%q,"error":%q}`, id.String(), err.Error())
				}
			}
		}
	}
}

func (w *TimerWorker) dispatch(ctx context.Context, id store.TimerID) (terminal bool, err error) {
	var cmdType string
	payload := map[string]any{}
	switch id.Family {
	case "uno":
		cmdType = app.CmdExpireUnoWindow
		payload["playerId"] = id.PlayerID
		payload["gameId"] = id.GameID
		payload["triggeringGameEventId"] = id.Trigger
		payload["openingRoomSequence"] = id.OpeningSeq
	case "reconnect":
		cmdType = app.CmdForfeitPlayer
		payload["playerId"] = id.PlayerID
		payload["disconnectVersion"] = id.Version
	default:
		return true, fmt.Errorf("unknown timer family %q", id.Family)
	}
	body, _ := json.Marshal(map[string]any{
		"commandId":     "timer:" + id.String(),
		"type":          cmdType,
		"schemaVersion": envelope.CurrentSchemaVersion,
		"payload":       payload,
		"asSystem":      true,
	})
	url := fmt.Sprintf("%s/internal/v1/rooms/%s/timer-commands", w.baseURL, id.RoomID)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return false, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Service-Credential", w.credential)
	resp, err := w.client.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode >= 500 {
		return false, fmt.Errorf("status %d: %s", resp.StatusCode, string(raw))
	}
	// 2xx accepted/rejected and 4xx stale/terminal → ack.
	return true, nil
}

func stringsTrimRightSlash(s string) string {
	for len(s) > 0 && s[len(s)-1] == '/' {
		s = s[:len(s)-1]
	}
	return s
}
