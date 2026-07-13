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
	timers        *store.TimerIndex
	continuations nextGameContinuationQueue
	baseURL       string
	credential    string
	client        *http.Client
	stopCh        chan struct{}
	doneCh        chan struct{}
}

type nextGameContinuationQueue interface {
	ClaimDue(context.Context, time.Time, int) ([]store.NextGameContinuation, error)
	Release(context.Context, store.NextGameContinuation, time.Time) error
	Ack(context.Context, store.NextGameContinuation) error
}

func NewTimerWorker(timers *store.TimerIndex, baseURL, credential string) *TimerWorker {
	return NewTimerWorkerWithContinuations(timers, nil, baseURL, credential)
}

func NewTimerWorkerWithContinuations(timers *store.TimerIndex, continuations nextGameContinuationQueue, baseURL, credential string) *TimerWorker {
	return &TimerWorker{
		timers:        timers,
		continuations: continuations,
		baseURL:       stringsTrimRightSlash(baseURL),
		credential:    credential,
		client:        &http.Client{Timeout: 10 * time.Second},
		stopCh:        make(chan struct{}),
		doneCh:        make(chan struct{}),
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
	w.tickNextGameContinuations(ctx, now)
}

func (w *TimerWorker) tickNextGameContinuations(ctx context.Context, now time.Time) {
	if w.continuations == nil {
		return
	}
	claimed, err := w.continuations.ClaimDue(ctx, now, 16)
	if err != nil {
		log.Printf(`{"level":"warn","event":"next_game_continuation_claim_failed","error":%q}`, err.Error())
		return
	}
	for _, item := range claimed {
		terminal, err := w.dispatchNextGame(ctx, item)
		if err != nil {
			log.Printf(`{"level":"warn","event":"next_game_continuation_dispatch_failed","roomId":%q,"commandId":%q,"error":%q}`, item.RoomID, item.CommandID, err.Error())
			if releaseErr := w.continuations.Release(ctx, item, now.Add(continuationRetryDelay(item.Attempts))); releaseErr != nil {
				log.Printf(`{"level":"warn","event":"next_game_continuation_release_failed","roomId":%q,"commandId":%q,"error":%q}`, item.RoomID, item.CommandID, releaseErr.Error())
			}
			continue
		}
		if terminal {
			if err := w.continuations.Ack(ctx, item); err != nil {
				log.Printf(`{"level":"warn","event":"next_game_continuation_ack_failed","roomId":%q,"commandId":%q,"error":%q}`, item.RoomID, item.CommandID, err.Error())
			}
		}
	}
}

func (w *TimerWorker) dispatchNextGame(ctx context.Context, item store.NextGameContinuation) (terminal bool, err error) {
	body, _ := json.Marshal(map[string]any{
		"commandId":     item.CommandID,
		"type":          app.CmdStartNextGame,
		"schemaVersion": envelope.CurrentSchemaVersion,
		"payload": map[string]string{
			"roomId": item.RoomID,
			"gameId": item.NextGameID,
		},
	})
	url := fmt.Sprintf("%s/internal/v1/rooms/%s/timer-commands", w.baseURL, item.RoomID)
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
	if resp.StatusCode >= 400 {
		return false, fmt.Errorf("status %d: %s", resp.StatusCode, string(raw))
	}
	var result envelope.Result
	if err := json.Unmarshal(raw, &result); err != nil {
		return false, fmt.Errorf("decode continuation result: %w", err)
	}
	switch result.Status {
	case envelope.StatusAccepted:
		return true, nil
	case envelope.StatusRejected:
		switch result.Reason {
		case "already_terminal", "game_still_active":
			return true, nil
		default:
			return false, fmt.Errorf("retryable continuation rejection: %s", result.Reason)
		}
	default:
		return false, fmt.Errorf("unexpected continuation result status %q", result.Status)
	}
}

func continuationRetryDelay(attempts int) time.Duration {
	if attempts < 1 {
		attempts = 1
	}
	delay := time.Second
	for i := 1; i < attempts && delay < 15*time.Second; i++ {
		delay *= 2
	}
	if delay > 15*time.Second {
		return 15 * time.Second
	}
	return delay
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
