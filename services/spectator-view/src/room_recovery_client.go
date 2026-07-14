package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"unoarena/services/spectator-view/domain"
)

const (
	roomRecoverySchemaVersion = 1
	roomRecoveryPathFmt       = "/internal/v1/rooms/%s/spectator-recovery-snapshot"
)

// RoomSpectatorRecoverySnapshot is the Room recovery API response surface used by the rebuilder.
type RoomSpectatorRecoverySnapshot struct {
	SchemaVersion    int
	RoomID           string
	RecoveryJobID    string
	FailedCheckpoint int64
	SequenceNumber   int64
	ResumeCheckpoint int64
	Payload          map[string]any // SnapshotSanitized-compatible public fields
}

// RoomSpectatorRecoveryClient fetches authoritative sanitized recovery snapshots.
type RoomSpectatorRecoveryClient interface {
	FetchSpectatorRecoverySnapshot(ctx context.Context, roomID string, failedCheckpoint int64, recoveryJobID string) (RoomSpectatorRecoverySnapshot, error)
}

// HTTPRoomSpectatorRecoveryClient calls Room over HTTP with the scoped recovery credential.
type HTTPRoomSpectatorRecoveryClient struct {
	BaseURL    string
	Credential string
	HTTPClient *http.Client
}

func NewHTTPRoomSpectatorRecoveryClient(baseURL, credential string) *HTTPRoomSpectatorRecoveryClient {
	return &HTTPRoomSpectatorRecoveryClient{
		BaseURL:    strings.TrimRight(strings.TrimSpace(baseURL), "/"),
		Credential: strings.TrimSpace(credential),
		HTTPClient: tracedHTTPClient(15 * time.Second),
	}
}

func (c *HTTPRoomSpectatorRecoveryClient) FetchSpectatorRecoverySnapshot(
	ctx context.Context,
	roomID string,
	failedCheckpoint int64,
	recoveryJobID string,
) (RoomSpectatorRecoverySnapshot, error) {
	if c == nil || c.BaseURL == "" || c.Credential == "" {
		return RoomSpectatorRecoverySnapshot{}, fmt.Errorf("room recovery client not configured")
	}
	roomID = strings.TrimSpace(roomID)
	recoveryJobID = strings.TrimSpace(recoveryJobID)
	if roomID == "" || recoveryJobID == "" || failedCheckpoint < 1 {
		return RoomSpectatorRecoverySnapshot{}, fmt.Errorf("invalid recovery snapshot request")
	}

	q := url.Values{}
	q.Set("failedCheckpoint", strconv.FormatInt(failedCheckpoint, 10))
	q.Set("recoveryJobId", recoveryJobID)
	q.Set("schemaVersion", strconv.Itoa(roomRecoverySchemaVersion))
	endpoint := c.BaseURL + fmt.Sprintf(roomRecoveryPathFmt, url.PathEscape(roomID)) + "?" + q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return RoomSpectatorRecoverySnapshot{}, err
	}
	req.Header.Set(internalCredentialHeader, c.Credential)
	req.Header.Set("Accept", "application/json")

	client := c.HTTPClient
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(req)
	if err != nil {
		return RoomSpectatorRecoverySnapshot{}, fmt.Errorf("room recovery request: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return RoomSpectatorRecoverySnapshot{}, err
	}
	if resp.StatusCode != http.StatusOK {
		return RoomSpectatorRecoverySnapshot{}, fmt.Errorf("room recovery status %d: %s", resp.StatusCode, sanitizeDLQErrorSummary(string(body)))
	}
	return mapRoomRecoverySnapshotJSON(body, roomID, recoveryJobID, failedCheckpoint)
}

func mapRoomRecoverySnapshotJSON(body []byte, expectRoom, expectJob string, expectFailed int64) (RoomSpectatorRecoverySnapshot, error) {
	var raw map[string]any
	if err := json.Unmarshal(body, &raw); err != nil {
		return RoomSpectatorRecoverySnapshot{}, fmt.Errorf("room recovery json: %w", err)
	}
	schemaVersion, err := requireIntegralInt(raw["schemaVersion"], "schemaVersion")
	if err != nil || schemaVersion != 1 {
		return RoomSpectatorRecoverySnapshot{}, fmt.Errorf("room recovery schemaVersion must be 1")
	}
	roomID, err := requireNonEmptyString(raw, "roomId")
	if err != nil {
		return RoomSpectatorRecoverySnapshot{}, err
	}
	if roomID != expectRoom {
		return RoomSpectatorRecoverySnapshot{}, fmt.Errorf("room recovery roomId mismatch")
	}
	jobID, err := requireNonEmptyString(raw, "recoveryJobId")
	if err != nil {
		return RoomSpectatorRecoverySnapshot{}, err
	}
	if jobID != expectJob {
		return RoomSpectatorRecoverySnapshot{}, fmt.Errorf("room recovery recoveryJobId mismatch")
	}
	failed, err := requireIntegralInt64(raw["failedCheckpoint"], "failedCheckpoint")
	if err != nil {
		return RoomSpectatorRecoverySnapshot{}, err
	}
	if failed != expectFailed {
		return RoomSpectatorRecoverySnapshot{}, fmt.Errorf("room recovery failedCheckpoint mismatch")
	}
	seq, err := requireIntegralInt64(raw["sequenceNumber"], "sequenceNumber")
	if err != nil {
		return RoomSpectatorRecoverySnapshot{}, err
	}
	if seq < 1 {
		return RoomSpectatorRecoverySnapshot{}, fmt.Errorf("sequenceNumber must be >= 1")
	}
	resume, err := requireIntegralInt64(raw["resumeCheckpoint"], "resumeCheckpoint")
	if err != nil {
		return RoomSpectatorRecoverySnapshot{}, err
	}
	if resume < 1 {
		return RoomSpectatorRecoverySnapshot{}, fmt.Errorf("resumeCheckpoint must be >= 1")
	}

	payload := cloneStringAnyMap(raw)
	for _, meta := range []string{
		"schemaVersion", "roomId", "recoveryJobId", "failedCheckpoint",
		"sequenceNumber", "resumeCheckpoint", "eventId", "eventType", "correlationId",
	} {
		delete(payload, meta)
	}
	return RoomSpectatorRecoverySnapshot{
		SchemaVersion:    schemaVersion,
		RoomID:           roomID,
		RecoveryJobID:    jobID,
		FailedCheckpoint: failed,
		SequenceNumber:   seq,
		ResumeCheckpoint: resume,
		Payload:          payload,
	}, nil
}

// ToSnapshotSanitizedEvent maps the Room recovery response into a domain bootstrap event.
func (s RoomSpectatorRecoverySnapshot) ToSnapshotSanitizedEvent() domain.SpectatorSafeEvent {
	eventID := "recovery-snap-" + s.RecoveryJobID + "-" + strconv.FormatInt(s.ResumeCheckpoint, 10)
	return domain.SpectatorSafeEvent{
		EventID:       domain.EventID(eventID),
		EventType:     domain.EventSnapshotSanitized,
		SchemaVersion: roomRecoverySchemaVersion,
		RoomID:        domain.RoomID(s.RoomID),
		Sequence:      domain.SequenceNumber(s.ResumeCheckpoint),
		Payload:       cloneStringAnyMap(s.Payload),
	}
}
