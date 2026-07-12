package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"unoarena/services/analytics/domain"
)

const (
	analyticsBackfillMaxLimit     = 1000
	analyticsBackfillDefaultLimit = 1000 // one bounded producer page per Kafka request
	internalCredentialHeaderName  = "X-Service-Credential"
)

// AnalyticsBackfillHTTPRequest is the producer-owned backfill POST body.
type AnalyticsBackfillHTTPRequest struct {
	RecoveryJobID  string `json:"recoveryJobId"`
	SourceTopic    string `json:"sourceTopic"`
	SchemaVersion  int    `json:"schemaVersion"`
	Cursor         string `json:"cursor,omitempty"`
	Limit          int    `json:"limit"`
	FromCheckpoint string `json:"fromCheckpoint,omitempty"`
	ToCheckpoint   string `json:"toCheckpoint,omitempty"`
	FromOccurredAt string `json:"fromOccurredAt,omitempty"`
	ToOccurredAt   string `json:"toOccurredAt,omitempty"`
}

// AnalyticsBackfillHTTPResponse is the producer page response.
type AnalyticsBackfillHTTPResponse struct {
	Records        []json.RawMessage `json:"records"`
	NextCursor     string            `json:"nextCursor,omitempty"`
	FromCheckpoint string            `json:"fromCheckpoint,omitempty"`
	ToCheckpoint   string            `json:"toCheckpoint,omitempty"`
	FromOccurredAt string            `json:"fromOccurredAt,omitempty"`
	ToOccurredAt   string            `json:"toOccurredAt,omitempty"`
	RecoveryJobID  string            `json:"recoveryJobId"`
	SourceTopic    string            `json:"sourceTopic"`
	SchemaVersion  int               `json:"schemaVersion"`
}

// AnalyticsBackfillClient fetches one bounded sanitized page from a producer.
type AnalyticsBackfillClient interface {
	FetchPage(ctx context.Context, req ParsedAnalyticsProjectionRebuildRequest) (AnalyticsBackfillHTTPResponse, []domain.UpstreamEvent, error)
}

// HTTPAnalyticsBackfillClients routes by sourceContext to Room/Tournament/Ranking.
type HTTPAnalyticsBackfillClients struct {
	HTTP      *http.Client
	RoomURL   string
	RoomCred  string
	TournURL  string
	TournCred string
	RankURL   string
	RankCred  string
}

func (c *HTTPAnalyticsBackfillClients) endpointFor(ctx string) (url, cred, path string, err error) {
	switch ctx {
	case "room":
		return strings.TrimRight(c.RoomURL, "/"), c.RoomCred, "/internal/v1/rooms/analytics-backfill", nil
	case "tournament":
		return strings.TrimRight(c.TournURL, "/"), c.TournCred, "/internal/v1/tournaments/analytics-backfill", nil
	case "ranking":
		return strings.TrimRight(c.RankURL, "/"), c.RankCred, "/internal/v1/rankings/analytics-backfill", nil
	default:
		return "", "", "", newTerminalKafkaError(KafkaFailurePayloadInvalid, fmt.Errorf("unknown sourceContext %q", ctx))
	}
}

// FetchPage POSTs one page (limit hard-capped at 1000) and validates each record.
func (c *HTTPAnalyticsBackfillClients) FetchPage(ctx context.Context, req ParsedAnalyticsProjectionRebuildRequest) (AnalyticsBackfillHTTPResponse, []domain.UpstreamEvent, error) {
	if c == nil {
		return AnalyticsBackfillHTTPResponse{}, nil, fmt.Errorf("backfill client not configured")
	}
	base, cred, path, err := c.endpointFor(req.SourceContext)
	if err != nil {
		return AnalyticsBackfillHTTPResponse{}, nil, err
	}
	if base == "" || cred == "" {
		return AnalyticsBackfillHTTPResponse{}, nil, fmt.Errorf("backfill URL/credential missing for %s", req.SourceContext)
	}
	limit := analyticsBackfillDefaultLimit
	if limit > analyticsBackfillMaxLimit {
		limit = analyticsBackfillMaxLimit
	}
	body := AnalyticsBackfillHTTPRequest{
		RecoveryJobID: req.RecoveryJobID,
		SourceTopic:   req.ExpectedSourceTopic,
		SchemaVersion: 1,
		Cursor:        req.PageCursor,
		Limit:         limit,
	}
	if req.HasCheckpointRange {
		body.FromCheckpoint = req.FromCheckpoint
		body.ToCheckpoint = req.ToCheckpoint
	}
	if req.HasOccurredRange {
		body.FromOccurredAt = req.FromOccurredAt.UTC().Format(time.RFC3339Nano)
		body.ToOccurredAt = req.ToOccurredAt.UTC().Format(time.RFC3339Nano)
	}
	raw, err := json.Marshal(body)
	if err != nil {
		return AnalyticsBackfillHTTPResponse{}, nil, err
	}
	httpClient := c.HTTP
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 30 * time.Second}
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, base+path, bytes.NewReader(raw))
	if err != nil {
		return AnalyticsBackfillHTTPResponse{}, nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set(internalCredentialHeaderName, cred)
	if req.CorrelationID != "" {
		httpReq.Header.Set("X-Correlation-Id", req.CorrelationID)
	}
	resp, err := httpClient.Do(httpReq)
	if err != nil {
		return AnalyticsBackfillHTTPResponse{}, nil, fmt.Errorf("backfill http: %w", err)
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
	if err != nil {
		return AnalyticsBackfillHTTPResponse{}, nil, err
	}
	if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
		return AnalyticsBackfillHTTPResponse{}, nil, newTerminalKafkaError(KafkaFailurePayloadInvalid,
			fmt.Errorf("backfill auth failed status %d", resp.StatusCode))
	}
	if resp.StatusCode == http.StatusBadRequest {
		return AnalyticsBackfillHTTPResponse{}, nil, newTerminalKafkaError(KafkaFailurePayloadInvalid,
			fmt.Errorf("backfill bad request status %d", resp.StatusCode))
	}
	if resp.StatusCode != http.StatusOK {
		return AnalyticsBackfillHTTPResponse{}, nil, fmt.Errorf("backfill status %d", resp.StatusCode)
	}
	var page AnalyticsBackfillHTTPResponse
	if err := json.Unmarshal(respBody, &page); err != nil {
		return AnalyticsBackfillHTTPResponse{}, nil, newTerminalKafkaError(KafkaFailurePayloadInvalid, fmt.Errorf("invalid backfill response json"))
	}
	if page.RecoveryJobID != req.RecoveryJobID {
		return AnalyticsBackfillHTTPResponse{}, nil, newTerminalKafkaError(KafkaFailurePayloadInvalid, fmt.Errorf("backfill recoveryJobId mismatch"))
	}
	if page.SourceTopic != req.ExpectedSourceTopic {
		return AnalyticsBackfillHTTPResponse{}, nil, newTerminalKafkaError(KafkaFailurePayloadInvalid, fmt.Errorf("backfill sourceTopic mismatch"))
	}
	if page.SchemaVersion != 1 {
		return AnalyticsBackfillHTTPResponse{}, nil, newTerminalKafkaError(KafkaFailurePayloadInvalid,
			fmt.Errorf("backfill schemaVersion must be 1, got %d", page.SchemaVersion))
	}
	if page.NextCursor != "" && page.NextCursor == req.PageCursor {
		return AnalyticsBackfillHTTPResponse{}, nil, newTerminalKafkaError(KafkaFailurePayloadInvalid,
			fmt.Errorf("backfill nextCursor must not equal current cursor"))
	}
	if len(page.Records) > analyticsBackfillMaxLimit {
		return AnalyticsBackfillHTTPResponse{}, nil, newTerminalKafkaError(KafkaFailurePayloadInvalid,
			fmt.Errorf("backfill returned %d records > hard max %d", len(page.Records), analyticsBackfillMaxLimit))
	}
	events := make([]domain.UpstreamEvent, 0, len(page.Records))
	for i, rec := range page.Records {
		evt, err := ParseAnalyticsBackfillRecord(req.ExpectedSourceTopic, rec)
		if err != nil {
			return AnalyticsBackfillHTTPResponse{}, nil, fmt.Errorf("backfill record[%d]: %w", i, err)
		}
		events = append(events, evt)
	}
	return page, events, nil
}
