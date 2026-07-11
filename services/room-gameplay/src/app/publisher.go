package app

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"unoarena/shared/correlation"
)

const (
	headerServiceCredential = "X-Service-Credential"
	streamEventsPath        = "/internal/v1/streams/events"
	defaultPublishTimeout   = 10 * time.Second
)

// PublisherDestinations holds offline HTTP base URLs and credentials for each sink.
type PublisherDestinations struct {
	GatewayURL     string
	GatewayCred    string
	SpectatorURL   string
	SpectatorCred  string
	RankingURL     string
	RankingCred    string
	AnalyticsURL   string
	AnalyticsCred  string
	TournamentURL  string
	TournamentCred string
}

// MultiDestinationPublisher posts one versioned event to every required sink
// for its Topic/Stream. Any destination failure is returned so the Room outbox
// stays pending for retry. Topic-only events are never silently no-op'd.
type MultiDestinationPublisher struct {
	dest   PublisherDestinations
	client *http.Client
}

// NewMultiDestinationPublisher constructs the offline multi-sink publisher.
func NewMultiDestinationPublisher(dest PublisherDestinations, client *http.Client) *MultiDestinationPublisher {
	if client == nil {
		client = &http.Client{Timeout: defaultPublishTimeout}
	}
	return &MultiDestinationPublisher{dest: dest, client: client}
}

// NewHTTPEventPublisher preserves the legacy Gateway-only constructor as a
// multi-destination publisher with only Gateway configured.
func NewHTTPEventPublisher(baseURL, serviceCredential string, client *http.Client) *MultiDestinationPublisher {
	return NewMultiDestinationPublisher(PublisherDestinations{
		GatewayURL:  baseURL,
		GatewayCred: serviceCredential,
	}, client)
}

// Publish routes one event to all required destinations for its Topic/Stream.
func (p *MultiDestinationPublisher) Publish(ctx context.Context, event PublishedEvent) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if event.EventID == "" || event.EventType == "" || event.RoomID == "" {
		return fmt.Errorf("eventId, eventType, and roomId are required")
	}
	if event.SchemaVersion == 0 {
		event.SchemaVersion = 1
	}

	dests, err := p.planDestinations(event)
	if err != nil {
		return err
	}
	for _, d := range dests {
		if err := p.post(ctx, d); err != nil {
			return err
		}
	}
	return nil
}

type publishCall struct {
	name string
	url  string
	cred string
	body []byte
}

func (p *MultiDestinationPublisher) planDestinations(event PublishedEvent) ([]publishCall, error) {
	var out []publishCall

	needsGateway := event.Stream == StreamPlayer || event.Stream == StreamSpectator
	needsSpectator := event.Topic == TopicSpectatorSafe || event.Stream == StreamSpectator
	needsRanking := event.Topic == TopicGameCompleted
	needsAnalyticsGame := event.Topic == TopicGameCompleted
	needsAnalyticsMetric := event.Topic == TopicGameplayMetrics
	needsTournament := event.Topic == TopicMatchCompleted

	if !needsGateway && !needsSpectator && !needsRanking && !needsAnalyticsGame && !needsAnalyticsMetric && !needsTournament {
		if event.Topic != "" || event.Stream != "" {
			return nil, fmt.Errorf("unsupported publish route topic=%q stream=%q", event.Topic, event.Stream)
		}
		return nil, fmt.Errorf("published event requires stream or topic")
	}

	if needsGateway {
		if strings.TrimSpace(p.dest.GatewayURL) == "" {
			return nil, fmt.Errorf("gateway streams base URL not configured")
		}
		body, err := gatewayStreamBody(event)
		if err != nil {
			return nil, err
		}
		out = append(out, publishCall{
			name: "gateway",
			url:  strings.TrimRight(p.dest.GatewayURL, "/") + streamEventsPath,
			cred: p.dest.GatewayCred,
			body: body,
		})
	}

	if needsSpectator {
		if strings.TrimSpace(p.dest.SpectatorURL) == "" {
			return nil, fmt.Errorf("spectator view base URL not configured")
		}
		body, err := spectatorCanonicalBody(event)
		if err != nil {
			return nil, err
		}
		out = append(out, publishCall{
			name: "spectator",
			url:  strings.TrimRight(p.dest.SpectatorURL, "/") + "/internal/v1/spectator/rooms/" + event.RoomID + "/events",
			cred: p.dest.SpectatorCred,
			body: body,
		})
	}

	if needsRanking {
		if !gameCompletedRoutesToRanking(event) {
			// Tournament / abandoned games are not Ranking-eligible ("as appropriate").
		} else {
			if strings.TrimSpace(p.dest.RankingURL) == "" {
				return nil, fmt.Errorf("ranking base URL not configured")
			}
			body, err := RankingGameCompletedBody(event)
			if err != nil {
				return nil, err
			}
			raw, err := json.Marshal(body)
			if err != nil {
				return nil, err
			}
			out = append(out, publishCall{
				name: "ranking",
				url:  strings.TrimRight(p.dest.RankingURL, "/") + "/internal/v1/rankings/games-results",
				cred: p.dest.RankingCred,
				body: raw,
			})
		}
	}

	if needsAnalyticsGame {
		if !gameCompletedRoutesToAnalytics(event) {
			// Skip when not appropriate.
		} else {
			if strings.TrimSpace(p.dest.AnalyticsURL) == "" {
				return nil, fmt.Errorf("analytics base URL not configured")
			}
			body, err := analyticsGameCompletedBody(event)
			if err != nil {
				return nil, err
			}
			out = append(out, publishCall{
				name: "analytics",
				url:  strings.TrimRight(p.dest.AnalyticsURL, "/") + "/internal/v1/analytics/room/events",
				cred: p.dest.AnalyticsCred,
				body: body,
			})
		}
	}

	if needsAnalyticsMetric {
		if strings.TrimSpace(p.dest.AnalyticsURL) == "" {
			return nil, fmt.Errorf("analytics base URL not configured")
		}
		body, err := analyticsMetricBody(event)
		if err != nil {
			return nil, err
		}
		out = append(out, publishCall{
			name: "analytics",
			url:  strings.TrimRight(p.dest.AnalyticsURL, "/") + "/internal/v1/analytics/room/events",
			cred: p.dest.AnalyticsCred,
			body: body,
		})
	}

	if needsTournament {
		var peek map[string]any
		_ = json.Unmarshal(event.Payload, &peek)
		tid, _ := peek["tournamentId"].(string)
		if tid == "" {
			// Ad-hoc MatchCompleted has no tournament sink ("as appropriate").
		} else {
			if strings.TrimSpace(p.dest.TournamentURL) == "" {
				return nil, fmt.Errorf("tournament base URL not configured")
			}
			tournamentID, body, err := tournamentMatchCompletedBody(event)
			if err != nil {
				return nil, err
			}
			out = append(out, publishCall{
				name: "tournament",
				url:  strings.TrimRight(p.dest.TournamentURL, "/") + "/internal/v1/tournaments/" + tournamentID + "/match-results",
				cred: p.dest.TournamentCred,
				body: body,
			})
		}
	}

	if len(out) == 0 {
		// Eligible sinks were filtered as not appropriate (e.g. ad-hoc MatchCompleted
		// without tournamentId). Misconfiguration already errored above.
		if needsTournament || (needsRanking && !gameCompletedRoutesToRanking(event)) {
			return nil, nil
		}
		return nil, fmt.Errorf("no publish destinations for topic=%q stream=%q", event.Topic, event.Stream)
	}
	return out, nil
}

func (p *MultiDestinationPublisher) post(ctx context.Context, call publishCall) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, call.url, bytes.NewReader(call.body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if call.cred != "" {
		req.Header.Set(headerServiceCredential, call.cred)
	}
	if corr, ok := correlationFromContext(ctx); ok {
		corr.Apply(req.Header)
	}
	resp, err := p.client.Do(req)
	if err != nil {
		return fmt.Errorf("%s publish: %w", call.name, err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode >= 400 {
		return fmt.Errorf("%s publish status %d", call.name, resp.StatusCode)
	}
	return nil
}

func gatewayStreamBody(event PublishedEvent) ([]byte, error) {
	if event.Stream == "" {
		return nil, fmt.Errorf("gateway stream publish requires stream")
	}
	if event.SequenceNumber < 1 {
		return nil, fmt.Errorf("sequence must be >= 1")
	}
	data := event.Payload
	if len(data) == 0 {
		data = json.RawMessage(`{}`)
	}
	var envelope map[string]any
	if err := json.Unmarshal(data, &envelope); err == nil {
		if payload, ok := envelope["payload"].(map[string]any); ok {
			if raw, err := json.Marshal(payload); err == nil {
				data = raw
			}
		} else if d, ok := envelope["data"].(map[string]any); ok {
			if raw, err := json.Marshal(d); err == nil {
				data = raw
			}
		}
	}
	return json.Marshal(streamIngestBody{
		SchemaVersion: event.SchemaVersion,
		EventID:       event.EventID,
		Stream:        event.Stream,
		RoomID:        event.RoomID,
		SessionID:     event.SessionID,
		PlayerID:      event.PlayerID,
		Sequence:      event.SequenceNumber,
		Event:         event.EventType,
		Data:          data,
	})
}

type streamIngestBody struct {
	SchemaVersion int             `json:"schemaVersion"`
	EventID       string          `json:"eventId"`
	Stream        string          `json:"stream"`
	RoomID        string          `json:"roomId"`
	SessionID     string          `json:"sessionId,omitempty"`
	PlayerID      string          `json:"playerId,omitempty"`
	Sequence      int64           `json:"sequence"`
	Event         string          `json:"event"`
	Data          json.RawMessage `json:"data"`
}

func spectatorCanonicalBody(event PublishedEvent) ([]byte, error) {
	if event.SequenceNumber < 1 {
		return nil, fmt.Errorf("sequence must be >= 1")
	}
	data := map[string]any{}
	if len(event.Payload) > 0 {
		var raw map[string]any
		if err := json.Unmarshal(event.Payload, &raw); err != nil {
			return nil, fmt.Errorf("spectator payload: %w", err)
		}
		// Accept already-flat data, or unwrap legacy nested envelopes.
		if payload, ok := raw["payload"].(map[string]any); ok {
			data = payload
		} else if d, ok := raw["data"].(map[string]any); ok {
			data = d
		} else {
			// Strip envelope keys if present at top level.
			data = raw
			delete(data, "roomId")
			delete(data, "eventType")
			delete(data, "sequenceNumber")
			delete(data, "schemaVersion")
			delete(data, "eventId")
			delete(data, "stream")
			delete(data, "sequence")
			delete(data, "event")
		}
	}
	return json.Marshal(map[string]any{
		"schemaVersion": event.SchemaVersion,
		"eventId":       event.EventID,
		"stream":        StreamSpectator,
		"roomId":        event.RoomID,
		"sequence":      event.SequenceNumber,
		"event":         event.EventType,
		"data":          data,
	})
}

// RankingGameCompletedBody maps a room.game.completed payload to Ranking's
// /internal/v1/rankings/games-results body.
func RankingGameCompletedBody(event PublishedEvent) (map[string]any, error) {
	var p map[string]any
	if err := json.Unmarshal(event.Payload, &p); err != nil {
		return nil, fmt.Errorf("game completed payload: %w", err)
	}
	commandID, _ := p["commandId"].(string)
	if commandID == "" {
		commandID = event.CausationID
	}
	eventID, _ := p["eventId"].(string)
	if eventID == "" {
		eventID = event.EventID
	}
	gameID, _ := p["gameId"].(string)
	roomID, _ := p["roomId"].(string)
	if roomID == "" {
		roomID = event.RoomID
	}
	roomType, _ := p["roomType"].(string)
	isAbandoned, _ := p["isAbandoned"].(bool)
	authoritative := true
	if v, ok := p["authoritative"].(bool); ok {
		authoritative = v
	}
	completed := true
	if v, ok := p["completed"].(bool); ok {
		completed = v
	}

	participants := participantsFromGameCompleted(p)
	if len(participants) == 0 {
		return nil, fmt.Errorf("game completed requires participants")
	}
	return map[string]any{
		"commandId":     commandID,
		"eventId":       eventID,
		"gameId":        gameID,
		"roomId":        roomID,
		"roomType":      roomType,
		"isAbandoned":   isAbandoned,
		"authoritative": authoritative,
		"completed":     completed,
		"participants":  participants,
	}, nil
}

func participantsFromGameCompleted(p map[string]any) []map[string]any {
	if raw, ok := p["participants"].([]any); ok {
		out := make([]map[string]any, 0, len(raw))
		for _, item := range raw {
			m, ok := item.(map[string]any)
			if !ok {
				continue
			}
			part := map[string]any{
				"playerId":  m["playerId"],
				"placement": m["placement"],
			}
			if _, has := m["cardPoints"]; has {
				part["cardPoints"] = m["cardPoints"]
			}
			if _, has := m["outcome"]; has {
				part["outcome"] = m["outcome"]
			}
			out = append(out, part)
		}
		return out
	}
	// Fallback: derive from placementOrder.
	order, _ := p["placementOrder"].([]any)
	out := make([]map[string]any, 0, len(order))
	for i, item := range order {
		pid, _ := item.(string)
		if pid == "" {
			continue
		}
		out = append(out, map[string]any{
			"playerId":  pid,
			"placement": i + 1,
		})
	}
	return out
}

func gameCompletedRoutesToRanking(event PublishedEvent) bool {
	var p map[string]any
	if err := json.Unmarshal(event.Payload, &p); err != nil {
		return false
	}
	roomType, _ := p["roomType"].(string)
	isAbandoned, _ := p["isAbandoned"].(bool)
	return roomType == "ad_hoc" && !isAbandoned
}

func gameCompletedRoutesToAnalytics(event PublishedEvent) bool {
	// Analytics receives completion metrics for every GameCompleted.
	return true
}

func analyticsGameCompletedBody(event PublishedEvent) ([]byte, error) {
	var p map[string]any
	if err := json.Unmarshal(event.Payload, &p); err != nil {
		return nil, err
	}
	roomID, _ := p["roomId"].(string)
	if roomID == "" {
		roomID = event.RoomID
	}
	gameID, _ := p["gameId"].(string)
	roomType, _ := p["roomType"].(string)
	visibility := "anonymized_adhoc"
	if roomType == "tournament" {
		visibility = "public_tournament"
	}
	occurredAt, _ := p["occurredAt"].(string)
	if occurredAt == "" {
		occurredAt = event.OccurredAt
	}
	if occurredAt == "" {
		occurredAt = time.Now().UTC().Format(time.RFC3339)
	}
	corr, _ := p["correlationId"].(string)
	if corr == "" {
		corr = event.CorrelationID
	}
	payload := map[string]any{
		"visibility": visibility,
		"metricType": "game_completed",
		"roomId":     roomID,
	}
	if gameID != "" {
		payload["gameId"] = gameID
	}
	return json.Marshal(map[string]any{
		"eventId":       event.EventID,
		"eventType":     "GameplayMetric",
		"schemaVersion": event.SchemaVersion,
		"correlationId": corr,
		"occurredAt":    occurredAt,
		"payload":       payload,
	})
}

func analyticsMetricBody(event PublishedEvent) ([]byte, error) {
	// Payload may already be the analytics upstream envelope.
	var raw map[string]any
	if err := json.Unmarshal(event.Payload, &raw); err != nil {
		return nil, err
	}
	if _, ok := raw["payload"]; ok {
		if raw["eventType"] == nil {
			raw["eventType"] = "GameplayMetric"
		}
		if raw["eventId"] == nil {
			raw["eventId"] = event.EventID
		}
		if raw["schemaVersion"] == nil {
			raw["schemaVersion"] = event.SchemaVersion
		}
		return json.Marshal(raw)
	}
	return json.Marshal(map[string]any{
		"eventId":       event.EventID,
		"eventType":     "GameplayMetric",
		"schemaVersion": event.SchemaVersion,
		"correlationId": event.CorrelationID,
		"occurredAt":    firstNonEmpty(event.OccurredAt, time.Now().UTC().Format(time.RFC3339)),
		"payload":       raw,
	})
}

func tournamentMatchCompletedBody(event PublishedEvent) (tournamentID string, body []byte, err error) {
	var p map[string]any
	if err := json.Unmarshal(event.Payload, &p); err != nil {
		return "", nil, err
	}
	tournamentID, _ = p["tournamentId"].(string)
	if tournamentID == "" {
		return "", nil, fmt.Errorf("match completed requires tournamentId")
	}
	raw, err := json.Marshal(p)
	return tournamentID, raw, err
}

type corrCtxKey struct{}

// WithCorrelation attaches correlation headers for outbound publish.
func WithCorrelation(ctx context.Context, h correlation.Headers) context.Context {
	return context.WithValue(ctx, corrCtxKey{}, h)
}

func correlationFromContext(ctx context.Context) (correlation.Headers, bool) {
	v := ctx.Value(corrCtxKey{})
	if v == nil {
		return correlation.Headers{}, false
	}
	h, ok := v.(correlation.Headers)
	return h, ok
}
