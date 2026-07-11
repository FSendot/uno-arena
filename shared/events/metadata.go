// Package events defines shared domain-event metadata carried on Kafka payloads.
package events

import (
	"fmt"
	"strings"
	"time"
)

// Metadata is the common envelope for bounded-context Kafka events.
type Metadata struct {
	EventID       string    `json:"eventId"`
	EventType     string    `json:"eventType"`
	SchemaVersion int       `json:"schemaVersion"`
	CorrelationID string    `json:"correlationId"`
	CausationID   string    `json:"causationId,omitempty"`
	OccurredAt    time.Time `json:"occurredAt"`
}

// Validate checks required event metadata fields.
func (m Metadata) Validate() error {
	if strings.TrimSpace(m.EventID) == "" {
		return fmt.Errorf("eventId is required")
	}
	if strings.TrimSpace(m.EventType) == "" {
		return fmt.Errorf("eventType is required")
	}
	if m.SchemaVersion < 1 {
		return fmt.Errorf("schemaVersion must be >= 1")
	}
	if strings.TrimSpace(m.CorrelationID) == "" {
		return fmt.Errorf("correlationId is required")
	}
	// CausationID is optional, but whitespace-only values are invalid.
	if len(m.CausationID) > 0 && strings.TrimSpace(m.CausationID) == "" {
		return fmt.Errorf("causationId must not be blank")
	}
	if m.OccurredAt.IsZero() {
		return fmt.Errorf("occurredAt is required")
	}
	return nil
}

// NewMetadata builds metadata with schemaVersion 1 and UTC occurredAt.
func NewMetadata(eventID, eventType, correlationID, causationID string, occurredAt time.Time) Metadata {
	if occurredAt.IsZero() {
		occurredAt = time.Now().UTC()
	} else {
		occurredAt = occurredAt.UTC()
	}
	return Metadata{
		EventID:       eventID,
		EventType:     eventType,
		SchemaVersion: 1,
		CorrelationID: correlationID,
		CausationID:   causationID,
		OccurredAt:    occurredAt,
	}
}
