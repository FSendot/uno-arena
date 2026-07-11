// Package envelope defines the versioned BFF command envelope and outcome shapes.
package envelope

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
)

// CurrentSchemaVersion is the envelope schema version carried on every command.
const CurrentSchemaVersion = 1

// Status values for command outcomes.
const (
	StatusAccepted = "accepted"
	StatusRejected = "rejected"
)

// Command is the compact REST command envelope submitted through the BFF.
// CommandID is the idempotency key; Type maps to the design command catalog.
type Command struct {
	CommandID              string          `json:"commandId"`
	Type                   string          `json:"type"`
	ExpectedSequenceNumber *int64          `json:"expectedSequenceNumber,omitempty"`
	SchemaVersion          int             `json:"schemaVersion"`
	Payload                json.RawMessage `json:"payload"`
}

// Result is the synchronous command outcome returned by the BFF.
type Result struct {
	CommandID     string          `json:"commandId"`
	Type          string          `json:"type"`
	Status        string          `json:"status"`
	SchemaVersion int             `json:"schemaVersion"`
	Reason        string          `json:"reason,omitempty"`
	Sequence      *int64          `json:"sequenceNumber,omitempty"`
	Payload       json.RawMessage `json:"payload,omitempty"`
}

// Validate checks required envelope fields without interpreting domain payload.
func (c Command) Validate() error {
	if strings.TrimSpace(c.CommandID) == "" {
		return fmt.Errorf("commandId is required")
	}
	if strings.TrimSpace(c.Type) == "" {
		return fmt.Errorf("type is required")
	}
	if c.SchemaVersion < 1 {
		return fmt.Errorf("schemaVersion must be >= 1")
	}
	if c.ExpectedSequenceNumber != nil && *c.ExpectedSequenceNumber < 0 {
		return fmt.Errorf("expectedSequenceNumber must be >= 0")
	}
	if err := validatePayload(c.Payload); err != nil {
		return err
	}
	return nil
}

func validatePayload(payload json.RawMessage) error {
	trimmed := bytes.TrimSpace(payload)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return fmt.Errorf("payload is required")
	}
	if !json.Valid(trimmed) {
		return fmt.Errorf("payload must be valid JSON")
	}
	if trimmed[0] != '{' {
		return fmt.Errorf("payload must be a JSON object")
	}
	return nil
}

// Decode unmarshals a command envelope from JSON and validates shape.
func Decode(data []byte) (Command, error) {
	var c Command
	if err := json.Unmarshal(data, &c); err != nil {
		return Command{}, fmt.Errorf("decode command envelope: %w", err)
	}
	if c.SchemaVersion == 0 {
		c.SchemaVersion = CurrentSchemaVersion
	}
	if err := c.Validate(); err != nil {
		return Command{}, err
	}
	return c, nil
}

// Accepted builds a successful command result.
func Accepted(commandID, commandType string, sequence *int64, payload json.RawMessage) Result {
	return Result{
		CommandID:     commandID,
		Type:          commandType,
		Status:        StatusAccepted,
		SchemaVersion: CurrentSchemaVersion,
		Sequence:      sequence,
		Payload:       payload,
	}
}

// Rejected builds a rejected command result (no domain event implied).
func Rejected(commandID, commandType, reason string, sequence *int64) Result {
	return Result{
		CommandID:     commandID,
		Type:          commandType,
		Status:        StatusRejected,
		SchemaVersion: CurrentSchemaVersion,
		Reason:        reason,
		Sequence:      sequence,
	}
}
