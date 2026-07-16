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

// Catalog command types that must not carry expectedSequenceNumber (OpenAPI
// additionalProperties:false variants without that property).
var sequenceProhibitedTypes = map[string]struct{}{
	"CreateRoom":        {},
	"CreateTournament":  {},
	"RegisterPlayer":    {},
	"CloseRegistration": {},
}

// Catalog command types that require expectedSequenceNumber (OpenAPI required
// on the 11 existing-room public mutations).
var sequenceRequiredTypes = map[string]struct{}{
	"JoinRoom":         {},
	"LeaveRoom":        {},
	"LockRoom":         {},
	"StartMatch":       {},
	"CancelRoom":       {},
	"PlayCard":         {},
	"DrawCard":         {},
	"ChooseColor":      {},
	"CallUno":          {},
	"ReportMissingUno": {},
	"ReconnectToRoom":  {},
}

// fieldKind classifies OpenAPI payload property types for catalog validation.
type fieldKind int

const (
	fieldString fieldKind = iota
	fieldInteger
	fieldMaxSeats
)

type fieldSpec struct {
	kind fieldKind
	enum map[string]struct{}
	min  *int64
}

func int64ptr(v int64) *int64 { return &v }

func stringEnum(values ...string) map[string]struct{} {
	out := make(map[string]struct{}, len(values))
	for _, v := range values {
		out[v] = struct{}{}
	}
	return out
}

// Payload field schemas for the 15 published OpenAPI command types.
// The public catalog is closed: Decode rejects types with no entry.
var payloadSchemas = map[string]map[string]fieldSpec{
	"CreateRoom": {
		"roomId":     {kind: fieldString},
		"visibility": {kind: fieldString, enum: stringEnum("public", "private")},
		"maxSeats":   {kind: fieldMaxSeats},
	},
	"JoinRoom": {
		"roomId": {kind: fieldString},
	},
	"LeaveRoom": {
		"roomId": {kind: fieldString},
	},
	"LockRoom": {
		"roomId": {kind: fieldString},
	},
	"StartMatch": {
		"roomId": {kind: fieldString},
		"gameId": {kind: fieldString},
	},
	"CancelRoom": {
		"roomId": {kind: fieldString},
	},
	"PlayCard": {
		"roomId": {kind: fieldString},
		"cardId": {kind: fieldString},
	},
	"DrawCard": {
		"roomId": {kind: fieldString},
	},
	"ChooseColor": {
		"roomId": {kind: fieldString},
		"color":  {kind: fieldString, enum: stringEnum("red", "yellow", "green", "blue")},
	},
	"CallUno": {
		"roomId": {kind: fieldString},
	},
	"ReportMissingUno": {
		"roomId":   {kind: fieldString},
		"targetId": {kind: fieldString},
	},
	"ReconnectToRoom": {
		"roomId":            {kind: fieldString},
		"disconnectVersion": {kind: fieldInteger, min: int64ptr(0)},
	},
	"CreateTournament": {
		"tournamentId": {kind: fieldString},
		"capacity":     {kind: fieldInteger, min: int64ptr(1)},
		"retryBudget":  {kind: fieldInteger, min: int64ptr(0)},
		"batchSize":    {kind: fieldInteger, min: int64ptr(1)},
		"visibility":   {kind: fieldString, enum: stringEnum("public", "private")},
	},
	"RegisterPlayer": {
		"tournamentId": {kind: fieldString},
		"playerId":     {kind: fieldString},
	},
	"CloseRegistration": {
		"tournamentId": {kind: fieldString},
	},
}

// IsPublicCommandType reports whether type is one of the 15 OpenAPI catalog variants.
func IsPublicCommandType(commandType string) bool {
	_, ok := payloadSchemas[commandType]
	return ok
}

// ProhibitsExpectedSequence reports whether expectedSequenceNumber must be absent
// for the command type (CreateRoom and tournament catalog commands).
func ProhibitsExpectedSequence(commandType string) bool {
	_, ok := sequenceProhibitedTypes[commandType]
	return ok
}

// RequiresExpectedSequence reports whether expectedSequenceNumber is required for
// the command type (mutations of an existing room aggregate).
func RequiresExpectedSequence(commandType string) bool {
	_, ok := sequenceRequiredTypes[commandType]
	return ok
}

// Validate checks required envelope fields without interpreting domain payload.
func (c Command) Validate() error {
	if strings.TrimSpace(c.CommandID) == "" {
		return fmt.Errorf("commandId is required")
	}
	if strings.TrimSpace(c.Type) == "" {
		return fmt.Errorf("type is required")
	}
	if c.SchemaVersion != CurrentSchemaVersion {
		return fmt.Errorf("schemaVersion must be %d", CurrentSchemaVersion)
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

func decodeJSONString(raw json.RawMessage) (string, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	tok, err := dec.Token()
	if err != nil {
		return "", fmt.Errorf("must be a string")
	}
	s, ok := tok.(string)
	if !ok {
		return "", fmt.Errorf("must be a string")
	}
	if dec.More() {
		return "", fmt.Errorf("must be a string")
	}
	return s, nil
}

func decodeJSONInteger(raw json.RawMessage) (int64, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	tok, err := dec.Token()
	if err != nil {
		return 0, fmt.Errorf("must be an integer")
	}
	num, ok := tok.(json.Number)
	if !ok {
		return 0, fmt.Errorf("must be an integer")
	}
	if dec.More() {
		return 0, fmt.Errorf("must be an integer")
	}
	n, err := num.Int64()
	if err != nil {
		return 0, fmt.Errorf("must be an integer")
	}
	return n, nil
}

func validateFieldValue(commandType, key string, spec fieldSpec, raw json.RawMessage) error {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return fmt.Errorf("payload field %q for %s must not be null", key, commandType)
	}
	switch spec.kind {
	case fieldString:
		s, err := decodeJSONString(trimmed)
		if err != nil {
			return fmt.Errorf("payload field %q for %s %v", key, commandType, err)
		}
		if spec.enum != nil {
			if _, ok := spec.enum[s]; !ok {
				return fmt.Errorf("payload field %q for %s has invalid value %q", key, commandType, s)
			}
		}
	case fieldInteger:
		n, err := decodeJSONInteger(trimmed)
		if err != nil {
			return fmt.Errorf("payload field %q for %s %v", key, commandType, err)
		}
		if spec.min != nil && n < *spec.min {
			return fmt.Errorf("payload field %q for %s must be >= %d", key, commandType, *spec.min)
		}
	case fieldMaxSeats:
		n, err := decodeJSONInteger(trimmed)
		if err != nil {
			return fmt.Errorf("payload field %q for %s %v", key, commandType, err)
		}
		// OpenAPI anyOf: <=0 (domain default) or 2..10; reject 1 and >10.
		if n <= 0 {
			return nil
		}
		if n < 2 || n > 10 {
			return fmt.Errorf("payload field %q for %s must be <=0 or between 2 and 10", key, commandType)
		}
	default:
		return fmt.Errorf("payload field %q for %s has unknown validator", key, commandType)
	}
	return nil
}

func validateCatalogPayload(commandType string, payload json.RawMessage) error {
	schema, ok := payloadSchemas[commandType]
	if !ok {
		return fmt.Errorf("unknown command type %q", commandType)
	}
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(payload, &obj); err != nil {
		return fmt.Errorf("payload must be a JSON object")
	}
	for key, raw := range obj {
		spec, ok := schema[key]
		if !ok {
			return fmt.Errorf("unknown payload field %q for %s", key, commandType)
		}
		if err := validateFieldValue(commandType, key, spec, raw); err != nil {
			return err
		}
	}
	return nil
}

// Decode unmarshals a command envelope from JSON and validates the published
// public shape: known top-level fields only, closed catalog of 15 OpenAPI
// command types, catalog payload types/enums/ranges, required/prohibited
// expectedSequenceNumber where the contract says so, and schemaVersion exactly 1
// (no omitted default, no other versions).
func Decode(data []byte) (Command, error) {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return Command{}, fmt.Errorf("decode command envelope: %w", err)
	}
	for key := range raw {
		switch key {
		case "commandId", "type", "expectedSequenceNumber", "schemaVersion", "payload":
		default:
			return Command{}, fmt.Errorf("unknown field %q", key)
		}
	}

	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	var c Command
	if err := dec.Decode(&c); err != nil {
		return Command{}, fmt.Errorf("decode command envelope: %w", err)
	}
	if err := c.Validate(); err != nil {
		return Command{}, err
	}
	if !IsPublicCommandType(c.Type) {
		return Command{}, fmt.Errorf("unknown command type %q", c.Type)
	}
	_, hasSeq := raw["expectedSequenceNumber"]
	if hasSeq && ProhibitsExpectedSequence(c.Type) {
		return Command{}, fmt.Errorf("expectedSequenceNumber is not allowed for %s", c.Type)
	}
	if RequiresExpectedSequence(c.Type) && (!hasSeq || c.ExpectedSequenceNumber == nil) {
		return Command{}, fmt.Errorf("expectedSequenceNumber is required for %s", c.Type)
	}
	if err := validateCatalogPayload(c.Type, c.Payload); err != nil {
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
