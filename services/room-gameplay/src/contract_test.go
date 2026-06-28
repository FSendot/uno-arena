package main

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"testing"
)

type contractSchema struct {
	Type                 string                            `json:"type"`
	Required             []string                          `json:"required"`
	Properties           map[string]contractSchemaProperty `json:"properties"`
	AdditionalProperties *bool                             `json:"additionalProperties"`
}

type contractSchemaProperty struct {
	Type    string   `json:"type"`
	Minimum *float64 `json:"minimum"`
}

func TestSpectatorSafeEventSchema(t *testing.T) {
	schema := loadContractSchema(t, "../contracts/room.spectator-safe.events.schema.json")

	tests := []struct {
		name      string
		payload   string
		wantValid bool
	}{
		{
			name:      "valid spectator-safe event",
			payload:   `{"roomId":"room-123","eventType":"card.played","sequenceNumber":1,"payload":{"card":"red-7"}}`,
			wantValid: true,
		},
		{
			name:      "missing required field",
			payload:   `{"roomId":"room-123","sequenceNumber":1,"payload":{"card":"red-7"}}`,
			wantValid: false,
		},
		{
			name:      "sequence number below schema minimum",
			payload:   `{"roomId":"room-123","eventType":"card.played","sequenceNumber":0,"payload":{"card":"red-7"}}`,
			wantValid: false,
		},
		{
			name:      "extra top-level field",
			payload:   `{"roomId":"room-123","eventType":"card.played","sequenceNumber":1,"payload":{"card":"red-7"},"privateHand":["red-9"]}`,
			wantValid: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			event := decodeContractPayload(t, tt.payload)
			err := validateContractPayload(schema, event)
			if tt.wantValid && err != nil {
				t.Fatalf("expected payload to pass schema validation: %v", err)
			}
			if !tt.wantValid && err == nil {
				t.Fatal("expected payload to fail schema validation")
			}
		})
	}
}

func loadContractSchema(t *testing.T, relativePath string) contractSchema {
	t.Helper()

	data, err := os.ReadFile(filepath.Clean(relativePath))
	if err != nil {
		t.Fatalf("failed to read contract schema: %v", err)
	}

	var schema contractSchema
	if err := json.Unmarshal(data, &schema); err != nil {
		t.Fatalf("failed to parse contract schema: %v", err)
	}
	if schema.Type != "object" {
		t.Fatalf("unsupported contract schema root type %q", schema.Type)
	}
	if len(schema.Properties) == 0 {
		t.Fatal("contract schema has no properties")
	}

	return schema
}

func decodeContractPayload(t *testing.T, raw string) map[string]interface{} {
	t.Helper()

	var event map[string]interface{}
	if err := json.Unmarshal([]byte(raw), &event); err != nil {
		t.Fatalf("failed to parse contract payload: %v", err)
	}

	return event
}

func validateContractPayload(schema contractSchema, event map[string]interface{}) error {
	for _, field := range schema.Required {
		if _, ok := event[field]; !ok {
			return fmt.Errorf("missing required field %q", field)
		}
	}

	if schema.AdditionalProperties != nil && !*schema.AdditionalProperties {
		for field := range event {
			if _, ok := schema.Properties[field]; !ok {
				return fmt.Errorf("unexpected top-level field %q", field)
			}
		}
	}

	for field, property := range schema.Properties {
		value, ok := event[field]
		if !ok {
			continue
		}
		if err := validateContractProperty(field, value, property); err != nil {
			return err
		}
	}

	return nil
}

func validateContractProperty(field string, value interface{}, property contractSchemaProperty) error {
	switch property.Type {
	case "string":
		if _, ok := value.(string); !ok {
			return fmt.Errorf("field %q must be a string", field)
		}
	case "integer":
		number, ok := value.(float64)
		if !ok || math.Trunc(number) != number {
			return fmt.Errorf("field %q must be an integer", field)
		}
		if property.Minimum != nil && number < *property.Minimum {
			return fmt.Errorf("field %q must be at least %v", field, *property.Minimum)
		}
	case "object":
		if _, ok := value.(map[string]interface{}); !ok {
			return fmt.Errorf("field %q must be an object", field)
		}
	default:
		return fmt.Errorf("unsupported schema type %q for field %q", property.Type, field)
	}

	return nil
}
