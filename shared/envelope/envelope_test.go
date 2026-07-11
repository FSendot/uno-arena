package envelope

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestDecodeValidEnvelope(t *testing.T) {
	raw := []byte(`{
		"commandId":"cmd_1",
		"type":"PlayCard",
		"expectedSequenceNumber":42,
		"schemaVersion":1,
		"payload":{"roomId":"room_1","cardId":"red-7"}
	}`)
	cmd, err := Decode(raw)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if cmd.CommandID != "cmd_1" || cmd.Type != "PlayCard" {
		t.Fatalf("unexpected command: %+v", cmd)
	}
	if cmd.ExpectedSequenceNumber == nil || *cmd.ExpectedSequenceNumber != 42 {
		t.Fatalf("expected sequence 42, got %v", cmd.ExpectedSequenceNumber)
	}
	if !json.Valid(cmd.Payload) {
		t.Fatalf("payload not valid JSON: %s", cmd.Payload)
	}
}

func TestDecodeDefaultsSchemaVersion(t *testing.T) {
	raw := []byte(`{"commandId":"cmd_2","type":"CreateRoom","payload":{}}`)
	cmd, err := Decode(raw)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if cmd.SchemaVersion != CurrentSchemaVersion {
		t.Fatalf("schemaVersion=%d, want %d", cmd.SchemaVersion, CurrentSchemaVersion)
	}
}

func TestDecodeAllowsZeroExpectedSequence(t *testing.T) {
	raw := []byte(`{
		"commandId":"cmd_z",
		"type":"PlayCard",
		"expectedSequenceNumber":0,
		"schemaVersion":1,
		"payload":{}
	}`)
	cmd, err := Decode(raw)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if cmd.ExpectedSequenceNumber == nil || *cmd.ExpectedSequenceNumber != 0 {
		t.Fatalf("expected sequence 0, got %v", cmd.ExpectedSequenceNumber)
	}
}

func TestDecodeRejectsMissingFields(t *testing.T) {
	cases := []struct {
		name string
		raw  string
		want string
	}{
		{"missing commandId", `{"type":"PlayCard","schemaVersion":1,"payload":{}}`, "commandId"},
		{"missing type", `{"commandId":"cmd_3","schemaVersion":1,"payload":{}}`, "type"},
		{"missing payload", `{"commandId":"cmd_5","type":"PlayCard","schemaVersion":1}`, "payload"},
		{"null payload", `{"commandId":"cmd_6","type":"PlayCard","schemaVersion":1,"payload":null}`, "payload"},
		{"array payload", `{"commandId":"cmd_7","type":"PlayCard","schemaVersion":1,"payload":[]}`, "JSON object"},
		{"negative sequence", `{"commandId":"cmd_8","type":"PlayCard","expectedSequenceNumber":-1,"schemaVersion":1,"payload":{}}`, "expectedSequenceNumber"},
		{"blank commandId", `{"commandId":"  ","type":"PlayCard","schemaVersion":1,"payload":{}}`, "commandId"},
		{"blank type", `{"commandId":"cmd_9","type":"\t","schemaVersion":1,"payload":{}}`, "type"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Decode([]byte(tc.raw))
			if err == nil {
				t.Fatalf("expected error containing %q", tc.want)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %q does not contain %q", err, tc.want)
			}
		})
	}
}

func TestDecodeRejectsMalformedJSON(t *testing.T) {
	if _, err := Decode([]byte(`{`)); err == nil {
		t.Fatal("expected decode error")
	}
}

func TestValidateDirect(t *testing.T) {
	seq := int64(-3)
	cmd := Command{
		CommandID:              "cmd",
		Type:                   "PlayCard",
		SchemaVersion:          1,
		ExpectedSequenceNumber: &seq,
		Payload:                json.RawMessage(`{}`),
	}
	if err := cmd.Validate(); err == nil || !strings.Contains(err.Error(), "expectedSequenceNumber") {
		t.Fatalf("expected sequence error, got %v", err)
	}

	badSchema := Command{
		CommandID:     "cmd",
		Type:          "PlayCard",
		SchemaVersion: 0,
		Payload:       json.RawMessage(`{}`),
	}
	if err := badSchema.Validate(); err == nil || !strings.Contains(err.Error(), "schemaVersion") {
		t.Fatalf("expected schemaVersion error, got %v", err)
	}
}

func TestAcceptedAndRejectedResults(t *testing.T) {
	seq := int64(7)
	accepted := Accepted("cmd_a", "PlayCard", &seq, json.RawMessage(`{"ok":true}`))
	if accepted.Status != StatusAccepted || accepted.SchemaVersion != CurrentSchemaVersion {
		t.Fatalf("accepted: %+v", accepted)
	}
	if accepted.Sequence == nil || *accepted.Sequence != 7 {
		t.Fatalf("accepted sequence: %+v", accepted.Sequence)
	}
	rejected := Rejected("cmd_b", "PlayCard", "stale_sequence", &seq)
	if rejected.Status != StatusRejected || rejected.Reason != "stale_sequence" {
		t.Fatalf("rejected: %+v", rejected)
	}
	if rejected.Payload != nil {
		t.Fatalf("rejected should not carry payload: %s", rejected.Payload)
	}
}

func TestValidatePayloadHelpers(t *testing.T) {
	if err := validatePayload(nil); err == nil {
		t.Fatal("expected error for nil payload")
	}
	if err := validatePayload(json.RawMessage(`"x"`)); err == nil {
		t.Fatal("expected error for non-object payload")
	}
	if err := validatePayload(json.RawMessage(`{`)); err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}
