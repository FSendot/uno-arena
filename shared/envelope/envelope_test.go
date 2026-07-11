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

func TestDecodeRejectsOmittedSchemaVersion(t *testing.T) {
	raw := []byte(`{"commandId":"cmd_2","type":"CreateRoom","payload":{}}`)
	_, err := Decode(raw)
	if err == nil || !strings.Contains(err.Error(), "schemaVersion") {
		t.Fatalf("want schemaVersion required error, got %v", err)
	}
}

func TestDecodeRequiresExplicitSchemaVersion(t *testing.T) {
	raw := []byte(`{"commandId":"cmd_2","type":"CreateRoom","schemaVersion":1,"payload":{}}`)
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
		{"missing commandId", `{"type":"PlayCard","expectedSequenceNumber":1,"schemaVersion":1,"payload":{}}`, "commandId"},
		{"missing type", `{"commandId":"cmd_3","expectedSequenceNumber":1,"schemaVersion":1,"payload":{}}`, "type"},
		{"missing payload", `{"commandId":"cmd_5","type":"PlayCard","expectedSequenceNumber":1,"schemaVersion":1}`, "payload"},
		{"null payload", `{"commandId":"cmd_6","type":"PlayCard","expectedSequenceNumber":1,"schemaVersion":1,"payload":null}`, "payload"},
		{"array payload", `{"commandId":"cmd_7","type":"PlayCard","expectedSequenceNumber":1,"schemaVersion":1,"payload":[]}`, "JSON object"},
		{"negative sequence", `{"commandId":"cmd_8","type":"PlayCard","expectedSequenceNumber":-1,"schemaVersion":1,"payload":{}}`, "expectedSequenceNumber"},
		{"blank commandId", `{"commandId":"  ","type":"PlayCard","expectedSequenceNumber":1,"schemaVersion":1,"payload":{}}`, "commandId"},
		{"blank type", `{"commandId":"cmd_9","type":"\t","expectedSequenceNumber":1,"schemaVersion":1,"payload":{}}`, "type"},
		{"missing schemaVersion", `{"commandId":"cmd_sv","type":"CreateRoom","payload":{}}`, "schemaVersion"},
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

	v2 := Command{
		CommandID:     "cmd",
		Type:          "CreateRoom",
		SchemaVersion: 2,
		Payload:       json.RawMessage(`{}`),
	}
	if err := v2.Validate(); err == nil || !strings.Contains(err.Error(), "schemaVersion") {
		t.Fatalf("expected schemaVersion exactly 1 error, got %v", err)
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

func TestDecodeRejectsUnknownTopLevelField(t *testing.T) {
	raw := []byte(`{
		"commandId":"cmd_1",
		"type":"CreateRoom",
		"schemaVersion":1,
		"payload":{},
		"extra":true
	}`)
	_, err := Decode(raw)
	if err == nil || !strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("want unknown field error, got %v", err)
	}
}

func TestDecodeRejectsUnknownPayloadFieldForKnownCommand(t *testing.T) {
	raw := []byte(`{
		"commandId":"cmd_1",
		"type":"PlayCard",
		"expectedSequenceNumber":1,
		"schemaVersion":1,
		"payload":{"roomId":"r1","cardId":"red-7","sneaky":true}
	}`)
	_, err := Decode(raw)
	if err == nil || !strings.Contains(err.Error(), "unknown payload field") {
		t.Fatalf("want unknown payload field error, got %v", err)
	}
}

func TestDecodeRejectsForbiddenExpectedSequence(t *testing.T) {
	cases := []string{"CreateRoom", "CreateTournament", "RegisterPlayer", "CloseRegistration"}
	for _, typ := range cases {
		t.Run(typ, func(t *testing.T) {
			payload := `{}`
			if typ != "CreateRoom" {
				payload = `{"tournamentId":"t1"}`
			}
			raw := []byte(`{
				"commandId":"cmd_1",
				"type":"` + typ + `",
				"expectedSequenceNumber":0,
				"schemaVersion":1,
				"payload":` + payload + `
			}`)
			_, err := Decode(raw)
			if err == nil || !strings.Contains(err.Error(), "expectedSequenceNumber is not allowed") {
				t.Fatalf("want prohibited sequence error, got %v", err)
			}
		})
	}
}

func TestDecodeRequiresExpectedSequenceForExistingRoomMutations(t *testing.T) {
	types := []string{
		"JoinRoom", "LeaveRoom", "LockRoom", "StartMatch", "CancelRoom",
		"PlayCard", "DrawCard", "ChooseColor", "CallUno", "ReportMissingUno", "ReconnectToRoom",
	}
	for _, typ := range types {
		t.Run(typ, func(t *testing.T) {
			raw := []byte(`{
				"commandId":"cmd_1",
				"type":"` + typ + `",
				"schemaVersion":1,
				"payload":{"roomId":"r1"}
			}`)
			_, err := Decode(raw)
			if err == nil || !strings.Contains(err.Error(), "expectedSequenceNumber is required") {
				t.Fatalf("want required sequence error, got %v", err)
			}
		})
	}
}

func TestDecodeRejectsSchemaVersionOtherThanOne(t *testing.T) {
	raw := []byte(`{"commandId":"cmd_2","type":"CreateRoom","schemaVersion":2,"payload":{}}`)
	_, err := Decode(raw)
	if err == nil || !strings.Contains(err.Error(), "schemaVersion") {
		t.Fatalf("want schemaVersion exactly 1 error, got %v", err)
	}
}

func TestDecodeRejectsUnknownCommandType(t *testing.T) {
	raw := []byte(`{
		"commandId":"cmd_x",
		"type":"NotARealCommand",
		"expectedSequenceNumber":9,
		"schemaVersion":1,
		"payload":{"anything":true}
	}`)
	_, err := Decode(raw)
	if err == nil || !strings.Contains(err.Error(), "unknown command type") {
		t.Fatalf("want unknown command type error, got %v", err)
	}
}

func TestDecodeAcceptsCatalogPayloadAllowlists(t *testing.T) {
	raw := []byte(`{
		"commandId":"cmd_1",
		"type":"CreateRoom",
		"schemaVersion":1,
		"payload":{"roomId":"r1","visibility":"public","maxSeats":4}
	}`)
	if _, err := Decode(raw); err != nil {
		t.Fatalf("Decode: %v", err)
	}
}

func TestDecodeRejectsInvalidPayloadTypesEnumsRanges(t *testing.T) {
	cases := []struct {
		name string
		raw  string
		want string
	}{
		{
			"non-string roomId",
			`{"commandId":"c","type":"JoinRoom","expectedSequenceNumber":1,"schemaVersion":1,"payload":{"roomId":1}}`,
			"must be a string",
		},
		{
			"non-integer disconnectVersion",
			`{"commandId":"c","type":"ReconnectToRoom","expectedSequenceNumber":1,"schemaVersion":1,"payload":{"disconnectVersion":"1"}}`,
			"must be an integer",
		},
		{
			"float disconnectVersion",
			`{"commandId":"c","type":"ReconnectToRoom","expectedSequenceNumber":1,"schemaVersion":1,"payload":{"disconnectVersion":1.5}}`,
			"must be an integer",
		},
		{
			"negative disconnectVersion",
			`{"commandId":"c","type":"ReconnectToRoom","expectedSequenceNumber":1,"schemaVersion":1,"payload":{"disconnectVersion":-1}}`,
			"must be >= 0",
		},
		{
			"invalid visibility",
			`{"commandId":"c","type":"CreateRoom","schemaVersion":1,"payload":{"visibility":"friends"}}`,
			"invalid value",
		},
		{
			"non-string visibility",
			`{"commandId":"c","type":"CreateRoom","schemaVersion":1,"payload":{"visibility":true}}`,
			"must be a string",
		},
		{
			"maxSeats 1",
			`{"commandId":"c","type":"CreateRoom","schemaVersion":1,"payload":{"maxSeats":1}}`,
			"maxSeats",
		},
		{
			"maxSeats 11",
			`{"commandId":"c","type":"CreateRoom","schemaVersion":1,"payload":{"maxSeats":11}}`,
			"maxSeats",
		},
		{
			"maxSeats float",
			`{"commandId":"c","type":"CreateRoom","schemaVersion":1,"payload":{"maxSeats":4.2}}`,
			"must be an integer",
		},
		{
			"invalid ChooseColor",
			`{"commandId":"c","type":"ChooseColor","expectedSequenceNumber":1,"schemaVersion":1,"payload":{"color":"purple"}}`,
			"invalid value",
		},
		{
			"CreateTournament obsolete name field",
			`{"commandId":"c","type":"CreateTournament","schemaVersion":1,"payload":{"name":"Open"}}`,
			"unknown payload field",
		},
		{
			"capacity below minimum",
			`{"commandId":"c","type":"CreateTournament","schemaVersion":1,"payload":{"capacity":0}}`,
			"must be >= 1",
		},
		{
			"null optional string",
			`{"commandId":"c","type":"PlayCard","expectedSequenceNumber":1,"schemaVersion":1,"payload":{"cardId":null}}`,
			"must not be null",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Decode([]byte(tc.raw))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("want error containing %q, got %v", tc.want, err)
			}
		})
	}
}

func TestDecodeAcceptsValidPayloadBounds(t *testing.T) {
	cases := []string{
		`{"commandId":"c","type":"CreateRoom","schemaVersion":1,"payload":{"maxSeats":0}}`,
		`{"commandId":"c","type":"CreateRoom","schemaVersion":1,"payload":{"maxSeats":-3}}`,
		`{"commandId":"c","type":"CreateRoom","schemaVersion":1,"payload":{"maxSeats":2}}`,
		`{"commandId":"c","type":"CreateRoom","schemaVersion":1,"payload":{"maxSeats":10}}`,
		`{"commandId":"c","type":"CreateRoom","schemaVersion":1,"payload":{"visibility":"private"}}`,
		`{"commandId":"c","type":"ChooseColor","expectedSequenceNumber":1,"schemaVersion":1,"payload":{"color":"red"}}`,
		`{"commandId":"c","type":"ChooseColor","expectedSequenceNumber":1,"schemaVersion":1,"payload":{"color":"yellow"}}`,
		`{"commandId":"c","type":"ChooseColor","expectedSequenceNumber":1,"schemaVersion":1,"payload":{"color":"green"}}`,
		`{"commandId":"c","type":"ChooseColor","expectedSequenceNumber":1,"schemaVersion":1,"payload":{"color":"blue"}}`,
		`{"commandId":"c","type":"ReconnectToRoom","expectedSequenceNumber":1,"schemaVersion":1,"payload":{"disconnectVersion":0}}`,
		`{"commandId":"c","type":"CreateTournament","schemaVersion":1,"payload":{"tournamentId":"t1","capacity":8,"retryBudget":0,"batchSize":1}}`,
		`{"commandId":"c","type":"CreateRoom","schemaVersion":1,"payload":{}}`,
	}
	for _, raw := range cases {
		if _, err := Decode([]byte(raw)); err != nil {
			t.Fatalf("Decode(%s): %v", raw, err)
		}
	}
}

func TestProhibitsExpectedSequence(t *testing.T) {
	if !ProhibitsExpectedSequence("CreateRoom") || !ProhibitsExpectedSequence("CreateTournament") {
		t.Fatal("CreateRoom/tournament must prohibit sequence")
	}
	if ProhibitsExpectedSequence("PlayCard") || ProhibitsExpectedSequence("Nope") {
		t.Fatal("existing-room/unknown must not prohibit via this helper")
	}
}

func TestRequiresExpectedSequence(t *testing.T) {
	if !RequiresExpectedSequence("PlayCard") || !RequiresExpectedSequence("JoinRoom") {
		t.Fatal("existing-room mutations must require sequence")
	}
	if RequiresExpectedSequence("CreateRoom") || RequiresExpectedSequence("CreateTournament") || RequiresExpectedSequence("Nope") {
		t.Fatal("create/tournament/unknown must not require sequence via this helper")
	}
}

func TestIsPublicCommandType(t *testing.T) {
	if !IsPublicCommandType("CreateRoom") || !IsPublicCommandType("PlayCard") || !IsPublicCommandType("CloseRegistration") {
		t.Fatal("catalog types must be public")
	}
	if IsPublicCommandType("NotARealCommand") || IsPublicCommandType("") {
		t.Fatal("unknown types must not be public")
	}
	if len(payloadSchemas) != 15 {
		t.Fatalf("public catalog size=%d want 15", len(payloadSchemas))
	}
}
