package domain

import (
	"strings"
)

// SpectatorAuth is the authorization input for spectator admission.
// Public rooms allow anonymous-tolerant admission; private rooms require a
// trusted participant (playerId on roster + session context), scoped invite
// capability, or operator scope — never a blanket authorized flag.
type SpectatorAuth struct {
	IsPublicRoom bool
	PlayerID     PlayerID
	SessionID    string
	HasInvite    bool
	IsOperator   bool
}

// SpectatorAdmissionDecision is a typed admission outcome, not a domain event.
type SpectatorAdmissionDecision struct {
	Allowed bool
	Code    RejectionCode
	Reason  string
}

// EvaluateSpectatorAdmission enforces waiting/locked/in_progress only with
// public/private authorization. Terminal room/match status denies admission.
// Individual game completion inside a best-of-three does not deny admission
// while the room remains in_progress.
//
// participants is the trusted projected roster; private admission requires
// playerId ∈ participants with a non-empty sessionId, or invite/operator scope.
func EvaluateSpectatorAdmission(status RoomStatus, auth SpectatorAuth, participants map[PlayerID]struct{}) SpectatorAdmissionDecision {
	if status.IsTerminal() {
		return SpectatorAdmissionDecision{
			Allowed: false,
			Code:    RejectSpectatorTerminal,
			Reason:  "spectator admission denied after terminal room/match state",
		}
	}
	if !status.AllowsSpectatorAdmission() {
		return SpectatorAdmissionDecision{
			Allowed: false,
			Code:    RejectSpectatorTerminal,
			Reason:  "spectator admission denied for room status",
		}
	}
	if auth.IsPublicRoom {
		return SpectatorAdmissionDecision{Allowed: true}
	}
	if auth.IsOperator {
		return SpectatorAdmissionDecision{Allowed: true}
	}
	if auth.HasInvite {
		return SpectatorAdmissionDecision{Allowed: true}
	}
	if auth.PlayerID.Valid() {
		if strings.TrimSpace(auth.SessionID) == "" {
			return SpectatorAdmissionDecision{
				Allowed: false,
				Code:    RejectSpectatorUnauthorized,
				Reason:  "private room participant admission requires session context",
			}
		}
		if _, ok := participants[auth.PlayerID]; ok {
			return SpectatorAdmissionDecision{Allowed: true}
		}
		return SpectatorAdmissionDecision{
			Allowed: false,
			Code:    RejectSpectatorUnauthorized,
			Reason:  "private room requires playerId on participant roster",
		}
	}
	return SpectatorAdmissionDecision{
		Allowed: false,
		Code:    RejectSpectatorUnauthorized,
		Reason:  "private room requires participant, invite, or operator scope",
	}
}

// SpectatorVisibilityPolicy enforces the strict allowed-field / event-type schema
// for room.spectator-safe events before any projection state mutation.
type SpectatorVisibilityPolicy struct{}

var allowedEventTypes = map[EventType]struct{}{
	EventRoomCreated:           {},
	EventPlayerJoinedRoom:      {},
	EventPlayerLeftRoom:        {},
	EventHostReassigned:        {},
	EventRoomLocked:            {},
	EventMatchStarted:          {},
	EventGameStarted:           {},
	EventCardPlayed:            {},
	EventTurnAdvanced:          {},
	EventColorChosen:           {},
	EventDirectionChanged:      {},
	EventPenaltyUpdated:        {},
	EventUnoWindowOpened:       {},
	EventUnoWindowClosed:       {},
	EventUnoWindowExpired:      {},
	EventGameCompleted:         {},
	EventMatchCompleted:        {},
	EventRoomCompleted:         {},
	EventRoomCancelled:         {},
	EventSpectatorStreamsClose: {},
	EventPlayerDisconnected:    {},
	EventPlayerReconnected:     {},
	EventPlayerForfeited:       {},
	EventSnapshotSanitized:     {},
}

var allowedFields = map[string]struct{}{
	"playerid":              {},
	"displayname":           {},
	"seatindex":             {},
	"cardcount":             {},
	"handcount":             {},
	"roster":                {},
	"seats":                 {},
	"discardtop":            {},
	"discard":               {},
	"activecolor":           {},
	"color":                 {},
	"direction":             {},
	"currentplayerid":       {},
	"turnplayerid":          {},
	"penaltyamount":         {},
	"penaltytarget":         {},
	"drawpilesize":          {},
	"gamewins":              {},
	"matchwins":             {},
	"gamescore":             {},
	"score":                 {},
	"matchwinner":           {},
	"winnerplayerid":        {},
	"status":                {},
	"visibility":            {},
	"roomstatus":            {},
	"expiresat":             {},
	"openingsequence":       {},
	"openingroomsequence":   {},
	"gameid":                {},
	"called":                {},
	"unowindow":             {},
	"triggeringgameeventid": {},
	"hostplayerid":          {},
	"roomtype":              {},
	"placementorder":        {},
	"isabandoned":           {},
	"completionreason":      {},
	"players":               {},
	"matchwinsbyplayer":     {},
	"gamenumber":            {},
	"occupied":              {},
	"completionversion":     {},
	"completedat":           {},
	"reason":                {},
	"tournamentid":          {},
	"roundnumber":           {},
	"slotid":                {},
	"gamecompleted":         {},
}

var playerIDKeyedMaps = map[string]struct{}{
	"gamewins":          {},
	"matchwins":         {},
	"gamescore":         {},
	"score":             {},
	"matchwinsbyplayer": {},
}

var forbiddenFields = map[string]struct{}{
	"hand":           {},
	"hands":          {},
	"cards":          {},
	"privatehand":    {},
	"drawncards":     {},
	"drawcards":      {},
	"cardidentity":   {},
	"drawncardids":   {},
	"drawidentity":   {},
	"deck":           {},
	"deckorder":      {},
	"hiddendeck":     {},
	"remainingdeck":  {},
	"seed":           {},
	"deckseed":       {},
	"session":        {},
	"sessionid":      {},
	"sessiontoken":   {},
	"token":          {},
	"accesstoken":    {},
	"refreshtoken":   {},
	"idempotencykey": {},
	"idempotency":    {},
	"commandid":      {},
	"audit":          {},
	"auditid":        {},
	"auditrecord":    {},
	"password":       {},
	"secret":         {},
	"privatepayload": {},
	"opponenthands":  {},
	"opponenthand":   {},
	"playerhand":     {},
}

func (SpectatorVisibilityPolicy) IsAllowedEventType(t EventType) bool {
	_, ok := allowedEventTypes[t]
	return ok
}

// ValidateEventType rejects event types outside the spectator-safe allowlist.
func (p SpectatorVisibilityPolicy) ValidateEventType(t EventType) *Rejection {
	if t == "" {
		return &Rejection{Code: RejectInvalidSchema, Message: "eventType is required"}
	}
	if !p.IsAllowedEventType(t) {
		return &Rejection{
			Code:    RejectUnknownEventType,
			Message: "event type not allowed for spectator projection: " + string(t),
		}
	}
	return nil
}

// ValidateEnvelope checks required versioned envelope fields and event type.
func (p SpectatorVisibilityPolicy) ValidateEnvelope(evt SpectatorSafeEvent) *Rejection {
	if !evt.EventID.Valid() {
		return &Rejection{Code: RejectInvalidIdentity, Message: "eventId is required"}
	}
	if !evt.RoomID.Valid() {
		return &Rejection{Code: RejectInvalidIdentity, Message: "roomId is required"}
	}
	if evt.SchemaVersion < 1 || evt.SchemaVersion != CurrentSchemaVersion {
		return &Rejection{Code: RejectInvalidSchema, Message: "unsupported schemaVersion"}
	}
	if evt.Sequence < 1 {
		return &Rejection{Code: RejectInvalidSchema, Message: "sequence must be >= 1"}
	}
	if evt.EventType == "" {
		return &Rejection{Code: RejectInvalidSchema, Message: "eventType is required"}
	}
	if !p.IsAllowedEventType(evt.EventType) {
		return &Rejection{
			Code:    RejectUnknownEventType,
			Message: "event type not allowed for spectator projection: " + string(evt.EventType),
		}
	}
	return nil
}

// ValidatePayload returns a rejection when forbidden or disallowed fields are present.
func (p SpectatorVisibilityPolicy) ValidatePayload(payload map[string]any) *Rejection {
	if payload == nil {
		return nil
	}
	if rej := RejectNestedEnvelope(payload); rej != nil {
		return rej
	}
	if reason, msg := validateObject(payload, ""); reason != "" {
		return &Rejection{Code: reason, Message: msg}
	}
	return nil
}

// IsTerminalEvent reports whether the event closes the room/match spectator stream.
// MatchCompleted and SpectatorStreamsClose map into the same terminal stream-close
// semantics as RoomCompleted/RoomCancelled without exposing private fields.
func IsTerminalEvent(t EventType) bool {
	switch t {
	case EventRoomCompleted, EventRoomCancelled, EventMatchCompleted, EventSpectatorStreamsClose:
		return true
	default:
		return false
	}
}

// RejectNestedEnvelope returns a rejection when payload looks like a nested
// spectator-safe envelope (roomId+eventType/sequence + inner payload) instead of
// flat fact data from the canonical Room/Gateway envelope.
func RejectNestedEnvelope(payload map[string]any) *Rejection {
	if payload == nil {
		return nil
	}
	_, hasInnerPayload := payload["payload"]
	_, hasEventType := payload["eventType"]
	if !hasEventType {
		_, hasEventType = payload["event"]
	}
	_, hasSeq := payload["sequenceNumber"]
	if !hasSeq {
		_, hasSeq = payload["sequence"]
	}
	_, hasRoom := payload["roomId"]
	if hasInnerPayload && (hasEventType || hasSeq || hasRoom) {
		return &Rejection{
			Code:    RejectInvalidSchema,
			Message: "nested spectator-safe envelope is not accepted; use canonical flat data",
		}
	}
	return nil
}

func validateObject(m map[string]any, path string) (RejectionCode, string) {
	for k, child := range m {
		norm := normalizeFieldKey(k)
		full := fieldPath(path, k)
		if _, bad := forbiddenFields[norm]; bad {
			return RejectForbiddenField, "forbidden private field: " + full
		}
		if _, ok := allowedFields[norm]; !ok {
			return RejectDisallowedField, "field not in spectator allowlist: " + full
		}
		if _, skipKeys := playerIDKeyedMaps[norm]; skipKeys {
			if nested, ok := child.(map[string]any); ok {
				for nk, nv := range nested {
					nn := normalizeFieldKey(nk)
					if _, bad := forbiddenFields[nn]; bad {
						return RejectForbiddenField, "forbidden private field: " + fieldPath(full, nk)
					}
					if code, msg := validateValue(nv, fieldPath(full, nk)); code != "" {
						return code, msg
					}
				}
			}
			continue
		}
		if code, msg := validateValue(child, full); code != "" {
			return code, msg
		}
	}
	return "", ""
}

func validateValue(v any, path string) (RejectionCode, string) {
	switch x := v.(type) {
	case map[string]any:
		return validateObject(x, path)
	case []any:
		for i, child := range x {
			if code, msg := validateValue(child, path+"["+itoa(i)+"]"); code != "" {
				return code, msg
			}
		}
	}
	return "", ""
}

func normalizeFieldKey(k string) string {
	return strings.ToLower(strings.ReplaceAll(strings.TrimSpace(k), "_", ""))
}

func fieldPath(parent, key string) string {
	if parent == "" {
		return key
	}
	return parent + "." + key
}
