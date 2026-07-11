package domain

import "strings"

// AnalyticsFieldPolicy enforces fail-closed allowlists for analytics ingestion.
// Unknown fields and forbidden private fields quarantine the event before mutation.
type AnalyticsFieldPolicy struct{}

var allowedEventTypes = map[EventType]struct{}{
	EventGameplayMetric:      {},
	EventTournamentStatistic: {},
	EventRatingStatistic:     {},
	EventLeaderboardSnapshot: {},
}

// Trusted source/topic -> event types the adapter may deliver on that stream.
var eventTypesBySource = map[SourceTopic]map[EventType]struct{}{
	SourceRoomGameplayMetrics: {
		EventGameplayMetric: {},
	},
	SourceTournamentMatchAssigned: {
		EventTournamentStatistic: {},
	},
	SourceTournamentMatchResultRecorded: {
		EventTournamentStatistic: {},
	},
	SourceTournamentPlayersAdvanced: {
		EventTournamentStatistic: {},
	},
	SourceTournamentRoundCompleted: {
		EventTournamentStatistic: {},
	},
	SourceRankingPlayerRatingUpdated: {
		EventRatingStatistic: {},
	},
	SourceRankingLeaderboardSnapshot: {
		EventLeaderboardSnapshot: {},
	},
}

// Always-forbidden private / session / audit fields (any nesting).
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
	"playeremail":    {},
	"email":          {},
}

// Top-level fields allowed on analytics payloads (event-specific nesting is separate).
var sharedAllowedFields = map[string]struct{}{
	"eventid":              {},
	"eventtype":            {},
	"schemaversion":        {},
	"correlationid":        {},
	"occurredat":           {},
	"visibility":           {},
	"roomid":               {},
	"gameid":               {},
	"tournamentid":         {},
	"metrictype":           {},
	"publiccard":           {},
	"publiccardrank":       {},
	"publiccardcolor":      {},
	"publiccardcount":      {},
	"publiccardcounttotal": {},
	"roomsequence":         {},
	"roundnumber":          {},
	"slotid":               {},
	"phase":                {},
	"registeredcount":      {},
	"advancingplayercount": {},
	"publicpayload":        {},
	"publicpayloadjson":    {},
	"playerid":             {},
	"displayname":          {},
	"sourcetype":           {},
	"previousrating":       {},
	"newrating":            {},
	"boardtype":            {},
	"snapshotid":           {},
	"entries":              {},
	"rating":               {},
}

// Nested allowlist for tournament publicPayload / publicPayloadJson.
var tournamentPublicPayloadAllowed = map[string]struct{}{
	"bracketlabel": {},
	"phaselabel":   {},
	"slotlabel":    {},
	"roundlabel":   {},
	"status":       {},
	"result":       {},
}

// Nested allowlist for each leaderboard entries[] row.
var leaderboardEntryAllowed = map[string]struct{}{
	"playerid":    {},
	"rating":      {},
	"displayname": {},
	"rank":        {},
}

func (AnalyticsFieldPolicy) IsAllowedEventType(t EventType) bool {
	_, ok := allowedEventTypes[t]
	return ok
}

// ValidateEnvelope checks required versioned envelope fields, trusted source, and event type.
func (p AnalyticsFieldPolicy) ValidateEnvelope(evt UpstreamEvent) *Rejection {
	if !evt.EventID.Valid() {
		return &Rejection{Code: RejectInvalidIdentity, Message: "eventId is required"}
	}
	if evt.SchemaVersion < 1 || evt.SchemaVersion != CurrentSchemaVersion {
		return &Rejection{Code: RejectInvalidSchema, Message: "unsupported schemaVersion"}
	}
	if evt.EventType == "" {
		return &Rejection{Code: RejectInvalidSchema, Message: "eventType is required"}
	}
	if !p.IsAllowedEventType(evt.EventType) {
		return &Rejection{
			Code:    RejectUnknownEventType,
			Message: "event type not allowed for analytics projection: " + string(evt.EventType),
		}
	}
	if rej := p.ValidateSource(evt); rej != nil {
		return rej
	}
	return nil
}

// ValidateSource requires adapter-supplied trusted Source/Topic bound to the event type.
// Payload visibility is never accepted as proof of a public/sanitized source.
func (AnalyticsFieldPolicy) ValidateSource(evt UpstreamEvent) *Rejection {
	if !evt.Source.Valid() {
		return &Rejection{
			Code:    RejectNonPublicSource,
			Message: "trusted source/topic is required from adapter boundary",
		}
	}
	allowed, ok := eventTypesBySource[evt.Source]
	if !ok {
		return &Rejection{
			Code:    RejectNonPublicSource,
			Message: "source/topic is not trusted for analytics: " + string(evt.Source),
		}
	}
	if _, ok := allowed[evt.EventType]; !ok {
		return &Rejection{
			Code:    RejectNonPublicSource,
			Message: "event type " + string(evt.EventType) + " is not bound to source " + string(evt.Source),
		}
	}
	return nil
}

// ValidatePayload returns a rejection when forbidden or disallowed fields are present.
// Fail-closed: every field must be on the allowlist and must not be forbidden.
// Nested publicPayload and entries use event-specific allowlists (no opaque fail-open).
func (p AnalyticsFieldPolicy) ValidatePayload(payload map[string]any) *Rejection {
	if payload == nil {
		return nil
	}
	if reason, msg := validateObject(payload, ""); reason != "" {
		return &Rejection{Code: reason, Message: msg}
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
		if _, ok := sharedAllowedFields[norm]; !ok {
			return RejectDisallowedField, "field not in analytics allowlist: " + full
		}
		switch norm {
		case "publicpayload", "publicpayloadjson":
			if code, msg := validatePublicPayloadNode(child, full); code != "" {
				return code, msg
			}
			continue
		case "entries":
			if code, msg := validateLeaderboardEntries(child, full); code != "" {
				return code, msg
			}
			continue
		}
		if code, msg := validateValue(child, full); code != "" {
			return code, msg
		}
	}
	return "", ""
}

func validatePublicPayloadNode(v any, path string) (RejectionCode, string) {
	switch x := v.(type) {
	case map[string]any:
		return validateNestedAllowlist(x, path, tournamentPublicPayloadAllowed)
	case map[string]string:
		tmp := make(map[string]any, len(x))
		for k, s := range x {
			tmp[k] = s
		}
		return validateNestedAllowlist(tmp, path, tournamentPublicPayloadAllowed)
	case string:
		if x == "" {
			return "", ""
		}
		// Typed projection parses JSON later; policy only rejects non-object shapes here
		// when already decoded. Raw JSON strings are validated at extract time.
		return "", ""
	default:
		return RejectInvalidSchema, "publicPayload must be an object or JSON string: " + path
	}
}

func validateLeaderboardEntries(v any, path string) (RejectionCode, string) {
	arr, ok := v.([]any)
	if !ok {
		return RejectInvalidSchema, "entries must be an array: " + path
	}
	for i, child := range arr {
		full := path + "[" + itoa(i) + "]"
		row, ok := child.(map[string]any)
		if !ok {
			return RejectInvalidSchema, "leaderboard entry must be an object: " + full
		}
		if code, msg := validateNestedAllowlist(row, full, leaderboardEntryAllowed); code != "" {
			return code, msg
		}
	}
	return "", ""
}

func validateNestedAllowlist(m map[string]any, path string, allowed map[string]struct{}) (RejectionCode, string) {
	for k, child := range m {
		norm := normalizeFieldKey(k)
		full := fieldPath(path, k)
		if _, bad := forbiddenFields[norm]; bad {
			return RejectForbiddenField, "forbidden private field: " + full
		}
		if _, ok := allowed[norm]; !ok {
			return RejectDisallowedField, "field not in nested allowlist: " + full
		}
		switch child.(type) {
		case map[string]any, []any:
			return RejectInvalidSchema, "nested object/array not allowed at: " + full
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
