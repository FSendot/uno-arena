package domain

import "strings"

// Allowed append event types: accepted gameplay/lifecycle commands only.
// Rejected commands and audit/rejection records are never appendable.
var allowedAppendEventTypes = map[string]struct{}{
	"CreateRoom":              {},
	"JoinRoom":                {},
	"LeaveRoom":               {},
	"LockRoom":                {},
	"StartMatch":              {},
	"StartNextGame":           {},
	"CancelRoom":              {},
	"PlayCard":                {},
	"DrawCard":                {},
	"ChooseColor":             {},
	"CallUno":                 {},
	"ReportMissingUno":        {},
	"ReconnectToRoom":         {},
	"DisconnectPlayer":        {},
	"ExpireUnoWindow":         {},
	"ForfeitPlayer":           {},
	"SkipDisconnectedTurn":    {},
	"ProvisionTournamentRoom": {},
}

// IsAppendEventTypeAllowed reports whether eventType may be appended to the room log.
func IsAppendEventTypeAllowed(eventType string) bool {
	if eventType == "" {
		return false
	}
	if isExplicitlyRejectedAppendType(eventType) {
		return false
	}
	_, ok := allowedAppendEventTypes[eventType]
	return ok
}

func isExplicitlyRejectedAppendType(eventType string) bool {
	switch eventType {
	case "CommandRejected", "RejectionRecord", "AuditRecord", "RejectionAudit":
		return true
	}
	lower := strings.ToLower(eventType)
	if strings.Contains(lower, "reject") || strings.Contains(lower, "audit") {
		return true
	}
	return false
}
