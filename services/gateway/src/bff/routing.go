package bff

// Command catalog routing for the OpenAPI BFF surface.

const (
	CmdCreateRoom        = "CreateRoom"
	CmdJoinRoom          = "JoinRoom"
	CmdLeaveRoom         = "LeaveRoom"
	CmdLockRoom          = "LockRoom"
	CmdStartMatch        = "StartMatch"
	CmdCancelRoom        = "CancelRoom"
	CmdPlayCard          = "PlayCard"
	CmdDrawCard          = "DrawCard"
	CmdChooseColor       = "ChooseColor"
	CmdCallUno           = "CallUno"
	CmdReportMissingUno  = "ReportMissingUno"
	CmdReconnectToRoom   = "ReconnectToRoom"
	CmdCreateTournament  = "CreateTournament"
	CmdRegisterPlayer    = "RegisterPlayer"
	CmdCloseRegistration = "CloseRegistration"
)

// Backend identifies which bounded context owns a command type.
type Backend string

const (
	BackendRoom       Backend = "room"
	BackendTournament Backend = "tournament"
	BackendUnknown    Backend = ""
)

var roomCommands = map[string]struct{}{
	CmdCreateRoom:       {},
	CmdJoinRoom:         {},
	CmdLeaveRoom:        {},
	CmdLockRoom:         {},
	CmdStartMatch:       {},
	CmdCancelRoom:       {},
	CmdPlayCard:         {},
	CmdDrawCard:         {},
	CmdChooseColor:      {},
	CmdCallUno:          {},
	CmdReportMissingUno: {},
	CmdReconnectToRoom:  {},
}

var tournamentCommands = map[string]struct{}{
	CmdCreateTournament:  {},
	CmdRegisterPlayer:    {},
	CmdCloseRegistration: {},
}

// RouteBackend maps a command type to its owning backend.
func RouteBackend(commandType string) Backend {
	if _, ok := roomCommands[commandType]; ok {
		return BackendRoom
	}
	if _, ok := tournamentCommands[commandType]; ok {
		return BackendTournament
	}
	return BackendUnknown
}

// RequiresExpectedSequence reports whether the BFF must require
// expectedSequenceNumber before dispatch (mutations of an existing room aggregate).
// CreateRoom is the documented exception.
func RequiresExpectedSequence(commandType string) bool {
	if RouteBackend(commandType) != BackendRoom {
		return false
	}
	return commandType != CmdCreateRoom
}
