package domain

// SpectatorAuth is the authorization input for spectator admission.
// Public rooms allow anonymous-tolerant admission; private rooms require
// an authorized invite, participant, or operator context.
type SpectatorAuth struct {
	IsPublicRoom bool
	Authorized   bool // invite, participant, or operator for private rooms
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
func EvaluateSpectatorAdmission(status RoomStatus, auth SpectatorAuth) SpectatorAdmissionDecision {
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
	if auth.Authorized {
		return SpectatorAdmissionDecision{Allowed: true}
	}
	return SpectatorAdmissionDecision{
		Allowed: false,
		Code:    RejectSpectatorUnauthorized,
		Reason:  "private room requires authorized spectator context",
	}
}
