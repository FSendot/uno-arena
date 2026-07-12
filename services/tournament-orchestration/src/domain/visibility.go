package domain

import (
	"fmt"
	"strings"
)

// TournamentVisibility is the tournament-owned public/private read policy.
// Distinct from Room Visibility (different bounded context).
type TournamentVisibility string

const (
	TournamentVisibilityPublic  TournamentVisibility = "public"
	TournamentVisibilityPrivate TournamentVisibility = "private"
)

func (v TournamentVisibility) String() string { return string(v) }

func (v TournamentVisibility) Valid() bool {
	return v == TournamentVisibilityPublic || v == TournamentVisibilityPrivate
}

// NormalizeTournamentVisibility maps raw create payloads to a visibility value.
// Empty defaults to public; unknown values are rejected.
func NormalizeTournamentVisibility(raw string) (TournamentVisibility, error) {
	switch strings.TrimSpace(raw) {
	case "", string(TournamentVisibilityPublic):
		return TournamentVisibilityPublic, nil
	case string(TournamentVisibilityPrivate):
		return TournamentVisibilityPrivate, nil
	default:
		return "", fmt.Errorf("invalid tournament visibility %q", raw)
	}
}
