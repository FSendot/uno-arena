package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"unoarena/services/tournament-orchestration/domain"
)

// ErrUnauthorizedRead is returned when a private tournament read lacks a principal (HTTP 401).
var ErrUnauthorizedRead = errors.New("tournament read unauthorized")

// ErrForbiddenRead is returned when a principal is authenticated but not allowed (HTTP 403).
var ErrForbiddenRead = errors.New("tournament read forbidden")

// ErrAssignmentNotFound is returned when the tournament or player registration is missing (HTTP 404).
var ErrAssignmentNotFound = errors.New("player assignment not found")

// ReadPrincipal is the trusted principal forwarded by Gateway (never client-spoofable).
type ReadPrincipal struct {
	PlayerID      string
	OperatorScope bool
}

// PlayerAssignmentView is the bounded assignment read model.
type PlayerAssignmentView struct {
	TournamentID       string                `json:"tournamentId"`
	PlayerID           string                `json:"playerId"`
	Visibility         string                `json:"visibility"`
	Phase              string                `json:"phase"`
	RegistrationStatus string                `json:"registrationStatus"`
	CurrentRound       int                   `json:"currentRound"`
	Assignment         *PlayerSlotAssignment `json:"assignment"`
	ProjectionVersion  int64                 `json:"projectionVersion,omitempty"`
	GeneratedAt        *time.Time            `json:"generatedAt,omitempty"`
}

// PlayerSlotAssignment is the latest-round slot mapping for a player (nullable roomId).
type PlayerSlotAssignment struct {
	RoundNumber int    `json:"roundNumber"`
	SlotID      string `json:"slotId"`
	RoomID      string `json:"roomId,omitempty"`
	SlotStatus  string `json:"slotStatus"`
}

// AuthorizeTournamentRead checks public/private bracket/standings access against
// tournaments.visibility and registration status (status != withdrawn).
// Missing tournament → ErrTournamentNotFound.
func (s *TournamentStore) AuthorizeTournamentRead(ctx context.Context, tournamentID string, principal *ReadPrincipal) error {
	if s == nil || s.pool == nil {
		return fmt.Errorf("nil store")
	}
	return authorizeTournamentReadQ(ctx, s.pool, tournamentID, principal)
}

func authorizeTournamentReadQ(ctx context.Context, q dbQuerier, tournamentID string, principal *ReadPrincipal) error {
	var visibility string
	err := q.QueryRow(ctx, `
		SELECT visibility FROM tournaments WHERE tournament_id = $1
	`, tournamentID).Scan(&visibility)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrTournamentNotFound
	}
	if err != nil {
		return wrapUnavailable(err)
	}
	vis, _ := domain.NormalizeTournamentVisibility(visibility)
	if vis == domain.TournamentVisibilityPublic {
		return nil
	}
	if principal == nil || (principal.PlayerID == "" && !principal.OperatorScope) {
		return ErrUnauthorizedRead
	}
	if principal.OperatorScope {
		return nil
	}
	var status string
	err = q.QueryRow(ctx, `
		SELECT status FROM tournament_registrations
		WHERE tournament_id = $1 AND player_id = $2
	`, tournamentID, principal.PlayerID).Scan(&status)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrForbiddenRead
	}
	if err != nil {
		return wrapUnavailable(err)
	}
	if status == "withdrawn" {
		return ErrForbiddenRead
	}
	return nil
}

// LoadPlayerAssignment loads a bounded assignment snapshot without aggregate hydrate.
// Latest mapping uses indexed (tournament_id, player_id, round_number DESC) LIMIT 1.
func (s *TournamentStore) LoadPlayerAssignment(ctx context.Context, tournamentID, playerID string) (PlayerAssignmentView, error) {
	if s == nil || s.pool == nil {
		return PlayerAssignmentView{}, fmt.Errorf("nil store")
	}
	return loadPlayerAssignmentQ(ctx, s.pool, tournamentID, playerID)
}

func loadPlayerAssignmentQ(ctx context.Context, q dbQuerier, tournamentID, playerID string) (PlayerAssignmentView, error) {
	var (
		found             bool
		visibility        string
		phase             string
		rulesRaw          []byte
		regStatus         *string
		projectionVersion int64
		generatedAt       *time.Time
		mapRound          *int
		mapSlotID         *string
		slotStatus        *string
		roomID            *string
	)
	// One statement: tournament + registration + projection + latest indexed mapping
	// LEFT JOIN slot status and assigned room. Discovery uses normalized index only.
	err := q.QueryRow(ctx, `
		WITH t AS (
			SELECT tournament_id, phase, visibility, rules
			FROM tournaments WHERE tournament_id = $1
		),
		reg AS (
			SELECT status FROM tournament_registrations
			WHERE tournament_id = $1 AND player_id = $2
		),
		proj AS (
			SELECT
				COALESCE((SELECT projection_version FROM bracket_projection_versions WHERE tournament_id = $1), 0)
				+ COALESCE((SELECT SUM(version) FROM bracket_projection_shards WHERE tournament_id = $1), 0)
					AS projection_version,
				GREATEST(
					(SELECT generated_at FROM bracket_projection_versions WHERE tournament_id = $1),
					(SELECT MAX(generated_at) FROM bracket_projection_shards WHERE tournament_id = $1)
				) AS generated_at
		),
		latest_map AS (
			SELECT m.round_number, m.slot_id
			FROM tournament_round_slot_players m
			WHERE m.tournament_id = $1 AND m.player_id = $2
			ORDER BY m.round_number DESC
			LIMIT 1
		)
		SELECT
			EXISTS(SELECT 1 FROM t),
			COALESCE((SELECT visibility FROM t), ''),
			COALESCE((SELECT phase FROM t), ''),
			COALESCE((SELECT rules FROM t), '{}'::jsonb),
			(SELECT status FROM reg),
			COALESCE((SELECT projection_version FROM proj), 0),
			(SELECT generated_at FROM proj),
			(SELECT round_number FROM latest_map),
			(SELECT slot_id FROM latest_map),
			(SELECT s.status FROM bracket_slots s, latest_map lm
				WHERE s.tournament_id = $1
				  AND s.round_number = lm.round_number
				  AND s.slot_id = lm.slot_id),
			(SELECT am.room_id FROM assigned_matches am, latest_map lm
				WHERE am.tournament_id = $1
				  AND am.round_number = lm.round_number
				  AND am.slot_id = lm.slot_id)
	`, tournamentID, playerID).Scan(
		&found, &visibility, &phase, &rulesRaw, &regStatus,
		&projectionVersion, &generatedAt,
		&mapRound, &mapSlotID, &slotStatus, &roomID,
	)
	if err != nil {
		return PlayerAssignmentView{}, wrapUnavailable(err)
	}
	if !found {
		return PlayerAssignmentView{}, ErrAssignmentNotFound
	}
	if regStatus == nil {
		return PlayerAssignmentView{}, ErrAssignmentNotFound
	}
	var rules tournamentRules
	jsonUnmarshalRules(rulesRaw, &rules)
	vis, _ := domain.NormalizeTournamentVisibility(visibility)
	view := PlayerAssignmentView{
		TournamentID:       tournamentID,
		PlayerID:           playerID,
		Visibility:         string(vis),
		Phase:              phase,
		RegistrationStatus: *regStatus,
		CurrentRound:       rules.CurrentRound,
		ProjectionVersion:  projectionVersion,
	}
	if generatedAt != nil && !generatedAt.IsZero() {
		t := generatedAt.UTC()
		view.GeneratedAt = &t
	}
	if mapRound != nil && mapSlotID != nil {
		asg := &PlayerSlotAssignment{
			RoundNumber: *mapRound,
			SlotID:      *mapSlotID,
		}
		if slotStatus != nil {
			asg.SlotStatus = *slotStatus
		}
		if roomID != nil && *roomID != "" {
			asg.RoomID = *roomID
		}
		view.Assignment = asg
	}
	return view, nil
}

// AuthorizePlayerAssignment checks assignment read: operator OR same player with
// non-withdrawn registration. Missing tournament/registration → ErrAssignmentNotFound.
func (s *TournamentStore) AuthorizePlayerAssignment(ctx context.Context, tournamentID, requestedPlayerID string, principal *ReadPrincipal) error {
	if s == nil || s.pool == nil {
		return fmt.Errorf("nil store")
	}
	return authorizePlayerAssignmentQ(ctx, s.pool, tournamentID, requestedPlayerID, principal)
}

func authorizePlayerAssignmentQ(ctx context.Context, q dbQuerier, tournamentID, requestedPlayerID string, principal *ReadPrincipal) error {
	if principal == nil || (principal.PlayerID == "" && !principal.OperatorScope) {
		return ErrUnauthorizedRead
	}
	err := q.QueryRow(ctx, `SELECT 1 FROM tournaments WHERE tournament_id = $1`, tournamentID).Scan(new(int))
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrAssignmentNotFound
	}
	if err != nil {
		return wrapUnavailable(err)
	}
	if !principal.OperatorScope && principal.PlayerID != requestedPlayerID {
		return ErrForbiddenRead
	}
	lookupPlayer := requestedPlayerID
	if !principal.OperatorScope {
		lookupPlayer = principal.PlayerID
	}
	var status string
	err = q.QueryRow(ctx, `
		SELECT status FROM tournament_registrations
		WHERE tournament_id = $1 AND player_id = $2
	`, tournamentID, lookupPlayer).Scan(&status)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrAssignmentNotFound
	}
	if err != nil {
		return wrapUnavailable(err)
	}
	if !principal.OperatorScope && status == "withdrawn" {
		return ErrForbiddenRead
	}
	return nil
}
