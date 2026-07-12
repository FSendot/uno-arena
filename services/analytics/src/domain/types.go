package domain

import "fmt"

// Visibility classifies how a gameplay metric may be stored.
type Visibility string

const (
	VisibilityAnonymizedAdhoc  Visibility = "anonymized_adhoc"
	VisibilityPublic           Visibility = "public"
	VisibilityPublicTournament Visibility = "public_tournament"
)

func (v Visibility) String() string { return string(v) }

func (v Visibility) Valid() bool {
	switch v {
	case VisibilityAnonymizedAdhoc, VisibilityPublic, VisibilityPublicTournament:
		return true
	default:
		return false
	}
}

// RequiresAnonymization reports whether player identity must be stripped.
func (v Visibility) RequiresAnonymization() bool {
	return v == VisibilityAnonymizedAdhoc
}

// AllowsPublicPlayerFacts reports whether visibility classifies the metric as
// already-public tournament player facts. Provenance still requires a trusted
// SourceTopic and tournamentId; visibility alone is never proof.
func (v Visibility) AllowsPublicPlayerFacts() bool {
	return v == VisibilityPublicTournament
}

// EventType is an analytics upstream event type.
type EventType string

const (
	EventGameplayMetric      EventType = "GameplayMetric"
	EventTournamentStatistic EventType = "TournamentStatistic"
	EventRatingStatistic     EventType = "RatingStatistic"
	EventLeaderboardSnapshot EventType = "LeaderboardSnapshot"
)

// SourceTopic is the trusted Kafka topic / adapter-boundary provenance for an
// upstream event. Adapters must set this; payload visibility is never proof.
type SourceTopic string

const (
	SourceRoomGameplayMetrics           SourceTopic = "room.gameplay.metrics"
	SourceRoomMatchCompleted            SourceTopic = "room.match.completed"
	SourceTournamentMatchAssigned       SourceTopic = "tournament.match.assigned"
	SourceTournamentMatchResultRecorded SourceTopic = "tournament.match.result_recorded"
	SourceTournamentPlayersAdvanced     SourceTopic = "tournament.players.advanced"
	SourceTournamentRoundCompleted      SourceTopic = "tournament.round.completed"
	SourceTournamentCompleted           SourceTopic = "tournament.completed"
	SourceRankingPlayerRatingUpdated    SourceTopic = "ranking.player_rating_updated"
	SourceRankingLeaderboardSnapshot    SourceTopic = "ranking.leaderboard_snapshot_published"
)

func (s SourceTopic) String() string { return string(s) }

func (s SourceTopic) Valid() bool {
	switch s {
	case SourceRoomGameplayMetrics,
		SourceRoomMatchCompleted,
		SourceTournamentMatchAssigned,
		SourceTournamentMatchResultRecorded,
		SourceTournamentPlayersAdvanced,
		SourceTournamentRoundCompleted,
		SourceTournamentCompleted,
		SourceRankingPlayerRatingUpdated,
		SourceRankingLeaderboardSnapshot:
		return true
	default:
		return false
	}
}

// AllowsPublicPlayerFacts reports whether this trusted source may carry
// already-public tournament player display facts into analytics.
func (s SourceTopic) AllowsPublicPlayerFacts() bool {
	return s == SourceRoomGameplayMetrics
}

// RejectionCode classifies why an event was not projected.
type RejectionCode string

const (
	RejectInvalidIdentity  RejectionCode = "invalid_identity"
	RejectInvalidSchema    RejectionCode = "invalid_schema"
	RejectUnknownEventType RejectionCode = "unknown_event_type"
	RejectForbiddenField   RejectionCode = "forbidden_field"
	RejectDisallowedField  RejectionCode = "disallowed_field"
	RejectAnonymization    RejectionCode = "anonymization_required"
	RejectNonPublicSource  RejectionCode = "non_public_source"
	RejectPayloadConflict  RejectionCode = "payload_conflict"
)

// Rejection is a typed non-mutating outcome detail.
type Rejection struct {
	Code    RejectionCode
	Message string
}

func (r Rejection) Error() string {
	if r.Message != "" {
		return fmt.Sprintf("%s: %s", r.Code, r.Message)
	}
	return string(r.Code)
}

// OutcomeKind classifies projection apply results.
type OutcomeKind string

const (
	OutcomeAccepted    OutcomeKind = "accepted"
	OutcomeDuplicate   OutcomeKind = "duplicate"
	OutcomeQuarantined OutcomeKind = "quarantined"
	OutcomeIgnored     OutcomeKind = "ignored"
)

// ApplyOutcome is the stable result of projecting one upstream event.
type ApplyOutcome struct {
	Kind      OutcomeKind
	EventID   EventID
	Rejection *Rejection
	Facts     []Fact
}

func (o ApplyOutcome) Accepted() bool {
	return o.Kind == OutcomeAccepted || (o.Kind == OutcomeDuplicate && o.Rejection == nil)
}

func (o ApplyOutcome) Mutated() bool {
	return o.Kind == OutcomeAccepted
}

// GameplayMetric is a derived, non-authoritative gameplay reporting row.
type GameplayMetric struct {
	EventID              EventID      `json:"eventId"`
	CorrelationID        string       `json:"correlationId"`
	RoomID               RoomID       `json:"roomId"`
	GameID               GameID       `json:"gameId"`
	TournamentID         TournamentID `json:"tournamentId"`
	Visibility           Visibility   `json:"visibility"`
	MetricType           string       `json:"metricType"`
	PublicCardRank       string       `json:"publicCardRank"`
	PublicCardColor      string       `json:"publicCardColor"`
	PublicCardCountTotal uint16       `json:"publicCardCountTotal"`
	RoomSequence         uint64       `json:"roomSequence"`
	// PublicPlayerID is set only for already-public tournament/public metrics.
	PublicPlayerID PlayerID `json:"publicPlayerId,omitempty"`
	DisplayName    string   `json:"displayName,omitempty"`
}

// TournamentStatistic is a derived public tournament reporting row.
type TournamentStatistic struct {
	EventID              EventID           `json:"eventId"`
	CorrelationID        string            `json:"correlationId"`
	TournamentID         TournamentID      `json:"tournamentId"`
	RoundNumber          int32             `json:"roundNumber"`
	SlotID               string            `json:"slotId"`
	EventType            string            `json:"eventType"`
	Phase                string            `json:"phase"`
	RegisteredCount      uint32            `json:"registeredCount"`
	AdvancingPlayerCount uint16            `json:"advancingPlayerCount"`
	PublicPayload        map[string]string `json:"publicPayload"`
}

// PlayerPublicStatistic is a derived public rating/leaderboard reporting row.
type PlayerPublicStatistic struct {
	EventID        EventID    `json:"eventId"`
	CorrelationID  string     `json:"correlationId"`
	PlayerID       PlayerID   `json:"playerId"`
	SourceType     string     `json:"sourceType"`
	PreviousRating int32      `json:"previousRating"`
	NewRating      int32      `json:"newRating"`
	BoardType      string     `json:"boardType"`
	SnapshotID     SnapshotID `json:"snapshotId"`
}

// AnalyticsSnapshot is a defensive public read of derived analytics state.
// Authoritative is always false: analytics never owns domain decisions.
type AnalyticsSnapshot struct {
	Authoritative     bool                    `json:"authoritative"`
	ProjectionVersion ProjectionVersion       `json:"projectionVersion"`
	GameplayMetrics   []GameplayMetric        `json:"gameplayMetrics"`
	TournamentStats   []TournamentStatistic   `json:"tournamentStats"`
	RatingStats       []PlayerPublicStatistic `json:"ratingStats"`
}
