package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"
	"strings"
	"time"
)

// UpstreamEvent is a versioned inbound analytics event from a sanitized/public stream.
// Source is required adapter-boundary provenance (trusted topic); never inferred from payload.
//
// IdempotencyKey is the adapter-supplied ADR-0029 contract key for this channel. It does not
// replace EventID on the envelope. When unset (HTTP bridge / rebuild), EffectiveIdempotencyKey
// falls back to EventID. PayloadFingerprint is the immutable identity of the mapped payload;
// when unset, EnsureIngressIdentity derives a deterministic fingerprint from Payload.
type UpstreamEvent struct {
	EventID            EventID
	EventType          EventType
	Source             SourceTopic
	SchemaVersion      int
	CorrelationID      string
	OccurredAt         time.Time
	Payload            map[string]any
	IdempotencyKey     string
	PayloadFingerprint string
	// DurableIgnore marks an intentional no-projection ingest (e.g. ad-hoc MatchCompleted
	// without tournamentId). ADR-0029 still requires a durable processed marker before commit.
	DurableIgnore bool
}

type storedIngressOutcome struct {
	Outcome     ApplyOutcome
	Fingerprint string
}

// PublicAnalyticsProjection is the aggregate consistency boundary for derived,
// non-authoritative public analytics. Pure stdlib; not goroutine-safe.
// It never decides gameplay, advancement, ratings, privacy enforcement, or audit truth.
type PublicAnalyticsProjection struct {
	version     ProjectionVersion
	gameplay    []GameplayMetric
	tournaments []TournamentStatistic
	ratings     []PlayerPublicStatistic

	policy    AnalyticsFieldPolicy
	anonymize AnonymizationPolicy
	outcomes  map[string]storedIngressOutcome // key = EffectiveIdempotencyKey
}

// NewPublicAnalyticsProjection creates an empty rebuildable analytics projection.
func NewPublicAnalyticsProjection() *PublicAnalyticsProjection {
	return &PublicAnalyticsProjection{
		version:     0,
		gameplay:    nil,
		tournaments: nil,
		ratings:     nil,
		policy:      AnalyticsFieldPolicy{},
		anonymize:   AnonymizationPolicy{},
		outcomes:    map[string]storedIngressOutcome{},
	}
}

// EffectiveIdempotencyKey returns the adapter key, falling back to eventId for HTTP/rebuild.
func EffectiveIdempotencyKey(evt UpstreamEvent) string {
	if k := strings.TrimSpace(evt.IdempotencyKey); k != "" {
		return k
	}
	return string(evt.EventID)
}

// FingerprintPayload returns a deterministic sha256 hex of the allowlisted payload map.
func FingerprintPayload(payload map[string]any) string {
	canonical, err := canonicalJSON(payload)
	if err != nil {
		sum := sha256.Sum256([]byte("unfingerprintable"))
		return hex.EncodeToString(sum[:])
	}
	sum := sha256.Sum256(canonical)
	return hex.EncodeToString(sum[:])
}

// EnsureIngressIdentity fills IdempotencyKey / PayloadFingerprint when unset (HTTP bridge).
func EnsureIngressIdentity(evt *UpstreamEvent) {
	if evt == nil {
		return
	}
	if strings.TrimSpace(evt.IdempotencyKey) == "" {
		evt.IdempotencyKey = string(evt.EventID)
	}
	if strings.TrimSpace(evt.PayloadFingerprint) == "" {
		evt.PayloadFingerprint = FingerprintPayload(evt.Payload)
	}
}

func canonicalJSON(v any) ([]byte, error) {
	switch x := v.(type) {
	case map[string]any:
		keys := make([]string, 0, len(x))
		for k := range x {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		buf := []byte{'{'}
		for i, k := range keys {
			if i > 0 {
				buf = append(buf, ',')
			}
			kb, err := json.Marshal(k)
			if err != nil {
				return nil, err
			}
			buf = append(buf, kb...)
			buf = append(buf, ':')
			vb, err := canonicalJSON(x[k])
			if err != nil {
				return nil, err
			}
			buf = append(buf, vb...)
		}
		buf = append(buf, '}')
		return buf, nil
	case []any:
		buf := []byte{'['}
		for i, child := range x {
			if i > 0 {
				buf = append(buf, ',')
			}
			vb, err := canonicalJSON(child)
			if err != nil {
				return nil, err
			}
			buf = append(buf, vb...)
		}
		buf = append(buf, ']')
		return buf, nil
	default:
		return json.Marshal(x)
	}
}

// Authoritative always returns false. Analytics outputs are derived only.
func (p *PublicAnalyticsProjection) Authoritative() bool { return false }

// ProjectionVersion returns the number of accepted mutations.
func (p *PublicAnalyticsProjection) ProjectionVersion() ProjectionVersion { return p.version }

// RebuildFrom creates a projection and reapplies upstream events in order.
func RebuildFrom(events []UpstreamEvent) (*PublicAnalyticsProjection, []ApplyOutcome) {
	p := NewPublicAnalyticsProjection()
	return p, p.RebuildFrom(events)
}

// RebuildFrom resets this projection and reapplies upstream events in order.
func (p *PublicAnalyticsProjection) RebuildFrom(events []UpstreamEvent) []ApplyOutcome {
	*p = *NewPublicAnalyticsProjection()
	outs := make([]ApplyOutcome, 0, len(events))
	for _, evt := range events {
		outs = append(outs, p.Apply(evt))
	}
	return outs
}

// Apply projects one upstream event with contract-key idempotency and fail-closed policy.
// Duplicate EffectiveIdempotencyKey with the same PayloadFingerprint returns the prior
// stable outcome without re-mutation. Conflicting fingerprints quarantine without mutation.
// DurableIgnore writes an ignored outcome marker with no projection rows.
func (p *PublicAnalyticsProjection) Apply(evt UpstreamEvent) ApplyOutcome {
	EnsureIngressIdentity(&evt)
	key := EffectiveIdempotencyKey(evt)
	fp := strings.TrimSpace(evt.PayloadFingerprint)

	if prior, ok := p.outcomes[key]; ok {
		if prior.Fingerprint == fp {
			return duplicateOutcome(prior.Outcome)
		}
		return p.conflictQuarantine(evt, key, fp)
	}

	if evt.DurableIgnore {
		out := ApplyOutcome{
			Kind:    OutcomeIgnored,
			EventID: evt.EventID,
			Facts: []Fact{
				newFact(FactProjectionEventIgnored, map[string]string{
					"eventId":        string(evt.EventID),
					"idempotencyKey": key,
					"source":         string(evt.Source),
					"authoritative":  "false",
					"reason":         "durable_ignore",
				}),
			},
		}
		p.outcomes[key] = storedIngressOutcome{Outcome: out, Fingerprint: fp}
		return copyOutcome(out)
	}

	if rej := p.policy.ValidateEnvelope(evt); rej != nil {
		return p.quarantine(evt, key, fp, *rej)
	}
	if rej := p.policy.ValidatePayload(evt.Payload); rej != nil {
		return p.quarantine(evt, key, fp, *rej)
	}

	switch evt.EventType {
	case EventGameplayMetric:
		return p.projectGameplay(evt, key, fp)
	case EventTournamentStatistic:
		return p.projectTournament(evt, key, fp)
	case EventRatingStatistic, EventLeaderboardSnapshot:
		return p.projectRating(evt, key, fp)
	default:
		return p.quarantine(evt, key, fp, Rejection{
			Code:    RejectUnknownEventType,
			Message: "event type not allowed for analytics projection: " + string(evt.EventType),
		})
	}
}

// ProjectGameplayMetric is the application-facing alias for gameplay projection.
func (p *PublicAnalyticsProjection) ProjectGameplayMetric(evt UpstreamEvent) ApplyOutcome {
	evt.EventType = EventGameplayMetric
	return p.Apply(evt)
}

// ProjectTournamentStatistic is the application-facing alias for tournament projection.
func (p *PublicAnalyticsProjection) ProjectTournamentStatistic(evt UpstreamEvent) ApplyOutcome {
	evt.EventType = EventTournamentStatistic
	return p.Apply(evt)
}

// ProjectRatingStatistic is the application-facing alias for rating projection.
func (p *PublicAnalyticsProjection) ProjectRatingStatistic(evt UpstreamEvent) ApplyOutcome {
	evt.EventType = EventRatingStatistic
	return p.Apply(evt)
}

func (p *PublicAnalyticsProjection) projectGameplay(evt UpstreamEvent, key, fp string) ApplyOutcome {
	pl := copyAnyMap(evt.Payload)

	visRaw, rej := optionalString(pl, "visibility")
	if rej != nil {
		return p.quarantine(evt, key, fp, *rej)
	}
	vis := Visibility(visRaw)
	if !vis.Valid() {
		return p.quarantine(evt, key, fp, Rejection{
			Code:    RejectInvalidSchema,
			Message: "visibility must be anonymized_adhoc, public, or public_tournament",
		})
	}

	metricType, rej := optionalString(pl, "metricType")
	if rej != nil {
		return p.quarantine(evt, key, fp, *rej)
	}
	if metricType == "" {
		metricType, rej = optionalString(pl, "metric_type")
		if rej != nil {
			return p.quarantine(evt, key, fp, *rej)
		}
	}
	if metricType == "" {
		return p.quarantine(evt, key, fp, Rejection{
			Code:    RejectInvalidSchema,
			Message: "metricType is required",
		})
	}

	roomID, rej := optionalString(pl, "roomId")
	if rej != nil {
		return p.quarantine(evt, key, fp, *rej)
	}
	gameID, rej := optionalString(pl, "gameId")
	if rej != nil {
		return p.quarantine(evt, key, fp, *rej)
	}
	tournamentID, rej := optionalString(pl, "tournamentId")
	if rej != nil {
		return p.quarantine(evt, key, fp, *rej)
	}
	publicCardRank, rej := optionalString(pl, "publicCardRank")
	if rej != nil {
		return p.quarantine(evt, key, fp, *rej)
	}
	publicCardColor, rej := optionalString(pl, "publicCardColor")
	if rej != nil {
		return p.quarantine(evt, key, fp, *rej)
	}
	publicCard, rej := optionalString(pl, "publicCard")
	if rej != nil {
		return p.quarantine(evt, key, fp, *rej)
	}
	cardCountTotal, rej := optionalUint16(pl, "publicCardCountTotal")
	if rej != nil {
		return p.quarantine(evt, key, fp, *rej)
	}
	if cardCountTotal == 0 {
		cardCountTotal, rej = optionalUint16(pl, "publicCardCount")
		if rej != nil {
			return p.quarantine(evt, key, fp, *rej)
		}
	}
	roomSequence, rej := optionalUint64(pl, "roomSequence")
	if rej != nil {
		return p.quarantine(evt, key, fp, *rej)
	}
	playerID, rej := optionalString(pl, "playerId")
	if rej != nil {
		return p.quarantine(evt, key, fp, *rej)
	}
	displayName, rej := optionalString(pl, "displayName")
	if rej != nil {
		return p.quarantine(evt, key, fp, *rej)
	}

	if vis.RequiresAnonymization() {
		if p.anonymize.ContainsIdentity(pl) {
			pl = p.anonymize.AnonymizeGameplayPayload(pl)
		}
		if p.anonymize.ContainsIdentity(pl) {
			return p.quarantine(evt, key, fp, Rejection{
				Code:    RejectAnonymization,
				Message: "ad-hoc gameplay metric retained player identity after anonymization",
			})
		}
		playerID = ""
		displayName = ""
	}

	hasPlayerFacts := playerID != "" || displayName != ""
	if hasPlayerFacts {
		// Visibility is classification only; trusted source + tournament provenance required.
		if !evt.Source.AllowsPublicPlayerFacts() || !vis.AllowsPublicPlayerFacts() || tournamentID == "" {
			return p.quarantine(evt, key, fp, Rejection{
				Code:    RejectNonPublicSource,
				Message: "public player facts require trusted gameplay source, public_tournament visibility, and tournamentId provenance",
			})
		}
	}

	metric := GameplayMetric{
		EventID:              evt.EventID,
		CorrelationID:        evt.CorrelationID,
		RoomID:               RoomID(roomID),
		GameID:               GameID(gameID),
		TournamentID:         TournamentID(tournamentID),
		Visibility:           vis,
		MetricType:           metricType,
		PublicCardRank:       publicCardRank,
		PublicCardColor:      publicCardColor,
		PublicCardCountTotal: cardCountTotal,
		RoomSequence:         roomSequence,
	}
	if metric.PublicCardRank == "" && metric.PublicCardColor == "" && publicCard != "" {
		metric.PublicCardRank, metric.PublicCardColor = splitPublicCard(publicCard)
	}
	if hasPlayerFacts {
		metric.PublicPlayerID = PlayerID(playerID)
		metric.DisplayName = displayName
	}

	p.gameplay = append(p.gameplay, metric)
	p.version++

	out := ApplyOutcome{
		Kind:    OutcomeAccepted,
		EventID: evt.EventID,
		Facts: []Fact{
			newFact(FactPublicGameplayMetricProjected, map[string]string{
				"eventId":       string(evt.EventID),
				"visibility":    string(vis),
				"metricType":    metric.MetricType,
				"roomId":        string(metric.RoomID),
				"authoritative": "false",
			}),
		},
	}
	p.outcomes[key] = storedIngressOutcome{Outcome: out, Fingerprint: fp}
	return copyOutcome(out)
}

func (p *PublicAnalyticsProjection) projectTournament(evt UpstreamEvent, key, fp string) ApplyOutcome {
	pl := copyAnyMap(evt.Payload)

	tidRaw, rej := optionalString(pl, "tournamentId")
	if rej != nil {
		return p.quarantine(evt, key, fp, *rej)
	}
	tid := TournamentID(tidRaw)
	if !tid.Valid() {
		return p.quarantine(evt, key, fp, Rejection{
			Code:    RejectInvalidIdentity,
			Message: "tournamentId is required",
		})
	}

	roundNumber, rej := optionalInt32(pl, "roundNumber")
	if rej != nil {
		return p.quarantine(evt, key, fp, *rej)
	}
	slotID, rej := optionalString(pl, "slotId")
	if rej != nil {
		return p.quarantine(evt, key, fp, *rej)
	}
	eventType, rej := optionalString(pl, "eventType")
	if rej != nil {
		return p.quarantine(evt, key, fp, *rej)
	}
	phase, rej := optionalString(pl, "phase")
	if rej != nil {
		return p.quarantine(evt, key, fp, *rej)
	}
	registeredCount, rej := optionalUint32(pl, "registeredCount")
	if rej != nil {
		return p.quarantine(evt, key, fp, *rej)
	}
	advancingCount, rej := optionalUint16(pl, "advancingPlayerCount")
	if rej != nil {
		return p.quarantine(evt, key, fp, *rej)
	}
	publicPayload, rej := extractPublicPayload(pl)
	if rej != nil {
		return p.quarantine(evt, key, fp, *rej)
	}

	stat := TournamentStatistic{
		EventID:              evt.EventID,
		CorrelationID:        evt.CorrelationID,
		TournamentID:         tid,
		RoundNumber:          roundNumber,
		SlotID:               slotID,
		EventType:            eventType,
		Phase:                phase,
		RegisteredCount:      registeredCount,
		AdvancingPlayerCount: advancingCount,
		PublicPayload:        publicPayload,
	}
	if stat.EventType == "" {
		stat.EventType = string(evt.EventType)
	}

	p.tournaments = append(p.tournaments, stat)
	p.version++

	out := ApplyOutcome{
		Kind:    OutcomeAccepted,
		EventID: evt.EventID,
		Facts: []Fact{
			newFact(FactPublicTournamentStatisticProjected, map[string]string{
				"eventId":       string(evt.EventID),
				"tournamentId":  string(tid),
				"phase":         stat.Phase,
				"authoritative": "false",
			}),
		},
	}
	p.outcomes[key] = storedIngressOutcome{Outcome: out, Fingerprint: fp}
	return copyOutcome(out)
}

func (p *PublicAnalyticsProjection) projectRating(evt UpstreamEvent, key, fp string) ApplyOutcome {
	pl := copyAnyMap(evt.Payload)

	if raw, ok := pl["entries"]; ok {
		return p.projectLeaderboardSnapshot(evt, key, fp, pl, raw)
	}

	pidRaw, rej := optionalString(pl, "playerId")
	if rej != nil {
		return p.quarantine(evt, key, fp, *rej)
	}
	pid := PlayerID(pidRaw)
	if !pid.Valid() {
		return p.quarantine(evt, key, fp, Rejection{
			Code:    RejectInvalidIdentity,
			Message: "playerId is required for public rating statistics",
		})
	}

	sourceType, rej := optionalString(pl, "sourceType")
	if rej != nil {
		return p.quarantine(evt, key, fp, *rej)
	}
	prev, rej := optionalInt32(pl, "previousRating")
	if rej != nil {
		return p.quarantine(evt, key, fp, *rej)
	}
	next, rej := optionalInt32(pl, "newRating")
	if rej != nil {
		return p.quarantine(evt, key, fp, *rej)
	}
	boardType, rej := optionalString(pl, "boardType")
	if rej != nil {
		return p.quarantine(evt, key, fp, *rej)
	}
	snapshotID, rej := optionalString(pl, "snapshotId")
	if rej != nil {
		return p.quarantine(evt, key, fp, *rej)
	}

	stat := PlayerPublicStatistic{
		EventID:        evt.EventID,
		CorrelationID:  evt.CorrelationID,
		PlayerID:       pid,
		SourceType:     sourceType,
		PreviousRating: prev,
		NewRating:      next,
		BoardType:      boardType,
		SnapshotID:     SnapshotID(snapshotID),
	}
	p.ratings = append(p.ratings, stat)
	p.version++

	out := ApplyOutcome{
		Kind:    OutcomeAccepted,
		EventID: evt.EventID,
		Facts: []Fact{
			newFact(FactPublicRatingStatisticProjected, map[string]string{
				"eventId":       string(evt.EventID),
				"playerId":      string(pid),
				"sourceType":    stat.SourceType,
				"authoritative": "false",
			}),
		},
	}
	p.outcomes[key] = storedIngressOutcome{Outcome: out, Fingerprint: fp}
	return copyOutcome(out)
}

func (p *PublicAnalyticsProjection) projectLeaderboardSnapshot(evt UpstreamEvent, key, fp string, pl map[string]any, raw any) ApplyOutcome {
	entries, ok := raw.([]any)
	if !ok {
		return p.quarantine(evt, key, fp, Rejection{
			Code:    RejectInvalidSchema,
			Message: "entries must be an array of public rating rows",
		})
	}

	boardType, rej := optionalString(pl, "boardType")
	if rej != nil {
		return p.quarantine(evt, key, fp, *rej)
	}
	snapshotIDRaw, rej := optionalString(pl, "snapshotId")
	if rej != nil {
		return p.quarantine(evt, key, fp, *rej)
	}
	snapshotID := SnapshotID(snapshotIDRaw)
	sourceType, rej := optionalString(pl, "sourceType")
	if rej != nil {
		return p.quarantine(evt, key, fp, *rej)
	}

	// Validate every entry into temp state; append atomically only if entire event is valid.
	pending := make([]PlayerPublicStatistic, 0, len(entries))
	for i, item := range entries {
		row, ok := item.(map[string]any)
		if !ok {
			return p.quarantine(evt, key, fp, Rejection{
				Code:    RejectInvalidSchema,
				Message: "leaderboard entry must be an object at index " + itoa(i),
			})
		}
		pidRaw, rej := optionalString(row, "playerId")
		if rej != nil {
			return p.quarantine(evt, key, fp, *rej)
		}
		pid := PlayerID(pidRaw)
		if !pid.Valid() {
			return p.quarantine(evt, key, fp, Rejection{
				Code:    RejectInvalidIdentity,
				Message: "playerId is required on public rating rows",
			})
		}
		rating, rej := optionalInt32(row, "rating")
		if rej != nil {
			return p.quarantine(evt, key, fp, *rej)
		}
		if _, hasRank := row["rank"]; !hasRank || row["rank"] == nil {
			return p.quarantine(evt, key, fp, Rejection{
				Code:    RejectInvalidSchema,
				Message: "rank is required on leaderboard entries",
			})
		}
		rank, rej := optionalInt32(row, "rank")
		if rej != nil {
			return p.quarantine(evt, key, fp, *rej)
		}
		if rank < 1 {
			return p.quarantine(evt, key, fp, Rejection{
				Code:    RejectInvalidSchema,
				Message: "rank must be >= 1",
			})
		}
		_ = rank
		pending = append(pending, PlayerPublicStatistic{
			EventID:       evt.EventID,
			CorrelationID: evt.CorrelationID,
			PlayerID:      pid,
			SourceType:    sourceType,
			NewRating:     rating,
			BoardType:     boardType,
			SnapshotID:    snapshotID,
		})
	}

	p.ratings = append(p.ratings, pending...)
	p.version++
	out := ApplyOutcome{
		Kind:    OutcomeAccepted,
		EventID: evt.EventID,
		Facts: []Fact{
			newFact(FactPublicRatingStatisticProjected, map[string]string{
				"eventId":       string(evt.EventID),
				"snapshotId":    string(snapshotID),
				"boardType":     boardType,
				"authoritative": "false",
			}),
		},
	}
	p.outcomes[key] = storedIngressOutcome{Outcome: out, Fingerprint: fp}
	return copyOutcome(out)
}

// Snapshot returns a defensive copy of derived analytics state.
func (p *PublicAnalyticsProjection) Snapshot() AnalyticsSnapshot {
	gameplay := make([]GameplayMetric, len(p.gameplay))
	copy(gameplay, p.gameplay)

	tournaments := make([]TournamentStatistic, len(p.tournaments))
	for i, t := range p.tournaments {
		tournaments[i] = t
		tournaments[i].PublicPayload = copyStringMap(t.PublicPayload)
	}

	ratings := make([]PlayerPublicStatistic, len(p.ratings))
	copy(ratings, p.ratings)

	return AnalyticsSnapshot{
		Authoritative:     false,
		ProjectionVersion: p.version,
		GameplayMetrics:   gameplay,
		TournamentStats:   tournaments,
		RatingStats:       ratings,
	}
}

// SnapshotJSON encodes the public snapshot; private fields are absent by construction.
func (p *PublicAnalyticsProjection) SnapshotJSON() ([]byte, error) {
	return json.Marshal(p.Snapshot())
}

func (p *PublicAnalyticsProjection) quarantine(evt UpstreamEvent, key, fp string, rej Rejection) ApplyOutcome {
	facts := []Fact{
		newFact(FactProjectionEventQuarantined, map[string]string{
			"eventId":       string(evt.EventID),
			"eventType":     string(evt.EventType),
			"code":          string(rej.Code),
			"reason":        rej.Message,
			"authoritative": "false",
		}),
	}
	out := ApplyOutcome{
		Kind:      OutcomeQuarantined,
		EventID:   evt.EventID,
		Rejection: &rej,
		Facts:     facts,
	}
	if key != "" {
		p.outcomes[key] = storedIngressOutcome{Outcome: out, Fingerprint: fp}
	}
	return copyOutcome(out)
}

// conflictQuarantine returns a conflict disposition without replacing a durable prior marker
// in memory when fingerprints disagree. The prior first-wins outcome remains stored.
func (p *PublicAnalyticsProjection) conflictQuarantine(evt UpstreamEvent, key, fp string) ApplyOutcome {
	rej := Rejection{
		Code:    RejectPayloadConflict,
		Message: "idempotency key collides with a different immutable payload fingerprint",
	}
	_ = key
	_ = fp
	return ApplyOutcome{
		Kind:      OutcomeQuarantined,
		EventID:   evt.EventID,
		Rejection: &rej,
		Facts: []Fact{
			newFact(FactProjectionEventQuarantined, map[string]string{
				"eventId":       string(evt.EventID),
				"eventType":     string(evt.EventType),
				"code":          string(rej.Code),
				"reason":        rej.Message,
				"authoritative": "false",
			}),
		},
	}
}

func duplicateOutcome(prior ApplyOutcome) ApplyOutcome {
	dup := copyOutcome(prior)
	dup.Kind = OutcomeDuplicate
	return dup
}

func copyOutcome(in ApplyOutcome) ApplyOutcome {
	out := in
	out.Facts = copyFacts(in.Facts)
	if in.Rejection != nil {
		r := *in.Rejection
		out.Rejection = &r
	}
	return out
}

func copyAnyMap(in map[string]any) map[string]any {
	if in == nil {
		return map[string]any{}
	}
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = deepCopyAny(v)
	}
	return out
}

func deepCopyAny(v any) any {
	switch x := v.(type) {
	case map[string]any:
		return copyAnyMap(x)
	case []any:
		out := make([]any, len(x))
		for i, child := range x {
			out[i] = deepCopyAny(child)
		}
		return out
	default:
		return x
	}
}

// extractPublicPayload validates nested publicPayload with the tournament allowlist.
// Malformed shapes/types quarantine; never fail-open to an empty map.
func extractPublicPayload(pl map[string]any) (map[string]string, *Rejection) {
	raw, ok := pl["publicPayload"]
	if !ok {
		raw, ok = pl["publicPayloadJson"]
	}
	if !ok {
		return map[string]string{}, nil
	}

	switch t := raw.(type) {
	case map[string]any:
		return decodePublicPayloadMap(t)
	case map[string]string:
		tmp := make(map[string]any, len(t))
		for k, v := range t {
			tmp[k] = v
		}
		return decodePublicPayloadMap(tmp)
	case string:
		if t == "" {
			return map[string]string{}, nil
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(t), &m); err != nil {
			return nil, &Rejection{
				Code:    RejectInvalidSchema,
				Message: "publicPayloadJson must be valid JSON object",
			}
		}
		return decodePublicPayloadMap(m)
	default:
		return nil, &Rejection{
			Code:    RejectInvalidSchema,
			Message: "publicPayload must be an object or JSON string",
		}
	}
}

func decodePublicPayloadMap(m map[string]any) (map[string]string, *Rejection) {
	out := make(map[string]string, len(m))
	for k, v := range m {
		norm := normalizeFieldKey(k)
		if _, bad := forbiddenFields[norm]; bad {
			return nil, &Rejection{Code: RejectForbiddenField, Message: "forbidden private field: publicPayload." + k}
		}
		if _, ok := tournamentPublicPayloadAllowed[norm]; !ok {
			return nil, &Rejection{Code: RejectDisallowedField, Message: "field not in nested allowlist: publicPayload." + k}
		}
		s, ok := v.(string)
		if !ok {
			return nil, &Rejection{
				Code:    RejectInvalidSchema,
				Message: "publicPayload values must be strings: publicPayload." + k,
			}
		}
		out[k] = s
	}
	return out, nil
}

func splitPublicCard(card string) (rank, color string) {
	// Expected forms: "red-7", "wild", "yellow-skip"
	for i := 0; i < len(card); i++ {
		if card[i] == '-' {
			return card[i+1:], card[:i]
		}
	}
	return card, ""
}

func optionalString(m map[string]any, key string) (string, *Rejection) {
	v, ok := m[key]
	if !ok || v == nil {
		return "", nil
	}
	s, ok := v.(string)
	if !ok {
		return "", &Rejection{
			Code:    RejectInvalidSchema,
			Message: "field " + key + " must be a string",
		}
	}
	return s, nil
}

func optionalInt32(m map[string]any, key string) (int32, *Rejection) {
	v, ok := m[key]
	if !ok || v == nil {
		return 0, nil
	}
	switch n := v.(type) {
	case int:
		if n < -2147483648 || n > 2147483647 {
			return 0, &Rejection{Code: RejectInvalidSchema, Message: "field " + key + " out of int32 range"}
		}
		return int32(n), nil
	case int32:
		return n, nil
	case int64:
		if n < -2147483648 || n > 2147483647 {
			return 0, &Rejection{Code: RejectInvalidSchema, Message: "field " + key + " out of int32 range"}
		}
		return int32(n), nil
	case float64:
		// JSON numbers decode as float64; accept only integral values in range.
		if n != float64(int64(n)) {
			return 0, &Rejection{Code: RejectInvalidSchema, Message: "field " + key + " must be an integer"}
		}
		if n < -2147483648 || n > 2147483647 {
			return 0, &Rejection{Code: RejectInvalidSchema, Message: "field " + key + " out of int32 range"}
		}
		return int32(n), nil
	default:
		return 0, &Rejection{
			Code:    RejectInvalidSchema,
			Message: "field " + key + " must be an integer",
		}
	}
}

func optionalUint16(m map[string]any, key string) (uint16, *Rejection) {
	n, rej := optionalInt32(m, key)
	if rej != nil {
		return 0, rej
	}
	if n < 0 || n > int32(^uint16(0)) {
		return 0, &Rejection{
			Code:    RejectInvalidSchema,
			Message: "field " + key + " out of uint16 range",
		}
	}
	return uint16(n), nil
}

func optionalUint32(m map[string]any, key string) (uint32, *Rejection) {
	v, ok := m[key]
	if !ok || v == nil {
		return 0, nil
	}
	switch n := v.(type) {
	case int:
		if n < 0 || uint64(n) > uint64(^uint32(0)) {
			return 0, &Rejection{Code: RejectInvalidSchema, Message: "field " + key + " out of uint32 range"}
		}
		return uint32(n), nil
	case int32:
		if n < 0 {
			return 0, &Rejection{Code: RejectInvalidSchema, Message: "field " + key + " out of uint32 range"}
		}
		return uint32(n), nil
	case int64:
		if n < 0 || n > int64(^uint32(0)) {
			return 0, &Rejection{Code: RejectInvalidSchema, Message: "field " + key + " out of uint32 range"}
		}
		return uint32(n), nil
	case float64:
		if n != float64(int64(n)) || n < 0 || n > float64(^uint32(0)) {
			return 0, &Rejection{Code: RejectInvalidSchema, Message: "field " + key + " must be a non-negative integer"}
		}
		return uint32(n), nil
	default:
		return 0, &Rejection{
			Code:    RejectInvalidSchema,
			Message: "field " + key + " must be an integer",
		}
	}
}

func optionalUint64(m map[string]any, key string) (uint64, *Rejection) {
	v, ok := m[key]
	if !ok || v == nil {
		return 0, nil
	}
	switch n := v.(type) {
	case int:
		if n < 0 {
			return 0, &Rejection{Code: RejectInvalidSchema, Message: "field " + key + " out of uint64 range"}
		}
		return uint64(n), nil
	case int64:
		if n < 0 {
			return 0, &Rejection{Code: RejectInvalidSchema, Message: "field " + key + " out of uint64 range"}
		}
		return uint64(n), nil
	case uint64:
		return n, nil
	case float64:
		if n != float64(uint64(n)) || n < 0 {
			return 0, &Rejection{Code: RejectInvalidSchema, Message: "field " + key + " must be a non-negative integer"}
		}
		return uint64(n), nil
	default:
		return 0, &Rejection{
			Code:    RejectInvalidSchema,
			Message: "field " + key + " must be an integer",
		}
	}
}
