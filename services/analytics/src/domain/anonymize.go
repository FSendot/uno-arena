package domain

// AnonymizationPolicy strips player identity from ad-hoc gameplay metrics
// before they enter public analytics projections.
type AnonymizationPolicy struct{}

// identityFields must never appear on anonymized_adhoc projections.
var identityFields = map[string]struct{}{
	"playerid":    {},
	"displayname": {},
	"player":      {},
	"players":     {},
	"username":    {},
	"user":        {},
}

// AnonymizeGameplayPayload returns a defensive copy with identity fields removed.
// Callers must only invoke this for visibility that RequiresAnonymization.
func (AnonymizationPolicy) AnonymizeGameplayPayload(payload map[string]any) map[string]any {
	if payload == nil {
		return map[string]any{}
	}
	return stripIdentity(payload)
}

// ContainsIdentity reports whether payload still carries player-identifying fields.
func (AnonymizationPolicy) ContainsIdentity(payload map[string]any) bool {
	if payload == nil {
		return false
	}
	return hasIdentity(payload)
}

func stripIdentity(v any) map[string]any {
	m, ok := v.(map[string]any)
	if !ok || m == nil {
		return map[string]any{}
	}
	out := make(map[string]any, len(m))
	for k, child := range m {
		norm := normalizeFieldKey(k)
		if _, id := identityFields[norm]; id {
			continue
		}
		out[k] = stripIdentityValue(child)
	}
	return out
}

func stripIdentityValue(v any) any {
	switch x := v.(type) {
	case map[string]any:
		return stripIdentity(x)
	case []any:
		out := make([]any, len(x))
		for i, child := range x {
			if nested, ok := child.(map[string]any); ok {
				out[i] = stripIdentity(nested)
			} else {
				out[i] = child
			}
		}
		return out
	default:
		return x
	}
}

func hasIdentity(v any) bool {
	switch x := v.(type) {
	case map[string]any:
		for k, child := range x {
			if _, id := identityFields[normalizeFieldKey(k)]; id {
				return true
			}
			if hasIdentity(child) {
				return true
			}
		}
	case []any:
		for _, child := range x {
			if hasIdentity(child) {
				return true
			}
		}
	}
	return false
}
