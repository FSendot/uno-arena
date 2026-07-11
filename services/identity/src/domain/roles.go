package domain

import (
	"sort"
	"strings"
)

// DefaultAllowedRoles is the safe OIDC ACL allowlist when none is configured.
var DefaultAllowedRoles = []string{"player"}

// NormalizeAcceptedRoles trims, lowercases, dedupes, drops blanks, always includes
// "player", and keeps only roles present in allowlist (default: player).
func NormalizeAcceptedRoles(roles, allowlist []string) []string {
	if len(allowlist) == 0 {
		allowlist = DefaultAllowedRoles
	}
	allowed := make(map[string]struct{}, len(allowlist))
	for _, a := range allowlist {
		a = strings.ToLower(strings.TrimSpace(a))
		if a != "" {
			allowed[a] = struct{}{}
		}
	}
	if _, ok := allowed["player"]; !ok {
		allowed["player"] = struct{}{}
	}
	seen := map[string]struct{}{"player": {}}
	out := []string{"player"}
	for _, r := range roles {
		r = strings.ToLower(strings.TrimSpace(r))
		if r == "" || r == "player" {
			continue
		}
		if _, ok := allowed[r]; !ok {
			continue
		}
		if _, dup := seen[r]; dup {
			continue
		}
		seen[r] = struct{}{}
		out = append(out, r)
	}
	sort.Strings(out)
	return out
}
