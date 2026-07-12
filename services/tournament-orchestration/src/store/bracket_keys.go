package store

import (
	"fmt"
	"strings"
)

const (
	bracketKeySchemaVersion = "v1"
	defaultBracketKeyPrefix = "tournament:"
)

// BracketKeySpace builds versioned Redis keys for Tournament bracket projections.
// Tournament-id hash tags keep meta/summary/chunk/index keys in one Redis Cluster
// slot so Lua can touch them atomically (mirrors Ranking {boardType}):
//
//	tournament:v1:br:{tournamentId}:meta
//	tournament:v1:br:{tournamentId}:gen:{N}:summary
//	tournament:v1:br:{tournamentId}:gen:{N}:chunk:{roundNumber}:{batchID}
//	tournament:v1:br:{tournamentId}:gen:{N}:idx       // ZSET score→"round:slot"
//	tournament:v1:br:{tournamentId}:gen:{N}:slotmap   // HASH "round:slot"→batchID
//	tournament:v1:br:{tournamentId}:gen:{N}:chunkset  // SET "round:batch" (bounded cleanup)
//
// Payloads are never one tournament-sized JSON document: summary is separate from
// per-batch chunk arrays (≤ MaxProvisioningBatchSize). The idx ZSET + slotmap are
// lightweight keyset paging aids — not full slot payloads. chunkset inventories
// batch keys for O(batches) rebuild cleanup (never HGETALL of the slotmap).
//
// Optional override via TOURNAMENT_REDIS_KEY_PREFIX (default tournament:).
type BracketKeySpace struct {
	prefix string
}

// NewBracketKeySpace returns a key space. Empty prefix defaults to tournament:.
func NewBracketKeySpace(prefix string) BracketKeySpace {
	p := strings.TrimSpace(prefix)
	if p == "" {
		p = defaultBracketKeyPrefix
	}
	if !strings.HasSuffix(p, ":") {
		p += ":"
	}
	return BracketKeySpace{prefix: p}
}

// Prefix returns the configured key prefix.
func (k BracketKeySpace) Prefix() string { return k.prefix }

// ScanPattern matches all keys under this prefix (integration cleanup).
func (k BracketKeySpace) ScanPattern() string { return k.prefix + "*" }

// TournamentRoot is the shared key root including the cluster hash-tag.
func (k BracketKeySpace) TournamentRoot(tournamentID string) string {
	return fmt.Sprintf("%s%s:br:{%s}:", k.prefix, bracketKeySchemaVersion, tournamentID)
}

func (k BracketKeySpace) tournamentRoot(tournamentID string) string {
	return k.TournamentRoot(tournamentID)
}

// Meta is the tournament metadata hash (generation, projectionVersion, generatedAt, rebuilding_gen, ready).
func (k BracketKeySpace) Meta(tournamentID string) string {
	return k.tournamentRoot(tournamentID) + "meta"
}

// GenSummary is the generation-scoped compact BracketSummary JSON.
func (k BracketKeySpace) GenSummary(tournamentID string, generation int64) string {
	return fmt.Sprintf("%sgen:%d:summary", k.tournamentRoot(tournamentID), generation)
}

// GenChunk is one provisioning-batch slot chunk JSON array (never whole-bracket).
func (k BracketKeySpace) GenChunk(tournamentID string, generation int64, roundNumber int, batchID string) string {
	return fmt.Sprintf("%sgen:%d:chunk:%d:%s", k.tournamentRoot(tournamentID), generation, roundNumber, batchID)
}

// GenIndex is the lightweight ordered ZSET for (roundNumber,slotIndex) keyset paging.
func (k BracketKeySpace) GenIndex(tournamentID string, generation int64) string {
	return fmt.Sprintf("%sgen:%d:idx", k.tournamentRoot(tournamentID), generation)
}

// GenSlotMap maps "round:slot" → batchID for chunk lookup during paging (not full payloads).
func (k BracketKeySpace) GenSlotMap(tournamentID string, generation int64) string {
	return fmt.Sprintf("%sgen:%d:slotmap", k.tournamentRoot(tournamentID), generation)
}

// GenChunkSet inventories "round:batch" members for bounded rebuild cleanup.
func (k BracketKeySpace) GenChunkSet(tournamentID string, generation int64) string {
	return fmt.Sprintf("%sgen:%d:chunkset", k.tournamentRoot(tournamentID), generation)
}

// BracketIndexScore encodes stable numeric (roundNumber, slotIndex) order for ZSET paging.
func BracketIndexScore(roundNumber, slotIndex int) float64 {
	return float64(roundNumber)*1_000_000_000 + float64(slotIndex)
}

// BracketIndexMember is the lightweight idx member "round:slot" (payloads stay in chunks).
func BracketIndexMember(roundNumber, slotIndex int) string {
	return fmt.Sprintf("%d:%d", roundNumber, slotIndex)
}
