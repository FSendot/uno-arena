package store_test

import (
	"os"
	"strings"
	"testing"
)

func TestReplaceActiveSessionLocksPlayerRowBeforeSessions(t *testing.T) {
	b, err := os.ReadFile("postgres_session.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(b)
	idx := strings.Index(src, "func (s *SessionStore) ReplaceActiveSession")
	if idx < 0 {
		t.Fatal("ReplaceActiveSession missing")
	}
	rest := src[idx+1:]
	end := strings.Index(rest, "\nfunc ")
	body := rest
	if end > 0 {
		body = rest[:end]
	}
	pIdx := strings.Index(body, "FROM players")
	sIdx := strings.Index(body, "FROM sessions")
	if pIdx < 0 || sIdx < 0 || pIdx > sIdx {
		t.Fatalf("player FOR UPDATE must precede sessions read: p=%d s=%d", pIdx, sIdx)
	}
	if !strings.Contains(body[pIdx:pIdx+80], "FOR UPDATE") {
		t.Fatal("players select must use FOR UPDATE")
	}
}
