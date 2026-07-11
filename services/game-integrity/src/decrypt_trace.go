package main

import (
	"context"
	"sort"
	"sync"
)

type decryptTraceCtxKey struct{}

// decryptTrace collects envelope key versions encountered during one request.
// It never stores plaintext, ciphertext, or key material.
type decryptTrace struct {
	mu       sync.Mutex
	versions map[int]struct{}
}

func withDecryptTrace(ctx context.Context) (context.Context, *decryptTrace) {
	t := &decryptTrace{versions: map[int]struct{}{}}
	return context.WithValue(ctx, decryptTraceCtxKey{}, t), t
}

func fromContextDecryptTrace(ctx context.Context) *decryptTrace {
	if ctx == nil {
		return nil
	}
	t, _ := ctx.Value(decryptTraceCtxKey{}).(*decryptTrace)
	return t
}

func (t *decryptTrace) noteKeyVersion(v int) {
	if t == nil || v <= 0 {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.versions[v] = struct{}{}
}

func (t *decryptTrace) keyVersions() []int {
	if t == nil {
		return nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]int, 0, len(t.versions))
	for v := range t.versions {
		out = append(out, v)
	}
	sort.Ints(out)
	return out
}

func (t *decryptTrace) unknown() bool {
	return len(t.keyVersions()) == 0
}

func noteDecryptKeyVersion(ctx context.Context, keyVersion int) {
	fromContextDecryptTrace(ctx).noteKeyVersion(keyVersion)
}
