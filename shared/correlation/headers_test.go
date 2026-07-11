package correlation

import (
	"net/http"
	"testing"
)

func TestFromHTTPAndApply(t *testing.T) {
	in := http.Header{}
	in.Set(HeaderCorrelationID, "corr_1")
	in.Set(HeaderCommandID, "cmd_1")
	in.Set(HeaderCausationID, "cause_1")
	in.Set(HeaderRequestID, "req_1")
	got := FromHTTP(in)
	if got.CorrelationID != "corr_1" || got.CommandID != "cmd_1" {
		t.Fatalf("FromHTTP: %+v", got)
	}
	if got.CausationID != "cause_1" || got.RequestID != "req_1" {
		t.Fatalf("FromHTTP extras: %+v", got)
	}
	out := http.Header{}
	got.Apply(out)
	if out.Get(HeaderCorrelationID) != "corr_1" {
		t.Fatalf("Apply missing correlation: %v", out)
	}
	if out.Get(HeaderCommandID) != "cmd_1" || out.Get(HeaderCausationID) != "cause_1" {
		t.Fatalf("Apply incomplete: %v", out)
	}
}

func TestApplyNilHeaderIsNoop(t *testing.T) {
	Headers{CorrelationID: "c"}.Apply(nil)
}

func TestFromHTTPTrimsWhitespace(t *testing.T) {
	in := http.Header{}
	in.Set(HeaderCorrelationID, "  corr  ")
	got := FromHTTP(in)
	if got.CorrelationID != "corr" {
		t.Fatalf("got %q", got.CorrelationID)
	}
}

func TestWithDefaults(t *testing.T) {
	h := Headers{CommandID: "cmd_9"}.WithDefaults()
	if h.CorrelationID != "cmd_9" {
		t.Fatalf("expected commandId fallback, got %q", h.CorrelationID)
	}
	h = Headers{RequestID: "req_2", CommandID: "cmd_9"}.WithDefaults()
	if h.CorrelationID != "req_2" {
		t.Fatalf("expected requestId preference, got %q", h.CorrelationID)
	}
	h = Headers{CorrelationID: "keep"}.WithDefaults()
	if h.CorrelationID != "keep" {
		t.Fatalf("should preserve existing correlationId, got %q", h.CorrelationID)
	}
	h = Headers{}.WithDefaults()
	if h.CorrelationID != "" {
		t.Fatalf("empty headers should stay empty, got %q", h.CorrelationID)
	}
}

func TestFromMapAndToMap(t *testing.T) {
	src := map[string]string{
		HeaderCorrelationID: "corr_m",
		HeaderCommandID:     "cmd_m",
		HeaderCausationID:   "cause_m",
		HeaderRequestID:     "req_m",
	}
	got := FromMap(src)
	if got.CorrelationID != "corr_m" || got.CommandID != "cmd_m" {
		t.Fatalf("FromMap: %+v", got)
	}
	round := got.ToMap()
	if round[HeaderCorrelationID] != "corr_m" || round[HeaderRequestID] != "req_m" {
		t.Fatalf("ToMap: %+v", round)
	}
	if FromMap(nil).CorrelationID != "" {
		t.Fatal("nil map should yield empty headers")
	}
	partial := Headers{CommandID: "only"}.ToMap()
	if len(partial) != 1 || partial[HeaderCommandID] != "only" {
		t.Fatalf("partial ToMap: %+v", partial)
	}
}
