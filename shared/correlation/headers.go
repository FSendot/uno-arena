// Package correlation defines HTTP/Kafka correlation header names and helpers.
package correlation

import (
	"net/http"
	"strings"
)

// Canonical header names propagated across BFF and internal hops.
const (
	HeaderCorrelationID = "X-Correlation-Id"
	HeaderCommandID     = "X-Command-Id"
	HeaderCausationID   = "X-Causation-Id"
	HeaderRequestID     = "X-Request-Id"
)

// Headers is a typed set of correlation identifiers.
type Headers struct {
	CorrelationID string
	CommandID     string
	CausationID   string
	RequestID     string
}

// FromHTTP extracts correlation headers from an incoming request.
func FromHTTP(h http.Header) Headers {
	return Headers{
		CorrelationID: strings.TrimSpace(h.Get(HeaderCorrelationID)),
		CommandID:     strings.TrimSpace(h.Get(HeaderCommandID)),
		CausationID:   strings.TrimSpace(h.Get(HeaderCausationID)),
		RequestID:     strings.TrimSpace(h.Get(HeaderRequestID)),
	}
}

// FromMap extracts correlation headers from a string map (e.g. Kafka headers).
func FromMap(m map[string]string) Headers {
	if m == nil {
		return Headers{}
	}
	return Headers{
		CorrelationID: strings.TrimSpace(m[HeaderCorrelationID]),
		CommandID:     strings.TrimSpace(m[HeaderCommandID]),
		CausationID:   strings.TrimSpace(m[HeaderCausationID]),
		RequestID:     strings.TrimSpace(m[HeaderRequestID]),
	}
}

// Apply writes correlation headers onto an outgoing HTTP header map.
func (c Headers) Apply(h http.Header) {
	if h == nil {
		return
	}
	if c.CorrelationID != "" {
		h.Set(HeaderCorrelationID, c.CorrelationID)
	}
	if c.CommandID != "" {
		h.Set(HeaderCommandID, c.CommandID)
	}
	if c.CausationID != "" {
		h.Set(HeaderCausationID, c.CausationID)
	}
	if c.RequestID != "" {
		h.Set(HeaderRequestID, c.RequestID)
	}
}

// ToMap returns a map of non-empty correlation headers for propagation.
func (c Headers) ToMap() map[string]string {
	out := make(map[string]string, 4)
	if c.CorrelationID != "" {
		out[HeaderCorrelationID] = c.CorrelationID
	}
	if c.CommandID != "" {
		out[HeaderCommandID] = c.CommandID
	}
	if c.CausationID != "" {
		out[HeaderCausationID] = c.CausationID
	}
	if c.RequestID != "" {
		out[HeaderRequestID] = c.RequestID
	}
	return out
}

// WithDefaults fills missing CorrelationID from RequestID or CommandID when present.
func (c Headers) WithDefaults() Headers {
	out := c
	if out.CorrelationID == "" {
		switch {
		case out.RequestID != "":
			out.CorrelationID = out.RequestID
		case out.CommandID != "":
			out.CorrelationID = out.CommandID
		}
	}
	return out
}
