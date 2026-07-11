// Package httpx provides stdlib JSON response and error helpers for HTTP APIs.
package httpx

import (
	"encoding/json"
	"fmt"
	"net/http"
)

// ErrorBody is the standard BFF/service error payload.
type ErrorBody struct {
	Code          string `json:"code"`
	Message       string `json:"message"`
	CorrelationID string `json:"correlationId,omitempty"`
	CommandID     string `json:"commandId,omitempty"`
}

// WriteJSON writes a JSON response with the given status code.
func WriteJSON(w http.ResponseWriter, status int, v any) error {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		return fmt.Errorf("write json: %w", err)
	}
	return nil
}

// WriteError writes a structured JSON error response.
func WriteError(w http.ResponseWriter, status int, code, message, correlationID, commandID string) error {
	return WriteJSON(w, status, ErrorBody{
		Code:          code,
		Message:       message,
		CorrelationID: correlationID,
		CommandID:     commandID,
	})
}

// DecodeJSON decodes a JSON request body into dest.
func DecodeJSON(r *http.Request, dest any) error {
	defer r.Body.Close()
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dest); err != nil {
		return fmt.Errorf("decode json: %w", err)
	}
	return nil
}
