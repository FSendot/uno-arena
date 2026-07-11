package domain

import "errors"

var (
	ErrUsernameTaken      = errors.New("username taken")
	ErrInvalidCredentials = errors.New("invalid credentials")
	ErrSessionNotFound    = errors.New("session not found")
	ErrSessionInvalid     = errors.New("session invalid")
	ErrSessionExpired     = errors.New("session expired")
	ErrPlayerNotFound     = errors.New("player not found")
	ErrInvalidInput       = errors.New("invalid input")
)
