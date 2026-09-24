package application

import "errors"

// Application errors describe expected use-case outcomes. They are mapped by
// primary adapters before domain errors because they carry more context.
var (
	ErrNotFound      = errors.New("resource not found")
	ErrNotAuthorized = errors.New("operation is not authorized")
	ErrValidation    = errors.New("request validation failed")
	ErrConflict      = errors.New("operation conflicts with current state")
	ErrUnavailable   = errors.New("required dependency is unavailable")
)
