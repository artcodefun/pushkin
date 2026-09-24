package domain

import "errors"

var (
	ErrInvalidArgument   = errors.New("invalid argument")
	ErrInvalidTransition = errors.New("invalid state transition")
	ErrRunMismatch       = errors.New("campaign run mismatch")
)
