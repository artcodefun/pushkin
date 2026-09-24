package domain

import (
	"fmt"
)

type Priority string

const (
	PriorityCritical Priority = "critical"
	PriorityHigh     Priority = "high"
	PriorityNormal   Priority = "normal"
)

func ParsePriority(value string) (Priority, error) {
	priority := Priority(value)
	if !priority.IsValid() {
		return "", fmt.Errorf("%w: unsupported priority %q", ErrInvalidArgument, value)
	}

	return priority, nil
}

func (p Priority) IsValid() bool {
	switch p {
	case PriorityCritical, PriorityHigh, PriorityNormal:
		return true
	default:
		return false
	}
}
