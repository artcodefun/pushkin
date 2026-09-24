package domain

import (
	"fmt"
	"time"
)

type ConfigurationStatus string

const (
	ConfigurationStatusActive   ConfigurationStatus = "active"
	ConfigurationStatusDisabled ConfigurationStatus = "disabled"
)

func (s ConfigurationStatus) isValid() bool {
	return s == ConfigurationStatusActive || s == ConfigurationStatusDisabled
}

func requireTime(name string, value time.Time) error {
	if value.IsZero() {
		return fmt.Errorf("%w: %s must not be zero", ErrInvalidArgument, name)
	}

	return nil
}
