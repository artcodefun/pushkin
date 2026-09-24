package readmodels

import (
	"uuid"

	"github.com/superman/pushkin/internal/domain"
)

type Tenant struct {
	ID                 uuid.UUID
	Name               string
	RateLimitPerMinute int
	Status             domain.ConfigurationStatus
}
