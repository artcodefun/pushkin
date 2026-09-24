package readmodels

import (
	"uuid"

	"github.com/superman/pushkin/internal/domain"
)

type Provider struct {
	ID             uuid.UUID
	TenantID       uuid.UUID
	Type           domain.ProviderType
	Status         domain.ConfigurationStatus
	RateLimitQPS   int
	RateLimitBurst int
}
