package readmodels

import (
	"uuid"

	"github.com/superman/pushkin/internal/domain"
)

type MobileApplication struct {
	ID          uuid.UUID
	TenantID    uuid.UUID
	ProviderID  *uuid.UUID
	Platform    domain.MobilePlatform
	PackageName string
	Status      domain.ConfigurationStatus
}
