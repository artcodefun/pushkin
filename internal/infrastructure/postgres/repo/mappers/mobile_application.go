package mappers

import (
	"github.com/superman/pushkin/internal/domain"
	"github.com/superman/pushkin/internal/infrastructure/postgres/gen"
)

func MobileApplicationFromRow(row gen.MobileApplication) (*domain.MobileApplication, error) {
	return domain.HydrateMobileApplication(domain.HydrateMobileApplicationParams{
		ID:          row.ID,
		TenantID:    row.TenantID,
		ProviderID:  row.ProviderID,
		Platform:    domain.MobilePlatform(row.Platform),
		PackageName: row.PackageName,
		Status:      domain.ConfigurationStatus(row.Status),
	})
}
