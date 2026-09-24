package mappers

import (
	"github.com/superman/pushkin/internal/application/queries/readmodels"
	"github.com/superman/pushkin/internal/domain"
	"github.com/superman/pushkin/internal/infrastructure/postgres/gen"
)

func MobileApplicationFromGetRow(row gen.GetMobileApplicationReadModelByIDRow) *readmodels.MobileApplication {
	return &readmodels.MobileApplication{
		ID:          row.ID,
		TenantID:    row.TenantID,
		ProviderID:  row.ProviderID,
		Platform:    domain.MobilePlatform(row.Platform),
		PackageName: row.PackageName,
		Status:      domain.ConfigurationStatus(row.Status),
	}
}

func MobileApplicationFromListRow(row gen.ListMobileApplicationReadModelsRow) readmodels.MobileApplication {
	return readmodels.MobileApplication{
		ID:          row.ID,
		TenantID:    row.TenantID,
		ProviderID:  row.ProviderID,
		Platform:    domain.MobilePlatform(row.Platform),
		PackageName: row.PackageName,
		Status:      domain.ConfigurationStatus(row.Status),
	}
}
