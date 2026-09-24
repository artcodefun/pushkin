package mappers

import (
	"github.com/superman/pushkin/internal/application/queries/readmodels"
	"github.com/superman/pushkin/internal/domain"
	"github.com/superman/pushkin/internal/infrastructure/postgres/gen"
)

func TenantFromListRow(row gen.ListTenantReadModelsRow) readmodels.Tenant {
	return readmodels.Tenant{
		ID:                 row.ID,
		Name:               row.Name,
		RateLimitPerMinute: int(row.RateLimitPerMinute),
		Status:             domain.ConfigurationStatus(row.Status),
	}
}
