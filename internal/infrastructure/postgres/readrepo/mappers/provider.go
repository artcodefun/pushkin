package mappers

import (
	"github.com/superman/pushkin/internal/application/queries/readmodels"
	"github.com/superman/pushkin/internal/domain"
	"github.com/superman/pushkin/internal/infrastructure/postgres/gen"
)

func ProviderFromGetRow(row gen.GetProviderReadModelByIDRow) *readmodels.Provider {
	return &readmodels.Provider{
		ID:             row.ID,
		TenantID:       row.TenantID,
		Type:           domain.ProviderType(row.ProviderType),
		Status:         domain.ConfigurationStatus(row.Status),
		RateLimitQPS:   int(row.RateLimitQps),
		RateLimitBurst: int(row.RateLimitBurst),
	}
}

func ProviderFromListRow(row gen.ListProviderReadModelsRow) readmodels.Provider {
	return readmodels.Provider{
		ID:             row.ID,
		TenantID:       row.TenantID,
		Type:           domain.ProviderType(row.ProviderType),
		Status:         domain.ConfigurationStatus(row.Status),
		RateLimitQPS:   int(row.RateLimitQps),
		RateLimitBurst: int(row.RateLimitBurst),
	}
}
