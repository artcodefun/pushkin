package mappers

import (
	"github.com/superman/pushkin/internal/domain"
	"github.com/superman/pushkin/internal/infrastructure/postgres/gen"
)

func ProviderFromRow(row gen.Provider) (*domain.Provider, error) {
	return domain.HydrateProvider(domain.HydrateProviderParams{
		ID:                   row.ID,
		TenantID:             row.TenantID,
		Type:                 domain.ProviderType(row.ProviderType),
		EncryptedCredentials: row.EncryptedCredentials,
		RateLimitQPS:         int(row.RateLimitQps),
		RateLimitBurst:       int(row.RateLimitBurst),
		Status:               domain.ConfigurationStatus(row.Status),
	})
}
