package queries

import (
	"context"

	"github.com/superman/pushkin/internal/application"
	"github.com/superman/pushkin/internal/application/ports"
	"github.com/superman/pushkin/internal/application/queries/readmodels"
	"github.com/superman/pushkin/internal/domain"
)

var _ application.ProviderQueries = (*ProviderQueries)(nil)

type ProviderQueries struct{ providers ports.ProviderReadRepository }

func NewProviderQueries(providers ports.ProviderReadRepository) *ProviderQueries {
	return &ProviderQueries{providers: providers}
}

func (q *ProviderQueries) GetProvider(ctx context.Context, tenantID domain.TenantID, providerID domain.ProviderID) (*readmodels.Provider, error) {
	return q.providers.FindByID(ctx, tenantID, providerID)
}

func (q *ProviderQueries) ListProviders(ctx context.Context, tenantID domain.TenantID) ([]readmodels.Provider, error) {
	return q.providers.List(ctx, tenantID)
}
