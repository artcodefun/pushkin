package ports

import (
	"context"

	"github.com/superman/pushkin/internal/application/queries/readmodels"
	"github.com/superman/pushkin/internal/domain"
)

// Read repositories retrieve immutable projections for query use cases. Their
// infrastructure implementations map database rows directly to these models
// and never need to rehydrate domain aggregates.
type CampaignReadRepository interface {
	FindByID(ctx context.Context, tenantID domain.TenantID, id domain.CampaignID) (*readmodels.Campaign, error)
}

type TenantReadRepository interface {
	List(ctx context.Context) ([]readmodels.Tenant, error)
}

type ChannelReadRepository interface {
	FindByID(ctx context.Context, tenantID domain.TenantID, id domain.ChannelID) (*readmodels.Channel, error)
	List(ctx context.Context, tenantID domain.TenantID) ([]readmodels.Channel, error)
}

type ProviderReadRepository interface {
	FindByID(ctx context.Context, tenantID domain.TenantID, id domain.ProviderID) (*readmodels.Provider, error)
	List(ctx context.Context, tenantID domain.TenantID) ([]readmodels.Provider, error)
}

type MobileApplicationReadRepository interface {
	FindByID(ctx context.Context, tenantID domain.TenantID, id domain.MobileApplicationID) (*readmodels.MobileApplication, error)
	List(ctx context.Context, tenantID domain.TenantID) ([]readmodels.MobileApplication, error)
}
