package repo

import (
	"context"
	"fmt"

	"github.com/superman/pushkin/internal/application/ports"
	"github.com/superman/pushkin/internal/domain"
	"github.com/superman/pushkin/internal/infrastructure/postgres/gen"
)

var _ ports.TenantRepository = (*TenantRepository)(nil)

type TenantRepository struct {
	queries *gen.Queries
}

func NewTenantRepository(queries *gen.Queries) *TenantRepository {
	return &TenantRepository{queries: queries}
}

func (r *TenantRepository) Create(ctx context.Context, tenant *domain.Tenant) error {
	if tenant == nil {
		return fmt.Errorf("tenant must not be nil")
	}
	return r.queries.CreateTenant(ctx, gen.CreateTenantParams{
		ID:                 tenant.ID(),
		Name:               tenant.Name(),
		RateLimitPerMinute: int64(tenant.RateLimitPerMinute()),
		Status:             string(tenant.Status()),
	})
}

func (r *TenantRepository) FindByID(ctx context.Context, id domain.TenantID) (*domain.Tenant, error) {
	row, err := r.queries.FindTenantByID(ctx, id)
	if err != nil {
		return nil, err
	}
	return domain.HydrateTenant(domain.HydrateTenantParams{
		ID:                 row.ID,
		Name:               row.Name,
		RateLimitPerMinute: int(row.RateLimitPerMinute),
		Status:             domain.ConfigurationStatus(row.Status),
	})
}
