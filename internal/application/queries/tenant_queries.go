package queries

import (
	"context"

	"github.com/superman/pushkin/internal/application"
	"github.com/superman/pushkin/internal/application/ports"
	"github.com/superman/pushkin/internal/application/queries/readmodels"
)

var _ application.TenantQueries = (*TenantQueries)(nil)

type TenantQueries struct{ tenants ports.TenantReadRepository }

func NewTenantQueries(tenants ports.TenantReadRepository) *TenantQueries {
	return &TenantQueries{tenants: tenants}
}

func (q *TenantQueries) ListTenants(ctx context.Context) ([]readmodels.Tenant, error) {
	return q.tenants.List(ctx)
}
