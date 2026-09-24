package readrepo

import (
	"context"

	"github.com/superman/pushkin/internal/application/ports"
	"github.com/superman/pushkin/internal/application/queries/readmodels"
	"github.com/superman/pushkin/internal/infrastructure/postgres/gen"
	"github.com/superman/pushkin/internal/infrastructure/postgres/readrepo/mappers"
)

var _ ports.TenantReadRepository = (*TenantRepository)(nil)

type TenantRepository struct {
	queries *gen.Queries
}

func NewTenantRepository(queries *gen.Queries) *TenantRepository {
	return &TenantRepository{queries: queries}
}

func (r *TenantRepository) List(ctx context.Context) ([]readmodels.Tenant, error) {
	rows, err := r.queries.ListTenantReadModels(ctx)
	if err != nil {
		return nil, err
	}
	tenants := make([]readmodels.Tenant, len(rows))
	for index, row := range rows {
		tenants[index] = mappers.TenantFromListRow(row)
	}
	return tenants, nil
}
