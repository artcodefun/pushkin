package readrepo

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"

	"github.com/superman/pushkin/internal/application/ports"
	"github.com/superman/pushkin/internal/application/queries/readmodels"
	"github.com/superman/pushkin/internal/domain"
	"github.com/superman/pushkin/internal/infrastructure/postgres/gen"
	"github.com/superman/pushkin/internal/infrastructure/postgres/readrepo/mappers"
)

var _ ports.ProviderReadRepository = (*ProviderRepository)(nil)

type ProviderRepository struct {
	queries *gen.Queries
}

func NewProviderRepository(queries *gen.Queries) *ProviderRepository {
	return &ProviderRepository{queries: queries}
}

func (r *ProviderRepository) FindByID(
	ctx context.Context,
	tenantID domain.TenantID,
	id domain.ProviderID,
) (*readmodels.Provider, error) {
	row, err := r.queries.GetProviderReadModelByID(ctx, gen.GetProviderReadModelByIDParams{
		TenantID: tenantID,
		ID:       id,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ports.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return mappers.ProviderFromGetRow(row), nil
}

func (r *ProviderRepository) List(
	ctx context.Context,
	tenantID domain.TenantID,
) ([]readmodels.Provider, error) {
	rows, err := r.queries.ListProviderReadModels(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	providers := make([]readmodels.Provider, len(rows))
	for index, row := range rows {
		providers[index] = mappers.ProviderFromListRow(row)
	}
	return providers, nil
}
