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

var _ ports.MobileApplicationReadRepository = (*MobileApplicationRepository)(nil)

type MobileApplicationRepository struct {
	queries *gen.Queries
}

func NewMobileApplicationRepository(queries *gen.Queries) *MobileApplicationRepository {
	return &MobileApplicationRepository{queries: queries}
}

func (r *MobileApplicationRepository) FindByID(
	ctx context.Context,
	tenantID domain.TenantID,
	id domain.MobileApplicationID,
) (*readmodels.MobileApplication, error) {
	row, err := r.queries.GetMobileApplicationReadModelByID(ctx, gen.GetMobileApplicationReadModelByIDParams{
		TenantID: tenantID,
		ID:       id,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ports.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return mappers.MobileApplicationFromGetRow(row), nil
}

func (r *MobileApplicationRepository) List(
	ctx context.Context,
	tenantID domain.TenantID,
) ([]readmodels.MobileApplication, error) {
	rows, err := r.queries.ListMobileApplicationReadModels(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	applications := make([]readmodels.MobileApplication, len(rows))
	for index, row := range rows {
		applications[index] = mappers.MobileApplicationFromListRow(row)
	}
	return applications, nil
}
