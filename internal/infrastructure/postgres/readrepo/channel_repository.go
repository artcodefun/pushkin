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

var _ ports.ChannelReadRepository = (*ChannelRepository)(nil)

type ChannelRepository struct {
	queries *gen.Queries
}

func NewChannelRepository(queries *gen.Queries) *ChannelRepository {
	return &ChannelRepository{queries: queries}
}

func (r *ChannelRepository) FindByID(
	ctx context.Context,
	tenantID domain.TenantID,
	id domain.ChannelID,
) (*readmodels.Channel, error) {
	row, err := r.queries.GetChannelReadModelByID(ctx, gen.GetChannelReadModelByIDParams{
		TenantID: tenantID,
		ID:       id,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ports.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return mappers.ChannelFromGetRow(row), nil
}

func (r *ChannelRepository) List(
	ctx context.Context,
	tenantID domain.TenantID,
) ([]readmodels.Channel, error) {
	rows, err := r.queries.ListChannelReadModels(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	channels := make([]readmodels.Channel, len(rows))
	for index, row := range rows {
		channels[index] = mappers.ChannelFromListRow(row)
	}
	return channels, nil
}
