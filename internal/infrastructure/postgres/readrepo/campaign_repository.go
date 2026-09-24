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

var _ ports.CampaignReadRepository = (*CampaignRepository)(nil)

// CampaignRepository maps PostgreSQL projections directly to application read
// models; it never rehydrates domain aggregates.
type CampaignRepository struct {
	queries *gen.Queries
}

func NewCampaignRepository(queries *gen.Queries) *CampaignRepository {
	return &CampaignRepository{queries: queries}
}

func (r *CampaignRepository) FindByID(
	ctx context.Context,
	tenantID domain.TenantID,
	id domain.CampaignID,
) (*readmodels.Campaign, error) {
	row, err := r.queries.GetCampaignReadModelByID(ctx, gen.GetCampaignReadModelByIDParams{
		TenantID: tenantID,
		ID:       id,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ports.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return mappers.CampaignFromRow(row)
}
