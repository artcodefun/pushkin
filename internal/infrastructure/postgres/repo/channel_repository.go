package repo

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"

	"github.com/superman/pushkin/internal/application/ports"
	"github.com/superman/pushkin/internal/domain"
	"github.com/superman/pushkin/internal/infrastructure/postgres/gen"
	"github.com/superman/pushkin/internal/infrastructure/postgres/repo/mappers"
)

var _ ports.ChannelRepository = (*ChannelRepository)(nil)

type ChannelRepository struct {
	queries *gen.Queries
}

func NewChannelRepository(queries *gen.Queries) *ChannelRepository {
	return &ChannelRepository{queries: queries}
}

func (r *ChannelRepository) FindByKey(
	ctx context.Context,
	tenantID domain.TenantID,
	key string,
) (*domain.Channel, error) {
	row, err := r.queries.GetChannelByTenantIDAndKey(ctx, gen.GetChannelByTenantIDAndKeyParams{
		TenantID:   tenantID,
		ChannelKey: key,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ports.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return mappers.ChannelFromRow(row)
}

func (r *ChannelRepository) ListActiveIDs(ctx context.Context) ([]domain.ChannelID, error) {
	return r.queries.ListActiveChannelIDs(ctx)
}

func (r *ChannelRepository) FindProvisioningForUpdate(ctx context.Context) (*domain.Channel, error) {
	row, err := r.queries.GetProvisioningChannelForUpdate(ctx)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ports.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return mappers.ChannelFromRow(row)
}

func (r *ChannelRepository) Create(ctx context.Context, channel *domain.Channel) error {
	return r.queries.CreateChannel(ctx, gen.CreateChannelParams{
		ID:          channel.ID(),
		TenantID:    channel.TenantID(),
		ProviderID:  channel.ProviderID(),
		ChannelType: string(channel.Type()),
		ChannelKey:  channel.Key(),
		Status:      string(channel.Status()),
	})
}

func (r *ChannelRepository) FindByID(ctx context.Context, id domain.ChannelID) (*domain.Channel, error) {
	row, err := r.queries.GetChannelByID(ctx, id)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ports.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return mappers.ChannelFromRow(row)
}

func (r *ChannelRepository) Update(ctx context.Context, channel *domain.Channel) error {
	return r.queries.UpdateChannel(ctx, gen.UpdateChannelParams{
		ID:          channel.ID(),
		TenantID:    channel.TenantID(),
		ProviderID:  channel.ProviderID(),
		ChannelType: string(channel.Type()),
		ChannelKey:  channel.Key(),
		Status:      string(channel.Status()),
	})
}

func (r *ChannelRepository) Tx(tx ports.Transaction) ports.ChannelRepository {
	return &ChannelRepository{queries: r.queries.WithTx(tx.(pgx.Tx))}
}

func (r *ChannelRepository) LinkMobileApplication(
	ctx context.Context,
	channelID domain.ChannelID,
	applicationID domain.MobileApplicationID,
) error {
	return r.queries.LinkChannelMobileApplication(ctx, gen.LinkChannelMobileApplicationParams{
		ChannelID:           channelID,
		MobileApplicationID: applicationID,
	})
}

func (r *ChannelRepository) UnlinkMobileApplication(
	ctx context.Context,
	channelID domain.ChannelID,
	applicationID domain.MobileApplicationID,
) error {
	return r.queries.UnlinkChannelMobileApplication(ctx, gen.UnlinkChannelMobileApplicationParams{
		ChannelID:           channelID,
		MobileApplicationID: applicationID,
	})
}
