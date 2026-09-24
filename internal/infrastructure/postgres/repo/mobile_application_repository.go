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

var _ ports.MobileApplicationRepository = (*MobileApplicationRepository)(nil)

type MobileApplicationRepository struct {
	queries *gen.Queries
}

func NewMobileApplicationRepository(queries *gen.Queries) *MobileApplicationRepository {
	return &MobileApplicationRepository{queries: queries}
}

func (r *MobileApplicationRepository) ListIDsByChannel(
	ctx context.Context,
	channelID domain.ChannelID,
) ([]domain.MobileApplicationID, error) {
	return r.queries.ListMobileApplicationIDsByChannel(ctx, channelID)
}

func (r *MobileApplicationRepository) ListIDsByChannels(
	ctx context.Context,
	channelIDs []domain.ChannelID,
) (map[domain.ChannelID][]domain.MobileApplicationID, error) {
	rows, err := r.queries.ListMobileApplicationIDsByChannels(ctx, channelIDs)
	if err != nil {
		return nil, err
	}
	result := make(map[domain.ChannelID][]domain.MobileApplicationID)
	for _, row := range rows {
		result[row.ChannelID] = append(result[row.ChannelID], row.MobileApplicationID)
	}
	return result, nil
}

func (r *MobileApplicationRepository) Create(
	ctx context.Context,
	application *domain.MobileApplication,
) error {
	return r.queries.CreateMobileApplication(ctx, gen.CreateMobileApplicationParams{
		ID:          application.ID(),
		TenantID:    application.TenantID(),
		ProviderID:  application.ProviderID(),
		Platform:    string(application.Platform()),
		PackageName: application.PackageName(),
		Status:      string(application.Status()),
	})
}

func (r *MobileApplicationRepository) FindByID(
	ctx context.Context,
	id domain.MobileApplicationID,
) (*domain.MobileApplication, error) {
	row, err := r.queries.GetMobileApplicationByID(ctx, id)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ports.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return mappers.MobileApplicationFromRow(row)
}

func (r *MobileApplicationRepository) FindByPlatformAndPackageName(
	ctx context.Context,
	tenantID domain.TenantID,
	platform domain.MobilePlatform,
	packageName string,
) (*domain.MobileApplication, error) {
	row, err := r.queries.GetMobileApplicationByPlatformAndPackageName(
		ctx,
		gen.GetMobileApplicationByPlatformAndPackageNameParams{
			TenantID:    tenantID,
			Platform:    string(platform),
			PackageName: packageName,
		},
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ports.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return mappers.MobileApplicationFromRow(row)
}

func (r *MobileApplicationRepository) Update(
	ctx context.Context,
	application *domain.MobileApplication,
) error {
	return r.queries.UpdateMobileApplication(ctx, gen.UpdateMobileApplicationParams{
		ID:         application.ID(),
		ProviderID: application.ProviderID(),
		Status:     string(application.Status()),
	})
}

func (r *MobileApplicationRepository) ListChannelIDs(
	ctx context.Context,
	applicationID domain.MobileApplicationID,
) ([]domain.ChannelID, error) {
	return r.queries.ListChannelIDsByMobileApplication(ctx, applicationID)
}
