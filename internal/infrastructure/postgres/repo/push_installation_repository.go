package repo

import (
	"context"

	"github.com/superman/pushkin/internal/application/ports"
	"github.com/superman/pushkin/internal/domain"
	"github.com/superman/pushkin/internal/infrastructure/postgres/gen"
)

var _ ports.PushInstallationRepository = (*PushInstallationRepository)(nil)

type PushInstallationRepository struct {
	queries *gen.Queries
}

func NewPushInstallationRepository(queries *gen.Queries) *PushInstallationRepository {
	return &PushInstallationRepository{queries: queries}
}

func (r *PushInstallationRepository) Upsert(
	ctx context.Context,
	installation *domain.PushInstallation,
) error {
	return r.queries.UpsertPushInstallation(ctx, gen.UpsertPushInstallationParams{
		ID:                  installation.ID(),
		TenantID:            installation.TenantID(),
		UserID:              string(installation.UserID()),
		MobileApplicationID: installation.MobileApplicationID(),
		InstallationID:      installation.InstallationID(),
		Token:               installation.Token(),
		Status:              string(installation.Status()),
	})
}

func (r *PushInstallationRepository) Deactivate(
	ctx context.Context,
	tenantID domain.TenantID,
	id domain.PushInstallationID,
) error {
	return r.queries.DeactivatePushInstallation(ctx, gen.DeactivatePushInstallationParams{
		TenantID: tenantID,
		ID:       id,
	})
}

func (r *PushInstallationRepository) ListActiveTokensByIDs(
	ctx context.Context,
	tenantID domain.TenantID,
	ids []domain.PushInstallationID,
) (map[domain.PushInstallationID]string, error) {
	rows, err := r.queries.ListActivePushInstallationTokensByIDs(
		ctx,
		gen.ListActivePushInstallationTokensByIDsParams{
			TenantID: tenantID,
			Ids:      ids,
		},
	)
	if err != nil {
		return nil, err
	}

	tokens := make(map[domain.PushInstallationID]string, len(rows))
	for _, row := range rows {
		tokens[row.ID] = row.Token
	}
	return tokens, nil
}

func (r *PushInstallationRepository) ListActiveIDs(
	ctx context.Context,
	tenantID domain.TenantID,
	userIDs []domain.UserID,
	mobileApplicationIDs []domain.MobileApplicationID,
) ([]domain.PushInstallationID, error) {
	return r.queries.ListActivePushInstallationIDs(ctx, gen.ListActivePushInstallationIDsParams{
		TenantID:             tenantID,
		UserIds:              userIDStrings(userIDs),
		MobileApplicationIds: mobileApplicationIDs,
	})
}

func (r *PushInstallationRepository) ListActiveIDsByUsers(
	ctx context.Context,
	tenantID domain.TenantID,
	userIDs []domain.UserID,
	mobileApplicationIDs []domain.MobileApplicationID,
) (map[domain.UserID][]domain.PushInstallationID, error) {
	rows, err := r.queries.ListActivePushInstallationIDsByUsers(ctx, gen.ListActivePushInstallationIDsByUsersParams{
		TenantID:             tenantID,
		UserIds:              userIDStrings(userIDs),
		MobileApplicationIds: mobileApplicationIDs,
	})
	if err != nil {
		return nil, err
	}
	result := make(map[domain.UserID][]domain.PushInstallationID, len(rows))
	for _, row := range rows {
		userID := domain.UserID(row.UserID)
		result[userID] = append(result[userID], row.ID)
	}
	return result, nil
}

func userIDStrings(userIDs []domain.UserID) []string {
	values := make([]string, len(userIDs))
	for index, userID := range userIDs {
		values[index] = string(userID)
	}
	return values
}
