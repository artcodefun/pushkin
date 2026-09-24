package repo

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/superman/pushkin/internal/application/ports"
	"github.com/superman/pushkin/internal/domain"
	"github.com/superman/pushkin/internal/infrastructure/postgres/gen"
)

var _ ports.TenantAPIKeyRepository = (*TenantAPIKeyRepository)(nil)

type TenantAPIKeyRepository struct {
	queries *gen.Queries
}

func NewTenantAPIKeyRepository(queries *gen.Queries) *TenantAPIKeyRepository {
	return &TenantAPIKeyRepository{queries: queries}
}

func (r *TenantAPIKeyRepository) Create(ctx context.Context, key *domain.TenantAPIKey) error {
	if key == nil {
		return fmt.Errorf("tenant API key must not be nil")
	}
	return r.queries.CreateTenantAPIKey(ctx, gen.CreateTenantAPIKeyParams{
		ID:         key.ID(),
		TenantID:   key.TenantID(),
		Name:       key.Name(),
		SecretHash: key.SecretHash(),
	})
}

func (r *TenantAPIKeyRepository) FindActiveByID(ctx context.Context, id domain.TenantAPIKeyID) (*domain.TenantAPIKey, error) {
	row, err := r.queries.FindActiveTenantAPIKeyByID(ctx, id)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	return domain.HydrateTenantAPIKey(domain.HydrateTenantAPIKeyParams{
		ID:         row.ID,
		TenantID:   row.TenantID,
		Name:       row.Name,
		SecretHash: row.SecretHash,
	})
}

func (r *TenantAPIKeyRepository) Revoke(ctx context.Context, id domain.TenantAPIKeyID) error {
	updated, err := r.queries.RevokeTenantAPIKey(ctx, id)
	if err != nil {
		return err
	}
	if updated == 0 {
		return ports.ErrNotFound
	}
	return nil
}
