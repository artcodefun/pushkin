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

var _ ports.ProviderRepository = (*ProviderRepository)(nil)

type ProviderRepository struct {
	queries *gen.Queries
}

func NewProviderRepository(queries *gen.Queries) *ProviderRepository {
	return &ProviderRepository{queries: queries}
}

func (r *ProviderRepository) Create(ctx context.Context, provider *domain.Provider) error {
	return r.queries.CreateProvider(ctx, gen.CreateProviderParams{
		ID:                   provider.ID(),
		TenantID:             provider.TenantID(),
		ProviderType:         string(provider.Type()),
		EncryptedCredentials: provider.EncryptedCredentials(),
		RateLimitQps:         int64(provider.RateLimitQPS()),
		RateLimitBurst:       int64(provider.RateLimitBurst()),
		Status:               string(provider.Status()),
	})
}

func (r *ProviderRepository) FindByID(ctx context.Context, id domain.ProviderID) (*domain.Provider, error) {
	row, err := r.queries.GetProviderByID(ctx, id)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ports.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return mappers.ProviderFromRow(row)
}
