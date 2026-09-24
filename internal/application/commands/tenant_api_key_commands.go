package commands

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"fmt"

	"github.com/superman/pushkin/internal/application"
	"github.com/superman/pushkin/internal/application/ports"
	"github.com/superman/pushkin/internal/domain"
)

const tenantAPIKeySecretSize = 32

var _ application.TenantAPIKeyCommands = (*TenantAPIKeyCommands)(nil)

type TenantAPIKeyCommandsParams struct {
	TenantRepository       ports.TenantRepository
	TenantAPIKeyRepository ports.TenantAPIKeyRepository
	TenantAPIKeyHasher     ports.TenantAPIKeyHasher
}

type TenantAPIKeyCommands struct {
	tenants ports.TenantRepository
	keys    ports.TenantAPIKeyRepository
	hasher  ports.TenantAPIKeyHasher
}

func NewTenantAPIKeyCommands(params TenantAPIKeyCommandsParams) *TenantAPIKeyCommands {
	return &TenantAPIKeyCommands{
		tenants: params.TenantRepository,
		keys:    params.TenantAPIKeyRepository,
		hasher:  params.TenantAPIKeyHasher,
	}
}

func (c *TenantAPIKeyCommands) IssueTenantAPIKey(
	ctx context.Context,
	command application.IssueTenantAPIKeyCommand,
) (application.IssueTenantAPIKeyResult, error) {
	if c.tenants == nil || c.keys == nil || c.hasher == nil {
		return application.IssueTenantAPIKeyResult{}, fmt.Errorf("tenant API key dependencies: %w", application.ErrUnavailable)
	}
	tenant, err := c.tenants.FindByID(ctx, command.TenantID)
	if err != nil {
		return application.IssueTenantAPIKeyResult{}, err
	}
	if tenant == nil {
		return application.IssueTenantAPIKeyResult{}, application.ErrNotFound
	}
	secret := make([]byte, tenantAPIKeySecretSize)
	if _, err := rand.Read(secret); err != nil {
		return application.IssueTenantAPIKeyResult{}, fmt.Errorf("generate tenant API key secret: %w", err)
	}
	hash, err := c.hasher.Hash(ctx, secret)
	if err != nil {
		return application.IssueTenantAPIKeyResult{}, err
	}
	key, err := domain.NewTenantAPIKey(domain.NewTenantAPIKeyParams{
		TenantID:   command.TenantID,
		Name:       command.Name,
		SecretHash: hash,
	})
	if err != nil {
		return application.IssueTenantAPIKeyResult{}, err
	}
	if err := c.keys.Create(ctx, key); err != nil {
		return application.IssueTenantAPIKeyResult{}, err
	}
	return application.IssueTenantAPIKeyResult{
		APIKeyID: key.ID(),
		APIKey:   "pk_" + key.ID().String() + "_" + base64.RawURLEncoding.EncodeToString(secret),
	}, nil
}

func (c *TenantAPIKeyCommands) RevokeTenantAPIKey(
	ctx context.Context,
	command application.RevokeTenantAPIKeyCommand,
) error {
	if c.keys == nil {
		return fmt.Errorf("tenant API key repository: %w", application.ErrUnavailable)
	}
	if command.APIKeyID == (domain.TenantAPIKeyID{}) {
		return fmt.Errorf("tenant API key ID: %w", application.ErrValidation)
	}
	return c.keys.Revoke(ctx, command.APIKeyID)
}
