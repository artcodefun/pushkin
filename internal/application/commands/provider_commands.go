package commands

import (
	"context"
	"fmt"

	"github.com/superman/pushkin/internal/application"
	"github.com/superman/pushkin/internal/application/ports"
	"github.com/superman/pushkin/internal/domain"
)

var _ application.ProviderCommands = (*ProviderCommands)(nil)

type ProviderCommandsParams struct {
	ProviderRepository ports.ProviderRepository
	CredentialsCipher  ports.CredentialsCipher
}

type ProviderCommands struct {
	providers         ports.ProviderRepository
	credentialsCipher ports.CredentialsCipher
}

func NewProviderCommands(params ProviderCommandsParams) *ProviderCommands {
	return &ProviderCommands{providers: params.ProviderRepository, credentialsCipher: params.CredentialsCipher}
}

func (c *ProviderCommands) CreateProvider(
	ctx context.Context,
	command application.CreateProviderCommand,
) (application.CreateProviderResult, error) {
	if len(command.Credentials) == 0 {
		return application.CreateProviderResult{}, fmt.Errorf("provider credentials: %w", application.ErrValidation)
	}
	encryptedCredentials, err := c.credentialsCipher.Encrypt(ctx, command.Credentials)
	if err != nil {
		return application.CreateProviderResult{}, fmt.Errorf("%w: encrypt provider credentials: %w", application.ErrUnavailable, err)
	}
	provider, err := domain.NewProvider(domain.NewProviderParams{
		TenantID:             command.TenantID,
		Type:                 command.Type,
		EncryptedCredentials: encryptedCredentials,
		RateLimitQPS:         command.RateLimitQPS,
		RateLimitBurst:       command.RateLimitBurst,
	})
	if err != nil {
		return application.CreateProviderResult{}, err
	}

	if err := c.providers.Create(ctx, provider); err != nil {
		return application.CreateProviderResult{}, err
	}

	return application.CreateProviderResult{ProviderID: provider.ID()}, nil
}
