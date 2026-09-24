package commands

import (
	"context"

	"github.com/superman/pushkin/internal/application"
	"github.com/superman/pushkin/internal/application/ports"
	"github.com/superman/pushkin/internal/domain"
)

var _ application.TenantCommands = (*TenantCommands)(nil)

type TenantCommandsParams struct {
	TenantRepository ports.TenantRepository
}

type TenantCommands struct {
	tenants ports.TenantRepository
}

func NewTenantCommands(params TenantCommandsParams) *TenantCommands {
	return &TenantCommands{tenants: params.TenantRepository}
}

func (c *TenantCommands) CreateTenant(
	ctx context.Context,
	command application.CreateTenantCommand,
) (application.CreateTenantResult, error) {
	tenant, err := domain.NewTenant(domain.NewTenantParams{
		Name:               command.Name,
		RateLimitPerMinute: command.RateLimitPerMinute,
	})
	if err != nil {
		return application.CreateTenantResult{}, err
	}

	if err := c.tenants.Create(ctx, tenant); err != nil {
		return application.CreateTenantResult{}, err
	}

	return application.CreateTenantResult{TenantID: tenant.ID()}, nil
}
