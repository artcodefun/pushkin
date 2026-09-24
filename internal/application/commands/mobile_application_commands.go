package commands

import (
	"context"
	"fmt"

	"github.com/superman/pushkin/internal/application"
	"github.com/superman/pushkin/internal/application/ports"
	"github.com/superman/pushkin/internal/domain"
)

var _ application.MobileApplicationCommands = (*MobileApplicationCommands)(nil)

type MobileApplicationCommandsParams struct {
	ProviderRepository          ports.ProviderRepository
	MobileApplicationRepository ports.MobileApplicationRepository
}

type MobileApplicationCommands struct {
	providers    ports.ProviderRepository
	applications ports.MobileApplicationRepository
}

func NewMobileApplicationCommands(params MobileApplicationCommandsParams) *MobileApplicationCommands {
	return &MobileApplicationCommands{
		providers:    params.ProviderRepository,
		applications: params.MobileApplicationRepository,
	}
}

func (c *MobileApplicationCommands) CreateMobileApplication(
	ctx context.Context,
	command application.CreateMobileApplicationCommand,
) (application.CreateMobileApplicationResult, error) {
	provider, err := c.providers.FindByID(ctx, command.ProviderID)
	if err != nil {
		return application.CreateMobileApplicationResult{}, err
	}

	mobileApplication, err := domain.NewMobileApplication(domain.NewMobileApplicationParams{
		TenantID:    command.TenantID,
		Provider:    provider,
		Platform:    command.Platform,
		PackageName: command.PackageName,
	})
	if err != nil {
		return application.CreateMobileApplicationResult{}, err
	}

	if err := c.applications.Create(ctx, mobileApplication); err != nil {
		return application.CreateMobileApplicationResult{}, err
	}

	return application.CreateMobileApplicationResult{MobileApplicationID: mobileApplication.ID()}, nil
}

func (c *MobileApplicationCommands) ConnectMobileApplicationProvider(
	ctx context.Context,
	command application.ConnectMobileApplicationProviderCommand,
) error {
	mobileApplication, err := c.applications.FindByID(ctx, command.MobileApplicationID)
	if err != nil {
		return err
	}
	if mobileApplication.TenantID() != command.TenantID {
		return fmt.Errorf("%w: mobile application belongs to another tenant", application.ErrNotAuthorized)
	}

	provider, err := c.providers.FindByID(ctx, command.ProviderID)
	if err != nil {
		return err
	}
	if provider.TenantID() != command.TenantID {
		return fmt.Errorf("%w: provider belongs to another tenant", application.ErrNotAuthorized)
	}

	if err := mobileApplication.ConnectProvider(provider); err != nil {
		return err
	}

	return c.applications.Update(ctx, mobileApplication)
}

func (c *MobileApplicationCommands) DisconnectMobileApplicationProvider(
	ctx context.Context,
	command application.DisconnectMobileApplicationProviderCommand,
) error {
	mobileApplication, err := c.applications.FindByID(ctx, command.MobileApplicationID)
	if err != nil {
		return err
	}
	if mobileApplication.TenantID() != command.TenantID {
		return fmt.Errorf("%w: mobile application belongs to another tenant", application.ErrNotAuthorized)
	}

	channelIDs, err := c.applications.ListChannelIDs(ctx, mobileApplication.ID())
	if err != nil {
		return err
	}
	if len(channelIDs) > 0 {
		return fmt.Errorf("%w: mobile application is linked to channels", application.ErrConflict)
	}

	if err := mobileApplication.DisconnectProvider(); err != nil {
		return err
	}

	return c.applications.Update(ctx, mobileApplication)
}
