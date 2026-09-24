package commands

import (
	"context"
	"fmt"

	"github.com/superman/pushkin/internal/application"
	"github.com/superman/pushkin/internal/application/ports"
	"github.com/superman/pushkin/internal/domain"
)

var _ application.ChannelCommands = (*ChannelCommands)(nil)

type ChannelCommandsParams struct {
	ProviderRepository          ports.ProviderRepository
	MobileApplicationRepository ports.MobileApplicationRepository
	ChannelRepository           ports.ChannelRepository
}

type ChannelCommands struct {
	providers    ports.ProviderRepository
	applications ports.MobileApplicationRepository
	channels     ports.ChannelRepository
}

func NewChannelCommands(params ChannelCommandsParams) *ChannelCommands {
	return &ChannelCommands{
		providers:    params.ProviderRepository,
		applications: params.MobileApplicationRepository,
		channels:     params.ChannelRepository,
	}
}

func (c *ChannelCommands) CreateChannel(
	ctx context.Context,
	command application.CreateChannelCommand,
) (application.CreateChannelResult, error) {
	provider, err := c.providers.FindByID(ctx, command.ProviderID)
	if err != nil {
		return application.CreateChannelResult{}, err
	}

	channel, err := domain.NewChannel(domain.NewChannelParams{
		TenantID: command.TenantID,
		Provider: provider,
		Type:     command.Type,
		Key:      command.Key,
	})
	if err != nil {
		return application.CreateChannelResult{}, err
	}

	if err := c.channels.Create(ctx, channel); err != nil {
		return application.CreateChannelResult{}, err
	}

	return application.CreateChannelResult{ChannelID: channel.ID()}, nil
}

func (c *ChannelCommands) LinkMobileApplicationToChannel(
	ctx context.Context,
	command application.LinkMobileApplicationToChannelCommand,
) error {
	channel, mobileApplication, err := c.findLinkEntities(ctx, command)
	if err != nil {
		return err
	}

	if err := domain.NewChannelMobileApplicationService().ValidateLink(channel, mobileApplication); err != nil {
		return err
	}

	return c.channels.LinkMobileApplication(ctx, channel.ID(), mobileApplication.ID())
}

func (c *ChannelCommands) UnlinkMobileApplicationFromChannel(
	ctx context.Context,
	command application.LinkMobileApplicationToChannelCommand,
) error {
	channel, mobileApplication, err := c.findLinkEntities(ctx, command)
	if err != nil {
		return err
	}

	return c.channels.UnlinkMobileApplication(ctx, channel.ID(), mobileApplication.ID())
}

func (c *ChannelCommands) findLinkEntities(
	ctx context.Context,
	command application.LinkMobileApplicationToChannelCommand,
) (*domain.Channel, *domain.MobileApplication, error) {
	channel, err := c.channels.FindByID(ctx, command.ChannelID)
	if err != nil {
		return nil, nil, err
	}
	if channel.TenantID() != command.TenantID {
		return nil, nil, fmt.Errorf("%w: channel belongs to another tenant", application.ErrNotAuthorized)
	}

	mobileApplication, err := c.applications.FindByID(ctx, command.MobileApplicationID)
	if err != nil {
		return nil, nil, err
	}
	if mobileApplication.TenantID() != command.TenantID {
		return nil, nil, fmt.Errorf("%w: mobile application belongs to another tenant", application.ErrNotAuthorized)
	}

	return channel, mobileApplication, nil
}
