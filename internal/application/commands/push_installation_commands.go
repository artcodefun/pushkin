package commands

import (
	"context"

	"github.com/superman/pushkin/internal/application"
	"github.com/superman/pushkin/internal/application/ports"
	"github.com/superman/pushkin/internal/domain"
)

var _ application.PushInstallationCommands = (*PushInstallationCommands)(nil)

type PushInstallationCommandsParams struct {
	MobileApplicationRepository ports.MobileApplicationRepository
	PushInstallationRepository  ports.PushInstallationRepository
}

type PushInstallationCommands struct {
	applications  ports.MobileApplicationRepository
	installations ports.PushInstallationRepository
}

func NewPushInstallationCommands(params PushInstallationCommandsParams) *PushInstallationCommands {
	return &PushInstallationCommands{
		applications:  params.MobileApplicationRepository,
		installations: params.PushInstallationRepository,
	}
}

func (c *PushInstallationCommands) RegisterPushInstallation(
	ctx context.Context,
	command application.RegisterPushInstallationCommand,
) error {
	mobileApplication, err := c.applications.FindByPlatformAndPackageName(
		ctx,
		command.TenantID,
		command.Platform,
		command.PackageName,
	)
	if err != nil {
		return err
	}

	installation, err := domain.NewPushInstallation(domain.NewPushInstallationParams{
		TenantID:          command.TenantID,
		UserID:            command.UserID,
		MobileApplication: mobileApplication,
		InstallationID:    command.InstallationID,
		Token:             command.Token,
	})
	if err != nil {
		return err
	}

	return c.installations.Upsert(ctx, installation)
}
