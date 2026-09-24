package commands

import (
	"context"

	"github.com/superman/pushkin/internal/application"
	"github.com/superman/pushkin/internal/application/ports"
	"github.com/superman/pushkin/internal/domain"
)

var _ application.UserCommands = (*UserCommands)(nil)

type UserCommandsParams struct{ UserRepository ports.UserRepository }
type UserCommands struct{ userRepository ports.UserRepository }

func NewUserCommands(params UserCommandsParams) *UserCommands {
	return &UserCommands{userRepository: params.UserRepository}
}

func (c *UserCommands) UpsertUser(ctx context.Context, command application.UpsertUserCommand) error {
	user, err := domain.NewUser(domain.NewUserParams{TenantID: command.TenantID, UserID: command.UserID, Attributes: command.Attributes})
	if err != nil {
		return err
	}
	return c.userRepository.Save(ctx, user)
}

func (c *UserCommands) DeleteUser(ctx context.Context, command application.DeleteUserCommand) error {
	user, err := domain.NewUser(domain.NewUserParams{TenantID: command.TenantID, UserID: command.UserID})
	if err != nil {
		return err
	}
	user.Delete()
	return c.userRepository.Save(ctx, user)
}
