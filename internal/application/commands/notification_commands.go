package commands

import (
	"context"
	"fmt"
	"time"

	"github.com/superman/pushkin/internal/application"
	"github.com/superman/pushkin/internal/application/ports"
	"github.com/superman/pushkin/internal/domain"
)

var _ application.NotificationCommands = (*NotificationCommands)(nil)

type NotificationCommands struct{ notifications ports.NotificationRepository }

func NewNotificationCommands(notifications ports.NotificationRepository) *NotificationCommands {
	return &NotificationCommands{notifications: notifications}
}

func (c *NotificationCommands) MarkNotificationRead(ctx context.Context, command application.MarkNotificationReadCommand) error {
	if string(command.UserID) == "" || command.NotificationID == (domain.NotificationID{}) {
		return fmt.Errorf("mark notification read: %w", application.ErrValidation)
	}
	return c.notifications.MarkRead(ctx, command.TenantID, command.UserID, command.NotificationID, time.Now().UTC())
}
