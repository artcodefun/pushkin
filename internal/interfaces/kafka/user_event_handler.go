package kafka

import (
	"bytes"
	"context"
	"fmt"
	"uuid"

	contract "github.com/superman/pushkin/api/kafka/v1"
	"github.com/superman/pushkin/internal/application"
	"github.com/superman/pushkin/internal/domain"
)

// UserEventHandler translates one external user-service record into a finite
// UserCommands call. Kafka polling and offset management remain in bootstrap.
type UserEventHandler struct {
	commands application.UserCommands
}

func NewUserEventHandler(commands application.UserCommands) *UserEventHandler {
	return &UserEventHandler{commands: commands}
}

func (h *UserEventHandler) Handle(ctx context.Context, key []byte, event contract.UserEventV1) error {
	if err := event.Validate(); err != nil {
		return fmt.Errorf("validate user event: %w", err)
	}
	if !bytes.Equal(key, contract.PartitionKey(event.TenantID, event.UserID)) {
		return fmt.Errorf("user event partition key does not match tenant_id and user_id")
	}
	tenantID, err := uuid.Parse(event.TenantID)
	if err != nil {
		return fmt.Errorf("parse user event tenant_id: %w", err)
	}

	switch event.Type {
	case contract.UserEventTypeCreated, contract.UserEventTypeUpdated:
		return h.commands.UpsertUser(ctx, application.UpsertUserCommand{
			TenantID: tenantID, UserID: domain.UserID(event.UserID), Attributes: event.Attributes,
		})
	case contract.UserEventTypeDeleted:
		return h.commands.DeleteUser(ctx, application.DeleteUserCommand{
			TenantID: tenantID, UserID: domain.UserID(event.UserID),
		})
	default:
		return fmt.Errorf("unsupported user event type %q", event.Type)
	}
}
