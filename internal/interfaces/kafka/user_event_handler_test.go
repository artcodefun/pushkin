package kafka

import (
	"context"
	"testing"
	"uuid"

	contract "github.com/superman/pushkin/api/kafka/v1"
	"github.com/superman/pushkin/internal/application"
)

func TestUserEventHandlerUpsertsCreatedAndUpdatedUsers(t *testing.T) {
	testCases := []contract.UserEventType{contract.UserEventTypeCreated, contract.UserEventTypeUpdated}
	for _, eventType := range testCases {
		t.Run(string(eventType), func(t *testing.T) {
			commands := &userEventCommands{}
			handler := NewUserEventHandler(commands)
			tenantID := uuid.NewV7()
			event := contract.UserEventV1{Type: eventType, TenantID: tenantID.String(), UserID: "user-1", Attributes: map[string]string{"locale": "ru"}}
			if err := handler.Handle(context.Background(), contract.PartitionKey(event.TenantID, event.UserID), event); err != nil {
				t.Fatalf("handle event: %v", err)
			}
			if commands.upsert.TenantID != tenantID || commands.upsert.UserID != "user-1" || commands.deleted {
				t.Fatalf("unexpected commands state: %+v", commands)
			}
		})
	}
}

func TestUserEventHandlerDeletesUser(t *testing.T) {
	commands := &userEventCommands{}
	handler := NewUserEventHandler(commands)
	tenantID := uuid.NewV7()
	event := contract.UserEventV1{Type: contract.UserEventTypeDeleted, TenantID: tenantID.String(), UserID: "user-1"}
	if err := handler.Handle(context.Background(), contract.PartitionKey(event.TenantID, event.UserID), event); err != nil {
		t.Fatalf("handle event: %v", err)
	}
	if commands.delete.TenantID != tenantID || commands.delete.UserID != "user-1" || commands.upsert.UserID != "" {
		t.Fatalf("unexpected commands state: %+v", commands)
	}
}

func TestUserEventHandlerRejectsInvalidEventOrKey(t *testing.T) {
	commands := &userEventCommands{}
	handler := NewUserEventHandler(commands)
	tenantID := uuid.NewV7()
	event := contract.UserEventV1{Type: contract.UserEventTypeCreated, TenantID: tenantID.String(), UserID: "user-1"}
	if err := handler.Handle(context.Background(), []byte("wrong"), event); err == nil {
		t.Fatal("handle event error = nil")
	}
	event.Type = "unsupported"
	if err := handler.Handle(context.Background(), contract.PartitionKey(event.TenantID, event.UserID), event); err == nil {
		t.Fatal("handle invalid event error = nil")
	}
}

type userEventCommands struct {
	upsert  application.UpsertUserCommand
	delete  application.DeleteUserCommand
	deleted bool
}

func (c *userEventCommands) UpsertUser(_ context.Context, command application.UpsertUserCommand) error {
	c.upsert = command
	return nil
}

func (c *userEventCommands) DeleteUser(_ context.Context, command application.DeleteUserCommand) error {
	c.delete = command
	c.deleted = true
	return nil
}

var _ application.UserCommands = (*userEventCommands)(nil)
