package v1

import (
	"encoding/json"
	"fmt"
	"strings"
)

const (
	UserEventsTopic Topic = "pushkin.user-events.v1"
)

type UserEventType string

const (
	UserEventTypeCreated UserEventType = "user_created"
	UserEventTypeUpdated UserEventType = "user_updated"
	UserEventTypeDeleted UserEventType = "user_deleted"
)

// UserEventV1 is a public record value published by a user service. Producers
// must use PartitionKey for the Kafka record key so that all events for one
// tenant/user pair stay ordered within one partition.
type UserEventV1 struct {
	Type       UserEventType     `json:"type"`
	TenantID   string            `json:"tenant_id"`
	UserID     string            `json:"user_id"`
	Attributes map[string]string `json:"attributes,omitempty"`
}

func (event UserEventV1) Validate() error {
	if event.Type != UserEventTypeCreated && event.Type != UserEventTypeUpdated && event.Type != UserEventTypeDeleted {
		return fmt.Errorf("unsupported user event type %q", event.Type)
	}
	if strings.TrimSpace(event.TenantID) == "" {
		return fmt.Errorf("tenant_id must not be empty")
	}
	if strings.TrimSpace(event.UserID) == "" {
		return fmt.Errorf("user_id must not be empty")
	}
	return nil
}

func (UserEventV1) isKafkaMessageV1() {}

func (UserEventV1) MessageType() MessageTypeV1 { return MessageTypeUserEventV1 }

// PartitionKey returns the canonical JSON key encoding for a tenant/user pair.
// JSON escapes arbitrary opaque identifiers, unlike delimiter-based keys.
func PartitionKey(tenantID, userID string) []byte {
	key, err := json.Marshal([2]string{tenantID, userID})
	if err != nil {
		panic(fmt.Sprintf("encode user event partition key: %v", err))
	}
	return key
}
