package domain

import (
	"fmt"
	"strings"
	"uuid"
)

// Named aliases document the role of internal IDs while keeping them directly
// interoperable with database and transport support for uuid.UUID.
type TenantID = uuid.UUID
type TenantAPIKeyID = uuid.UUID
type ChannelID = uuid.UUID
type ProviderID = uuid.UUID
type MobileApplicationID = uuid.UUID
type PushInstallationID = uuid.UUID
type CampaignID = uuid.UUID
type SourceBatchID = uuid.UUID
type DeliveryID = uuid.UUID
type NotificationID = uuid.UUID
type RunID = uuid.UUID

// UserID belongs to the tenant's user service and intentionally remains a
// string: subscriber, CRM and other external identifiers need not be UUIDs.
type UserID string

func requireIdentifier(name string, value string) error {
	if strings.TrimSpace(value) == "" {
		return fmt.Errorf("%w: %s must not be empty", ErrInvalidArgument, name)
	}

	return nil
}

func requireUUID(name string, value uuid.UUID) error {
	if value == uuid.Nil() {
		return fmt.Errorf("%w: %s must not be nil", ErrInvalidArgument, name)
	}

	return nil
}
