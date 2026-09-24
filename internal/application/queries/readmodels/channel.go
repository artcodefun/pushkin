package readmodels

import (
	"uuid"

	"github.com/superman/pushkin/internal/domain"
)

type Channel struct {
	ID         uuid.UUID
	TenantID   uuid.UUID
	ProviderID uuid.UUID
	Type       domain.ChannelType
	Key        string
	Status     domain.ChannelStatus
}
