package readmodels

import (
	"time"
	"uuid"

	"github.com/superman/pushkin/internal/domain"
)

type Campaign struct {
	ID                    uuid.UUID
	TenantID              uuid.UUID
	ChannelID             uuid.UUID
	Priority              domain.Priority
	Status                domain.CampaignStatus
	Title                 string
	Body                  string
	ImageURL              string
	Data                  map[string]string
	ScheduledAt           *time.Time
	StartedAt             *time.Time
	DeliveryTotal         uint64
	DeliveryProcessed     uint64
	DeliveryAcceptedCount uint64
	DeliveryFailedCount   uint64
}
