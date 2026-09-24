package mappers

import (
	"github.com/superman/pushkin/internal/domain"
	"github.com/superman/pushkin/internal/infrastructure/postgres/gen"
)

func ChannelFromRow(row gen.Channel) (*domain.Channel, error) {
	return domain.HydrateChannel(domain.HydrateChannelParams{
		ID:         row.ID,
		TenantID:   row.TenantID,
		ProviderID: row.ProviderID,
		Type:       domain.ChannelType(row.ChannelType),
		Key:        row.ChannelKey,
		Status:     domain.ChannelStatus(row.Status),
	})
}
