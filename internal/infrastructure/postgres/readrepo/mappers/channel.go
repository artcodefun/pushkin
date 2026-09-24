package mappers

import (
	"github.com/superman/pushkin/internal/application/queries/readmodels"
	"github.com/superman/pushkin/internal/domain"
	"github.com/superman/pushkin/internal/infrastructure/postgres/gen"
)

func ChannelFromGetRow(row gen.GetChannelReadModelByIDRow) *readmodels.Channel {
	return &readmodels.Channel{
		ID:         row.ID,
		TenantID:   row.TenantID,
		ProviderID: row.ProviderID,
		Type:       domain.ChannelType(row.ChannelType),
		Key:        row.ChannelKey,
		Status:     domain.ChannelStatus(row.Status),
	}
}

func ChannelFromListRow(row gen.ListChannelReadModelsRow) readmodels.Channel {
	return readmodels.Channel{
		ID:         row.ID,
		TenantID:   row.TenantID,
		ProviderID: row.ProviderID,
		Type:       domain.ChannelType(row.ChannelType),
		Key:        row.ChannelKey,
		Status:     domain.ChannelStatus(row.Status),
	}
}
