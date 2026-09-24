package queries

import (
	"context"

	"github.com/superman/pushkin/internal/application"
	"github.com/superman/pushkin/internal/application/ports"
	"github.com/superman/pushkin/internal/application/queries/readmodels"
	"github.com/superman/pushkin/internal/domain"
)

var _ application.ChannelQueries = (*ChannelQueries)(nil)

type ChannelQueries struct{ channels ports.ChannelReadRepository }

func NewChannelQueries(channels ports.ChannelReadRepository) *ChannelQueries {
	return &ChannelQueries{channels: channels}
}

func (q *ChannelQueries) GetChannel(ctx context.Context, tenantID domain.TenantID, channelID domain.ChannelID) (*readmodels.Channel, error) {
	return q.channels.FindByID(ctx, tenantID, channelID)
}

func (q *ChannelQueries) ListChannels(ctx context.Context, tenantID domain.TenantID) ([]readmodels.Channel, error) {
	return q.channels.List(ctx, tenantID)
}
