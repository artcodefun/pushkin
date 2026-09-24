package queries

import (
	"context"
	"testing"

	"uuid"

	"github.com/superman/pushkin/internal/application/queries/readmodels"
	"github.com/superman/pushkin/internal/domain"
)

func TestChannelQueriesAreTenantScoped(t *testing.T) {
	t.Parallel()

	tenantID := uuid.NewV7()
	channelID := uuid.NewV7()
	repository := &fakeChannelReadRepository{channel: &readmodels.Channel{ID: channelID, TenantID: tenantID}}
	queries := NewChannelQueries(repository)

	if _, err := queries.GetChannel(context.Background(), tenantID, channelID); err != nil {
		t.Fatalf("get channel: %v", err)
	}
	if _, err := queries.ListChannels(context.Background(), tenantID); err != nil {
		t.Fatalf("list channels: %v", err)
	}
	if repository.tenantID != tenantID || repository.id != channelID {
		t.Fatalf("unexpected lookup: tenant=%s channel=%s", repository.tenantID, repository.id)
	}
}

type fakeChannelReadRepository struct {
	tenantID domain.TenantID
	id       domain.ChannelID
	channel  *readmodels.Channel
}

func (r *fakeChannelReadRepository) FindByID(_ context.Context, tenantID domain.TenantID, id domain.ChannelID) (*readmodels.Channel, error) {
	r.tenantID = tenantID
	r.id = id
	return r.channel, nil
}

func (r *fakeChannelReadRepository) List(_ context.Context, tenantID domain.TenantID) ([]readmodels.Channel, error) {
	r.tenantID = tenantID
	return []readmodels.Channel{*r.channel}, nil
}
