package queries

import (
	"context"

	"github.com/superman/pushkin/internal/application"
	"github.com/superman/pushkin/internal/application/ports"
	"github.com/superman/pushkin/internal/application/queries/readmodels"
	"github.com/superman/pushkin/internal/domain"
)

var _ application.CampaignQueries = (*CampaignQueries)(nil)

type CampaignQueries struct{ campaigns ports.CampaignReadRepository }

func NewCampaignQueries(campaigns ports.CampaignReadRepository) *CampaignQueries {
	return &CampaignQueries{campaigns: campaigns}
}

func (q *CampaignQueries) GetCampaign(ctx context.Context, tenantID domain.TenantID, campaignID domain.CampaignID) (*readmodels.Campaign, error) {
	return q.campaigns.FindByID(ctx, tenantID, campaignID)
}
