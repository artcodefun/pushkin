package queries

import (
	"context"
	"testing"

	"uuid"

	"github.com/superman/pushkin/internal/application/queries/readmodels"
	"github.com/superman/pushkin/internal/domain"
)

func TestCampaignQueriesGetCampaign(t *testing.T) {
	t.Parallel()

	tenantID := uuid.NewV7()
	campaignID := uuid.NewV7()
	expected := &readmodels.Campaign{ID: campaignID, TenantID: tenantID, Title: "Balance reminder"}
	repository := &fakeCampaignReadRepository{campaign: expected}

	result, err := NewCampaignQueries(repository).GetCampaign(context.Background(), tenantID, campaignID)
	if err != nil {
		t.Fatalf("get campaign: %v", err)
	}
	if result != expected {
		t.Fatal("query must return the repository read model")
	}
	if repository.tenantID != tenantID || repository.id != campaignID {
		t.Fatalf("unexpected lookup: tenant=%s campaign=%s", repository.tenantID, repository.id)
	}
}

type fakeCampaignReadRepository struct {
	tenantID domain.TenantID
	id       domain.CampaignID
	campaign *readmodels.Campaign
}

func (r *fakeCampaignReadRepository) FindByID(_ context.Context, tenantID domain.TenantID, id domain.CampaignID) (*readmodels.Campaign, error) {
	r.tenantID = tenantID
	r.id = id
	return r.campaign, nil
}
