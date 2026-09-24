package queries

import (
	"context"
	"testing"

	"uuid"

	"github.com/superman/pushkin/internal/application/queries/readmodels"
	"github.com/superman/pushkin/internal/domain"
)

func TestProviderQueriesAreTenantScoped(t *testing.T) {
	t.Parallel()

	tenantID := uuid.NewV7()
	providerID := uuid.NewV7()
	repository := &fakeProviderReadRepository{provider: &readmodels.Provider{ID: providerID, TenantID: tenantID}}
	queries := NewProviderQueries(repository)

	if _, err := queries.GetProvider(context.Background(), tenantID, providerID); err != nil {
		t.Fatalf("get provider: %v", err)
	}
	if _, err := queries.ListProviders(context.Background(), tenantID); err != nil {
		t.Fatalf("list providers: %v", err)
	}
	if repository.tenantID != tenantID || repository.id != providerID {
		t.Fatalf("unexpected lookup: tenant=%s provider=%s", repository.tenantID, repository.id)
	}
}

type fakeProviderReadRepository struct {
	tenantID domain.TenantID
	id       domain.ProviderID
	provider *readmodels.Provider
}

func (r *fakeProviderReadRepository) FindByID(_ context.Context, tenantID domain.TenantID, id domain.ProviderID) (*readmodels.Provider, error) {
	r.tenantID = tenantID
	r.id = id
	return r.provider, nil
}

func (r *fakeProviderReadRepository) List(_ context.Context, tenantID domain.TenantID) ([]readmodels.Provider, error) {
	r.tenantID = tenantID
	return []readmodels.Provider{*r.provider}, nil
}
