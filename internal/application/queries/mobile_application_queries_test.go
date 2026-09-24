package queries

import (
	"context"
	"testing"

	"uuid"

	"github.com/superman/pushkin/internal/application/queries/readmodels"
	"github.com/superman/pushkin/internal/domain"
)

func TestMobileApplicationQueriesAreTenantScoped(t *testing.T) {
	t.Parallel()

	tenantID := uuid.NewV7()
	applicationID := uuid.NewV7()
	repository := &fakeMobileApplicationReadRepository{application: &readmodels.MobileApplication{ID: applicationID, TenantID: tenantID}}
	queries := NewMobileApplicationQueries(repository)

	if _, err := queries.GetMobileApplication(context.Background(), tenantID, applicationID); err != nil {
		t.Fatalf("get mobile application: %v", err)
	}
	if _, err := queries.ListMobileApplications(context.Background(), tenantID); err != nil {
		t.Fatalf("list mobile applications: %v", err)
	}
	if repository.tenantID != tenantID || repository.id != applicationID {
		t.Fatalf("unexpected lookup: tenant=%s application=%s", repository.tenantID, repository.id)
	}
}

type fakeMobileApplicationReadRepository struct {
	tenantID    domain.TenantID
	id          domain.MobileApplicationID
	application *readmodels.MobileApplication
}

func (r *fakeMobileApplicationReadRepository) FindByID(_ context.Context, tenantID domain.TenantID, id domain.MobileApplicationID) (*readmodels.MobileApplication, error) {
	r.tenantID = tenantID
	r.id = id
	return r.application, nil
}

func (r *fakeMobileApplicationReadRepository) List(_ context.Context, tenantID domain.TenantID) ([]readmodels.MobileApplication, error) {
	r.tenantID = tenantID
	return []readmodels.MobileApplication{*r.application}, nil
}
