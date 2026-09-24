package queries

import (
	"context"
	"testing"

	"uuid"

	"github.com/superman/pushkin/internal/application/queries/readmodels"
)

func TestTenantQueriesListsReadModels(t *testing.T) {
	t.Parallel()

	tenant := readmodels.Tenant{ID: uuid.NewV7(), Name: "Acme"}
	repository := &fakeTenantReadRepository{tenants: []readmodels.Tenant{tenant}}
	queries := NewTenantQueries(repository)

	tenants, err := queries.ListTenants(context.Background())
	if err != nil {
		t.Fatalf("list tenants: %v", err)
	}
	if len(tenants) != 1 || tenants[0] != tenant {
		t.Fatalf("unexpected tenants: %#v", tenants)
	}
}

type fakeTenantReadRepository struct{ tenants []readmodels.Tenant }

func (r *fakeTenantReadRepository) List(context.Context) ([]readmodels.Tenant, error) {
	return r.tenants, nil
}
