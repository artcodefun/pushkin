package queries

import (
	"context"

	"github.com/superman/pushkin/internal/application"
	"github.com/superman/pushkin/internal/application/ports"
	"github.com/superman/pushkin/internal/application/queries/readmodels"
	"github.com/superman/pushkin/internal/domain"
)

var _ application.MobileApplicationQueries = (*MobileApplicationQueries)(nil)

type MobileApplicationQueries struct {
	applications ports.MobileApplicationReadRepository
}

func NewMobileApplicationQueries(applications ports.MobileApplicationReadRepository) *MobileApplicationQueries {
	return &MobileApplicationQueries{applications: applications}
}

func (q *MobileApplicationQueries) GetMobileApplication(ctx context.Context, tenantID domain.TenantID, mobileApplicationID domain.MobileApplicationID) (*readmodels.MobileApplication, error) {
	return q.applications.FindByID(ctx, tenantID, mobileApplicationID)
}

func (q *MobileApplicationQueries) ListMobileApplications(ctx context.Context, tenantID domain.TenantID) ([]readmodels.MobileApplication, error) {
	return q.applications.List(ctx, tenantID)
}
