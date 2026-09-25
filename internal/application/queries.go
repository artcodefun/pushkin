package application

import (
	"context"

	"github.com/superman/pushkin/internal/application/queries/readmodels"
	"github.com/superman/pushkin/internal/domain"
)

type CampaignQueries interface {
	GetCampaign(ctx context.Context, tenantID domain.TenantID, campaignID domain.CampaignID) (*readmodels.Campaign, error)
}

type TenantQueries interface {
	ListTenants(ctx context.Context) ([]readmodels.Tenant, error)
}

type ChannelQueries interface {
	GetChannel(ctx context.Context, tenantID domain.TenantID, channelID domain.ChannelID) (*readmodels.Channel, error)
	ListChannels(ctx context.Context, tenantID domain.TenantID) ([]readmodels.Channel, error)
}

type ProviderQueries interface {
	GetProvider(ctx context.Context, tenantID domain.TenantID, providerID domain.ProviderID) (*readmodels.Provider, error)
	ListProviders(ctx context.Context, tenantID domain.TenantID) ([]readmodels.Provider, error)
}

type MobileApplicationQueries interface {
	GetMobileApplication(ctx context.Context, tenantID domain.TenantID, mobileApplicationID domain.MobileApplicationID) (*readmodels.MobileApplication, error)
	ListMobileApplications(ctx context.Context, tenantID domain.TenantID) ([]readmodels.MobileApplication, error)
}

type NotificationQueries interface {
	ListNotifications(ctx context.Context, tenantID domain.TenantID, userID domain.UserID, cursor string, limit int) (readmodels.NotificationPage, error)
}
