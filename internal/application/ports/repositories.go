package ports

import (
	"context"
	"errors"
	"time"

	"github.com/superman/pushkin/internal/domain"
)

var ErrNotFound = errors.New("repository resource not found")

type ChannelRepository interface {
	FindByKey(ctx context.Context, tenantID domain.TenantID, key string) (*domain.Channel, error)
	ListActiveIDs(ctx context.Context) ([]domain.ChannelID, error)
	FindProvisioningForUpdate(ctx context.Context) (*domain.Channel, error)
	Create(ctx context.Context, channel *domain.Channel) error
	FindByID(ctx context.Context, id domain.ChannelID) (*domain.Channel, error)
	Update(ctx context.Context, channel *domain.Channel) error
	LinkMobileApplication(ctx context.Context, channelID domain.ChannelID, applicationID domain.MobileApplicationID) error
	UnlinkMobileApplication(ctx context.Context, channelID domain.ChannelID, applicationID domain.MobileApplicationID) error
	Tx(tx Transaction) ChannelRepository
}

type CampaignRepository interface {
	Create(ctx context.Context, campaign *domain.Campaign) error
	FindByID(ctx context.Context, id domain.CampaignID) (*domain.Campaign, error)
	FindByIDs(ctx context.Context, ids []domain.CampaignID) (map[domain.CampaignID]*domain.Campaign, error)
	FindByIDsForUpdate(ctx context.Context, ids []domain.CampaignID) (map[domain.CampaignID]*domain.Campaign, error)
	FindStartCandidatesForUpdate(ctx context.Context, dueBefore time.Time, runAttemptedBefore time.Time, limit int) ([]*domain.Campaign, error)
	FindByIDForUpdate(ctx context.Context, id domain.CampaignID) (*domain.Campaign, error)
	Update(ctx context.Context, campaign *domain.Campaign) error
	UpdateBatch(ctx context.Context, campaigns []*domain.Campaign) error
	Tx(tx Transaction) CampaignRepository
}

type SourceBatchRepository interface {
	Create(ctx context.Context, batch *domain.SourceBatch) error
	FindByID(ctx context.Context, id domain.SourceBatchID) (*domain.SourceBatch, error)
	ListIDsByCampaign(ctx context.Context, campaignID domain.CampaignID) ([]domain.SourceBatchID, error)
	Tx(tx Transaction) SourceBatchRepository
}

type MobileApplicationRepository interface {
	ListIDsByChannel(ctx context.Context, channelID domain.ChannelID) ([]domain.MobileApplicationID, error)
	ListIDsByChannels(ctx context.Context, channelIDs []domain.ChannelID) (map[domain.ChannelID][]domain.MobileApplicationID, error)
	Create(ctx context.Context, application *domain.MobileApplication) error
	FindByID(ctx context.Context, id domain.MobileApplicationID) (*domain.MobileApplication, error)
	FindByPlatformAndPackageName(
		ctx context.Context,
		tenantID domain.TenantID,
		platform domain.MobilePlatform,
		packageName string,
	) (*domain.MobileApplication, error)
	Update(ctx context.Context, application *domain.MobileApplication) error
	ListChannelIDs(ctx context.Context, applicationID domain.MobileApplicationID) ([]domain.ChannelID, error)
}

type PushInstallationRepository interface {
	Upsert(ctx context.Context, installation *domain.PushInstallation) error
	Deactivate(ctx context.Context, tenantID domain.TenantID, id domain.PushInstallationID) error
	ListActiveTokensByIDs(
		ctx context.Context,
		tenantID domain.TenantID,
		ids []domain.PushInstallationID,
	) (map[domain.PushInstallationID]string, error)
	ListActiveIDs(
		ctx context.Context,
		tenantID domain.TenantID,
		userIDs []domain.UserID,
		mobileApplicationIDs []domain.MobileApplicationID,
	) ([]domain.PushInstallationID, error)
	ListActiveIDsByUsers(
		ctx context.Context,
		tenantID domain.TenantID,
		userIDs []domain.UserID,
		mobileApplicationIDs []domain.MobileApplicationID,
	) (map[domain.UserID][]domain.PushInstallationID, error)
}

type UserRepository interface {
	Save(ctx context.Context, user *domain.User) error
}

type TenantRepository interface {
	Create(ctx context.Context, tenant *domain.Tenant) error
	FindByID(ctx context.Context, id domain.TenantID) (*domain.Tenant, error)
}

type TenantAPIKeyRepository interface {
	Create(ctx context.Context, key *domain.TenantAPIKey) error
	FindActiveByID(ctx context.Context, id domain.TenantAPIKeyID) (*domain.TenantAPIKey, error)
	Revoke(ctx context.Context, id domain.TenantAPIKeyID) error
}
type ProviderRepository interface {
	Create(ctx context.Context, provider *domain.Provider) error
	FindByID(ctx context.Context, id domain.ProviderID) (*domain.Provider, error)
}
