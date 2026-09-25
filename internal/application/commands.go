package application

import (
	"context"
	"time"

	"github.com/superman/pushkin/internal/domain"
)

// CampaignCommands defines the control-plane mutations available for Campaigns.
type CampaignCommands interface {
	CreateCampaign(ctx context.Context, command CreateCampaignCommand) (CreateCampaignResult, error)
	CreateInlineCampaign(ctx context.Context, command CreateInlineCampaignCommand) (CreateInlineCampaignResult, error)
	AddCampaignRecipients(ctx context.Context, command AddCampaignRecipientsCommand) (AddCampaignRecipientsResult, error)
	StartCampaign(ctx context.Context, command StartCampaignCommand) (StartCampaignResult, error)
}

type CreateCampaignCommand struct {
	TenantID   domain.TenantID
	ChannelKey string
	Title      string
	Body       string
	ImageURL   string
	Data       map[string]string
	Priority   domain.Priority
}

type CreateCampaignResult struct {
	CampaignID domain.CampaignID
}

type CreateInlineCampaignCommand struct {
	TenantID    domain.TenantID
	ChannelKey  string
	Title       string
	Body        string
	ImageURL    string
	Data        map[string]string
	Priority    domain.Priority
	UserIDs     []domain.UserID
	ScheduledAt *time.Time
}

type CreateInlineCampaignResult struct {
	CampaignID domain.CampaignID
	Status     domain.CampaignStatus
}

type AddCampaignRecipientsCommand struct {
	TenantID   domain.TenantID
	CampaignID domain.CampaignID
	UserIDs    []domain.UserID
}

type AddCampaignRecipientsResult struct {
	SourceBatchID domain.SourceBatchID
}

type StartCampaignCommand struct {
	TenantID    domain.TenantID
	CampaignID  domain.CampaignID
	ScheduledAt *time.Time
}

type StartCampaignResult struct {
	CampaignID domain.CampaignID
	Status     domain.CampaignStatus
}

// UserCommands defines mutations for Pushkin's local user representation.
type UserCommands interface {
	UpsertUser(ctx context.Context, command UpsertUserCommand) error
	DeleteUser(ctx context.Context, command DeleteUserCommand) error
}

type UpsertUserCommand struct {
	TenantID   domain.TenantID
	UserID     domain.UserID
	Attributes map[string]string
}

type DeleteUserCommand struct {
	TenantID domain.TenantID
	UserID   domain.UserID
}

// TenantCommands defines platform control-plane mutations for Tenants.
type TenantCommands interface {
	CreateTenant(ctx context.Context, command CreateTenantCommand) (CreateTenantResult, error)
}

type CreateTenantCommand struct {
	Name               string
	RateLimitPerMinute int
}

type CreateTenantResult struct{ TenantID domain.TenantID }

// TenantAPIKeyCommands manages credentials that authenticate tenant-scoped API requests.
type TenantAPIKeyCommands interface {
	IssueTenantAPIKey(ctx context.Context, command IssueTenantAPIKeyCommand) (IssueTenantAPIKeyResult, error)
	RevokeTenantAPIKey(ctx context.Context, command RevokeTenantAPIKeyCommand) error
}

type IssueTenantAPIKeyCommand struct {
	TenantID domain.TenantID
	Name     string
}

type IssueTenantAPIKeyResult struct {
	APIKeyID domain.TenantAPIKeyID
	APIKey   string
}

type RevokeTenantAPIKeyCommand struct {
	APIKeyID domain.TenantAPIKeyID
}

// ProviderCommands defines tenant-scoped provider configuration mutations.
type ProviderCommands interface {
	CreateProvider(ctx context.Context, command CreateProviderCommand) (CreateProviderResult, error)
}

type CreateProviderCommand struct {
	TenantID                     domain.TenantID
	Type                         domain.ProviderType
	Credentials                  []byte
	RateLimitQPS, RateLimitBurst int
}

type CreateProviderResult struct{ ProviderID domain.ProviderID }

// MobileApplicationCommands defines mobile application configuration mutations.
type MobileApplicationCommands interface {
	CreateMobileApplication(ctx context.Context, command CreateMobileApplicationCommand) (CreateMobileApplicationResult, error)
	ConnectMobileApplicationProvider(ctx context.Context, command ConnectMobileApplicationProviderCommand) error
	DisconnectMobileApplicationProvider(ctx context.Context, command DisconnectMobileApplicationProviderCommand) error
}

type CreateMobileApplicationCommand struct {
	TenantID    domain.TenantID
	ProviderID  domain.ProviderID
	Platform    domain.MobilePlatform
	PackageName string
}

type CreateMobileApplicationResult struct{ MobileApplicationID domain.MobileApplicationID }

type ConnectMobileApplicationProviderCommand struct {
	TenantID            domain.TenantID
	MobileApplicationID domain.MobileApplicationID
	ProviderID          domain.ProviderID
}

type DisconnectMobileApplicationProviderCommand struct {
	TenantID            domain.TenantID
	MobileApplicationID domain.MobileApplicationID
}

// PushInstallationCommands defines mutations for mobile-push delivery targets.
type PushInstallationCommands interface {
	RegisterPushInstallation(ctx context.Context, command RegisterPushInstallationCommand) error
}

type RegisterPushInstallationCommand struct {
	TenantID       domain.TenantID
	UserID         domain.UserID
	Platform       domain.MobilePlatform
	PackageName    string
	InstallationID string
	Token          string
}

// NotificationCommands mutates tenant-scoped notification state.
type NotificationCommands interface {
	MarkNotificationRead(ctx context.Context, command MarkNotificationReadCommand) error
}

type MarkNotificationReadCommand struct {
	TenantID       domain.TenantID
	UserID         domain.UserID
	NotificationID domain.NotificationID
}

// ChannelCommands defines channel configuration and application link mutations.
type ChannelCommands interface {
	CreateChannel(ctx context.Context, command CreateChannelCommand) (CreateChannelResult, error)
	LinkMobileApplicationToChannel(ctx context.Context, command LinkMobileApplicationToChannelCommand) error
	UnlinkMobileApplicationFromChannel(ctx context.Context, command LinkMobileApplicationToChannelCommand) error
}

type CreateChannelCommand struct {
	TenantID   domain.TenantID
	ProviderID domain.ProviderID
	Type       domain.ChannelType
	Key        string
}

type CreateChannelResult struct{ ChannelID domain.ChannelID }

type LinkMobileApplicationToChannelCommand struct {
	TenantID            domain.TenantID
	ChannelID           domain.ChannelID
	MobileApplicationID domain.MobileApplicationID
}
