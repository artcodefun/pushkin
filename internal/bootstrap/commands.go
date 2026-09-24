package bootstrap

import (
	"github.com/superman/pushkin/internal/application"
	"github.com/superman/pushkin/internal/application/commands"
)

// Commands contains the write use cases exposed by primary adapters.
type Commands struct {
	Campaign          application.CampaignCommands
	Channel           application.ChannelCommands
	MobileApplication application.MobileApplicationCommands
	Provider          application.ProviderCommands
	PushInstallation  application.PushInstallationCommands
	Tenant            application.TenantCommands
	TenantAPIKey      application.TenantAPIKeyCommands
	User              application.UserCommands
}

func NewCommands(adapters *Adapters, config Config) *Commands {
	return &Commands{
		Campaign: commands.NewCampaignCommands(commands.CampaignCommandsParams{
			ChannelRepository:       adapters.Channels,
			CampaignRepository:      adapters.Campaigns,
			SourceBatchRepository:   adapters.SourceBatches,
			TransactionManager:      adapters.Transactions,
			KafkaProducer:           adapters.KafkaProducer,
			SourceBatchMaxSize:      config.SourceBatchMaxSize,
			CampaignStartingTimeout: config.CampaignStartingTimeout,
		}),
		Channel: commands.NewChannelCommands(commands.ChannelCommandsParams{
			ProviderRepository:          adapters.Providers,
			MobileApplicationRepository: adapters.MobileApplications,
			ChannelRepository:           adapters.Channels,
		}),
		MobileApplication: commands.NewMobileApplicationCommands(commands.MobileApplicationCommandsParams{
			ProviderRepository:          adapters.Providers,
			MobileApplicationRepository: adapters.MobileApplications,
		}),
		Provider: commands.NewProviderCommands(commands.ProviderCommandsParams{
			ProviderRepository: adapters.Providers,
			CredentialsCipher:  adapters.CredentialsCipher,
		}),
		PushInstallation: commands.NewPushInstallationCommands(commands.PushInstallationCommandsParams{
			MobileApplicationRepository: adapters.MobileApplications,
			PushInstallationRepository:  adapters.PushInstallations,
		}),
		Tenant: commands.NewTenantCommands(commands.TenantCommandsParams{TenantRepository: adapters.Tenants}),
		TenantAPIKey: commands.NewTenantAPIKeyCommands(commands.TenantAPIKeyCommandsParams{
			TenantRepository:       adapters.Tenants,
			TenantAPIKeyRepository: adapters.TenantAPIKeys,
			TenantAPIKeyHasher:     adapters.TenantAPIKeyHasher,
		}),
		User: commands.NewUserCommands(commands.UserCommandsParams{UserRepository: adapters.Users}),
	}
}
