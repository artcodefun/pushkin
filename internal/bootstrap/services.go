package bootstrap

import (
	"github.com/superman/pushkin/internal/application/ports"
	"github.com/superman/pushkin/internal/application/services"
	contracts "github.com/superman/pushkin/internal/contracts/kafka"
	"github.com/superman/pushkin/internal/domain"
)

// DeliveryServiceFactory creates the stateful processor for one delivery
// topic. The channel worker manager supplies its topic-specific consumer.
type DeliveryServiceFactory func(DeliveryServiceFactoryParams) (*services.DeliveryService, error)

type DeliveryServiceFactoryParams struct {
	ChannelID     domain.ChannelID
	Priority      domain.Priority
	KafkaConsumer ports.KafkaTransactionalConsumer
}

// RetryDeliveryServiceFactory creates the stateful processor for one retry
// bucket of one Channel. The channel worker manager supplies its consumer.
type RetryDeliveryServiceFactory func(RetryDeliveryServiceFactoryParams) (*services.RetryDeliveryService, error)

type RetryDeliveryServiceFactoryParams struct {
	ChannelID     domain.ChannelID
	Bucket        contracts.RetryBucketV1
	KafkaConsumer ports.KafkaTransactionalConsumer
}

// Services contains long-running-use-case implementations that do not belong
// to a primary HTTP or external-Kafka adapter.
type Services struct {
	ChannelProvisioning           *services.ChannelProvisioningService
	CampaignScheduler             *services.CampaignSchedulerService
	BatchedCampaignRunCoordinator *services.BatchedCampaignRunCoordinatorService
	BatchedSourceBatchFanout      *services.BatchedSourceBatchFanoutService
	InlineCampaignRunCoordinator  *services.InlineCampaignRunCoordinatorService
	InlineCampaignFanout          *services.InlineCampaignFanoutService
	CampaignProgressAggregator    *services.CampaignProgressAggregatorService
	CampaignStatsProjection       *services.CampaignStatsProjectionService
	NewDeliveryService            DeliveryServiceFactory
	NewRetryDeliveryService       RetryDeliveryServiceFactory
}

func NewServices(adapters *Adapters, config Config) *Services {
	return &Services{
		ChannelProvisioning: services.NewChannelProvisioningService(services.ChannelProvisioningServiceParams{
			ChannelRepository:       adapters.Channels,
			TransactionManager:      adapters.Transactions,
			ChannelTopicProvisioner: adapters.topicProvisioner,
		}),
		CampaignScheduler: services.NewCampaignSchedulerService(services.CampaignSchedulerServiceParams{
			CampaignRepository: adapters.Campaigns,
			TransactionManager: adapters.Transactions,
			KafkaProducer:      adapters.KafkaProducer,
			StartingTimeout:    config.CampaignStartingTimeout,
		}),
		BatchedCampaignRunCoordinator: services.NewBatchedCampaignRunCoordinatorService(services.BatchedCampaignRunCoordinatorServiceParams{
			CampaignRepository:    adapters.Campaigns,
			SourceBatchRepository: adapters.SourceBatches,
			TransactionManager:    adapters.Transactions,
			KafkaConsumer:         adapters.batchedCampaignRunConsumer,
		}),
		BatchedSourceBatchFanout: services.NewBatchedSourceBatchFanoutService(services.BatchedSourceBatchFanoutServiceParams{
			CampaignRepository:          adapters.Campaigns,
			SourceBatchRepository:       adapters.SourceBatches,
			MobileApplicationRepository: adapters.MobileApplications,
			PushInstallationRepository:  adapters.PushInstallations,
			KafkaConsumer:               adapters.batchedSourceFanoutConsumer,
		}),
		InlineCampaignRunCoordinator: services.NewInlineCampaignRunCoordinatorService(services.InlineCampaignRunCoordinatorServiceParams{
			CampaignRepository: adapters.Campaigns,
			TransactionManager: adapters.Transactions,
			KafkaConsumer:      adapters.inlineCampaignRunConsumer,
			BatchSize:          config.InlineCampaignRunBatchSize,
		}),
		InlineCampaignFanout: services.NewInlineCampaignFanoutService(services.InlineCampaignFanoutServiceParams{
			CampaignRepository:          adapters.Campaigns,
			MobileApplicationRepository: adapters.MobileApplications,
			PushInstallationRepository:  adapters.PushInstallations,
			KafkaConsumer:               adapters.inlineCampaignFanoutConsumer,
			BatchSize:                   config.InlineCampaignFanoutBatchSize,
		}),
		CampaignStatsProjection: services.NewCampaignStatsProjectionService(services.CampaignStatsProjectionServiceParams{
			CampaignRepository: adapters.Campaigns,
			TransactionManager: adapters.Transactions,
			KafkaConsumer:      adapters.campaignStatsConsumer,
			BatchSize:          config.CampaignStatsBatchSize,
		}),
		CampaignProgressAggregator: services.NewCampaignProgressAggregatorService(services.CampaignProgressAggregatorServiceParams{
			KafkaConsumer:        adapters.campaignProgressConsumer,
			CompactedTopicLoader: adapters.compactedTopicLoader,
			BatchSize:            config.CampaignProgressBatchSize,
		}),
		NewDeliveryService: func(params DeliveryServiceFactoryParams) (*services.DeliveryService, error) {
			return services.NewDeliveryService(services.DeliveryServiceParams{
				ChannelID:                  params.ChannelID,
				Priority:                   params.Priority,
				BatchSize:                  config.DeliveryProcessingBatchSize,
				CampaignRepository:         adapters.Campaigns,
				ChannelRepository:          adapters.Channels,
				TenantRepository:           adapters.Tenants,
				ProviderRepository:         adapters.Providers,
				PushInstallationRepository: adapters.PushInstallations,
				RateLimiter:                adapters.RateLimiter,
				CallSemaphore:              adapters.PushCallSemaphore,
				PushSender:                 adapters.PushSender,
				KafkaConsumer:              params.KafkaConsumer,
			})
		},
		NewRetryDeliveryService: func(params RetryDeliveryServiceFactoryParams) (*services.RetryDeliveryService, error) {
			return services.NewRetryDeliveryService(services.RetryDeliveryServiceParams{
				ChannelID:                  params.ChannelID,
				Bucket:                     params.Bucket,
				CampaignRepository:         adapters.Campaigns,
				ChannelRepository:          adapters.Channels,
				TenantRepository:           adapters.Tenants,
				ProviderRepository:         adapters.Providers,
				PushInstallationRepository: adapters.PushInstallations,
				RateLimiter:                adapters.RateLimiter,
				CallSemaphore:              adapters.PushCallSemaphore,
				PushSender:                 adapters.PushSender,
				KafkaConsumer:              params.KafkaConsumer,
			})
		},
	}
}
