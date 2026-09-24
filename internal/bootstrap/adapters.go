package bootstrap

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	redisclient "github.com/redis/go-redis/v9"

	publickafka "github.com/superman/pushkin/api/kafka/v1"
	"github.com/superman/pushkin/internal/application/ports"
	contracts "github.com/superman/pushkin/internal/contracts/kafka"
	"github.com/superman/pushkin/internal/infrastructure/credentials"
	infrakafka "github.com/superman/pushkin/internal/infrastructure/kafka"
	"github.com/superman/pushkin/internal/infrastructure/postgres/gen"
	readrepo "github.com/superman/pushkin/internal/infrastructure/postgres/readrepo"
	repo "github.com/superman/pushkin/internal/infrastructure/postgres/repo"
	"github.com/superman/pushkin/internal/infrastructure/providers"
	"github.com/superman/pushkin/internal/infrastructure/providers/fcm"
	"github.com/superman/pushkin/internal/infrastructure/providers/testsender"
	pushredis "github.com/superman/pushkin/internal/infrastructure/redis"
	"github.com/superman/pushkin/internal/infrastructure/security"
)

// Adapters contains the secondary adapters shared by Pushkin use cases.
// It is a typed dependency bundle, not a service locator.
type Adapters struct {
	Campaigns          ports.CampaignRepository
	Channels           ports.ChannelRepository
	MobileApplications ports.MobileApplicationRepository
	Providers          ports.ProviderRepository
	PushInstallations  ports.PushInstallationRepository
	SourceBatches      ports.SourceBatchRepository
	Tenants            ports.TenantRepository
	TenantAPIKeys      ports.TenantAPIKeyRepository
	Users              ports.UserRepository
	Transactions       ports.TransactionManager

	CampaignReads          ports.CampaignReadRepository
	TenantReads            ports.TenantReadRepository
	ChannelReads           ports.ChannelReadRepository
	MobileApplicationReads ports.MobileApplicationReadRepository
	ProviderReads          ports.ProviderReadRepository

	CredentialsCipher     ports.CredentialsCipher
	TenantAPIKeyHasher    ports.TenantAPIKeyHasher
	TenantAPIKeyValidator ports.TenantAPIKeyValidator
	KafkaProducer         ports.KafkaProducer
	RateLimiter           ports.DeliveryRateLimiter
	PushSender            ports.PushSender
	PushCallSemaphore     ports.PushCallSemaphore

	pool             *pgxpool.Pool
	redisClient      *redisclient.Client
	producer         *infrakafka.Producer
	topicProvisioner *infrakafka.TopicProvisioner

	batchedCampaignRunConsumer   *infrakafka.TransactionalConsumer
	batchedSourceFanoutConsumer  *infrakafka.TransactionalConsumer
	inlineCampaignRunConsumer    *infrakafka.TransactionalConsumer
	inlineCampaignFanoutConsumer *infrakafka.TransactionalConsumer
	campaignProgressConsumer     *infrakafka.TransactionalConsumer
	campaignStatsConsumer        *infrakafka.Consumer
	userEventsConsumer           *infrakafka.ExternalConsumer
	compactedTopicLoader         *infrakafka.CompactedTopicLoader
}

func NewAdapters(config Config) (*Adapters, error) {
	poolConfig, err := pgxpool.ParseConfig(config.PostgresDSN)
	if err != nil {
		return nil, fmt.Errorf("parse PostgreSQL configuration: %w", err)
	}
	pool, err := pgxpool.NewWithConfig(context.Background(), poolConfig)
	if err != nil {
		return nil, fmt.Errorf("create PostgreSQL pool: %w", err)
	}
	cleanup := true
	defer func() {
		if cleanup {
			pool.Close()
		}
	}()

	cipher, err := credentials.NewCipher(credentials.CipherParams{Key: config.CredentialsCipherKey})
	if err != nil {
		return nil, fmt.Errorf("create credentials cipher: %w", err)
	}
	apiKeyHasher, err := security.NewTenantAPIKeyHasher(config.APIKeyHashPepper)
	if err != nil {
		return nil, fmt.Errorf("create tenant API key hasher: %w", err)
	}
	producer, err := infrakafka.NewProducer(infrakafka.ProducerParams{Brokers: config.KafkaBrokers})
	if err != nil {
		return nil, fmt.Errorf("create Kafka producer: %w", err)
	}
	defer func() {
		if cleanup {
			producer.Close()
		}
	}()
	topicProvisioner, err := infrakafka.NewTopicProvisioner(infrakafka.TopicProvisionerParams{
		Brokers:           config.KafkaBrokers,
		WorkRetention:     config.KafkaWorkRetention,
		ProgressRetention: config.KafkaProgressRetention,
	})
	if err != nil {
		return nil, fmt.Errorf("create Kafka topic provisioner: %w", err)
	}
	defer func() {
		if cleanup {
			topicProvisioner.Close()
		}
	}()
	batchedCampaignRunConsumer, err := newTransactionalConsumer(
		config,
		batchedCampaignRunConsumerGroup,
		batchedCampaignRunTransactionalRole,
		contracts.TopicCampaignBatchedRun,
	)
	if err != nil {
		return nil, err
	}
	defer func() {
		if cleanup {
			batchedCampaignRunConsumer.Close()
		}
	}()
	batchedSourceFanoutConsumer, err := newTransactionalConsumer(
		config,
		batchedSourceFanoutConsumerGroup,
		batchedSourceFanoutTransactionalRole,
		contracts.TopicCampaignBatchedSourceBatchFanout,
	)
	if err != nil {
		return nil, err
	}
	defer func() {
		if cleanup {
			batchedSourceFanoutConsumer.Close()
		}
	}()
	inlineCampaignRunConsumer, err := newTransactionalConsumer(
		config,
		inlineCampaignRunConsumerGroup,
		inlineCampaignRunTransactionalRole,
		contracts.TopicCampaignInlineRun,
	)
	if err != nil {
		return nil, err
	}
	defer func() {
		if cleanup {
			inlineCampaignRunConsumer.Close()
		}
	}()
	inlineCampaignFanoutConsumer, err := newTransactionalConsumer(
		config,
		inlineCampaignFanoutConsumerGroup,
		inlineCampaignFanoutTransactionalRole,
		contracts.TopicCampaignInlineFanout,
	)
	if err != nil {
		return nil, err
	}
	defer func() {
		if cleanup {
			inlineCampaignFanoutConsumer.Close()
		}
	}()
	campaignProgressConsumer, err := newTransactionalConsumer(
		config,
		campaignProgressConsumerGroup,
		campaignProgressTransactionalRole,
		contracts.TopicCampaignProgress,
	)
	if err != nil {
		return nil, err
	}
	defer func() {
		if cleanup {
			campaignProgressConsumer.Close()
		}
	}()
	campaignStatsConsumer, err := infrakafka.NewConsumer(infrakafka.ConsumerParams{
		Brokers:       config.KafkaBrokers,
		ConsumerGroup: campaignStatsConsumerGroup,
		InputTopic:    contracts.TopicCampaignStats,
	})
	if err != nil {
		return nil, fmt.Errorf("create campaign stats Kafka consumer: %w", err)
	}
	defer func() {
		if cleanup {
			campaignStatsConsumer.Close()
		}
	}()
	userEventsConsumer, err := infrakafka.NewExternalConsumer(infrakafka.ExternalConsumerParams{
		Brokers:       config.KafkaBrokers,
		ConsumerGroup: userEventsConsumerGroup,
		InputTopic:    publickafka.UserEventsTopic,
	})
	if err != nil {
		return nil, fmt.Errorf("create user events Kafka consumer: %w", err)
	}
	defer func() {
		if cleanup {
			userEventsConsumer.Close()
		}
	}()
	compactedTopicLoader, err := infrakafka.NewCompactedTopicLoader(infrakafka.CompactedTopicLoaderParams{
		Brokers: config.KafkaBrokers,
	})
	if err != nil {
		return nil, fmt.Errorf("create compacted Kafka topic loader: %w", err)
	}

	redis := redisclient.NewClient(&redisclient.Options{Addr: config.RedisAddress})
	rateLimiter, err := pushredis.NewRateLimiter(pushredis.RateLimiterParams{Client: redis})
	if err != nil {
		return nil, fmt.Errorf("create Redis rate limiter: %w", err)
	}
	callSemaphore, err := providers.NewPushCallSemaphore(config.DeliveryMaxInFlight)
	if err != nil {
		return nil, fmt.Errorf("create push call semaphore: %w", err)
	}
	pushHTTPClient := newPushHTTPClient(config.DeliveryMaxInFlight)
	var fcmSender ports.PushSender
	if config.UseTestSender {
		fcmSender, err = testsender.NewSender(testsender.SenderParams{
			HTTPClient: pushHTTPClient,
			BaseURL:    config.TestSenderURL,
		})
	} else {
		fcmSender, err = fcm.NewSender(fcm.SenderParams{
			HTTPClient:        pushHTTPClient,
			CredentialsCipher: cipher,
		})
	}
	if err != nil {
		return nil, fmt.Errorf("create FCM sender: %w", err)
	}
	pushSender, err := providers.NewProxySender(fcmSender)
	if err != nil {
		return nil, fmt.Errorf("create provider sender proxy: %w", err)
	}

	queries := gen.New(pool)
	readQueries := gen.New(pool)
	tenantAPIKeys := repo.NewTenantAPIKeyRepository(queries)
	cleanup = false
	return &Adapters{
		Campaigns:          repo.NewCampaignRepository(queries),
		Channels:           repo.NewChannelRepository(queries),
		MobileApplications: repo.NewMobileApplicationRepository(queries),
		Providers:          repo.NewProviderRepository(queries),
		PushInstallations:  repo.NewPushInstallationRepository(queries),
		SourceBatches:      repo.NewSourceBatchRepository(queries),
		Tenants:            repo.NewTenantRepository(queries),
		TenantAPIKeys:      tenantAPIKeys,
		Users:              repo.NewUserRepository(queries),
		Transactions:       repo.NewTransactionManager(pool),

		CampaignReads:          readrepo.NewCampaignRepository(readQueries),
		TenantReads:            readrepo.NewTenantRepository(readQueries),
		ChannelReads:           readrepo.NewChannelRepository(readQueries),
		MobileApplicationReads: readrepo.NewMobileApplicationRepository(readQueries),
		ProviderReads:          readrepo.NewProviderRepository(readQueries),

		CredentialsCipher:     cipher,
		TenantAPIKeyHasher:    apiKeyHasher,
		TenantAPIKeyValidator: security.NewTenantAPIKeyValidator(tenantAPIKeys, apiKeyHasher),
		KafkaProducer:         producer,
		RateLimiter:           rateLimiter,
		PushSender:            pushSender,
		PushCallSemaphore:     callSemaphore,
		topicProvisioner:      topicProvisioner,

		pool:        pool,
		redisClient: redis,
		producer:    producer,

		batchedCampaignRunConsumer:   batchedCampaignRunConsumer,
		batchedSourceFanoutConsumer:  batchedSourceFanoutConsumer,
		inlineCampaignRunConsumer:    inlineCampaignRunConsumer,
		inlineCampaignFanoutConsumer: inlineCampaignFanoutConsumer,
		campaignProgressConsumer:     campaignProgressConsumer,
		campaignStatsConsumer:        campaignStatsConsumer,
		userEventsConsumer:           userEventsConsumer,
		compactedTopicLoader:         compactedTopicLoader,
	}, nil
}

func newPushHTTPClient(maxInFlight int) *http.Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConns = maxInFlight
	transport.MaxIdleConnsPerHost = maxInFlight
	transport.MaxConnsPerHost = maxInFlight
	return &http.Client{Timeout: 30 * time.Second, Transport: transport}
}

// CheckReadiness verifies the dependencies that are safe to probe without
// publishing, consuming, or mutating service state.
func (a *Adapters) CheckReadiness(ctx context.Context) error {
	if err := a.pool.Ping(ctx); err != nil {
		return fmt.Errorf("ping PostgreSQL: %w", err)
	}
	if err := a.redisClient.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("ping Redis: %w", err)
	}
	if err := a.topicProvisioner.EnsureSystemTopics(ctx); err != nil {
		return fmt.Errorf("ensure Kafka system topics: %w", err)
	}
	return nil
}

func (a *Adapters) Close() {
	a.batchedCampaignRunConsumer.Close()
	a.batchedSourceFanoutConsumer.Close()
	a.inlineCampaignRunConsumer.Close()
	a.inlineCampaignFanoutConsumer.Close()
	a.campaignProgressConsumer.Close()
	a.campaignStatsConsumer.Close()
	a.userEventsConsumer.Close()
	a.producer.Close()
	a.topicProvisioner.Close()
	a.redisClient.Close()
	a.pool.Close()
}

func newTransactionalConsumer(
	config Config,
	consumerGroup string,
	role string,
	inputTopic contracts.Topic,
) (*infrakafka.TransactionalConsumer, error) {
	consumer, err := infrakafka.NewTransactionalConsumer(infrakafka.TransactionalConsumerParams{
		Brokers:         config.KafkaBrokers,
		ConsumerGroup:   consumerGroup,
		TransactionalID: fmt.Sprintf("pushkin.%s.%s", config.InstanceID, role),
		InputTopic:      inputTopic,
	})
	if err != nil {
		return nil, fmt.Errorf("create %s Kafka consumer: %w", role, err)
	}
	return consumer, nil
}
