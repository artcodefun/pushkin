package services

import (
	"context"
	"fmt"
	"sort"
	"time"

	"uuid"

	"github.com/superman/pushkin/internal/application"
	"github.com/superman/pushkin/internal/application/ports"
	contracts "github.com/superman/pushkin/internal/contracts/kafka"
	"github.com/superman/pushkin/internal/domain"
)

type RetryDeliveryServiceParams struct {
	ChannelID                  domain.ChannelID
	Bucket                     contracts.RetryBucketV1
	CampaignRepository         ports.CampaignRepository
	ChannelRepository          ports.ChannelRepository
	TenantRepository           ports.TenantRepository
	ProviderRepository         ports.ProviderRepository
	PushInstallationRepository ports.PushInstallationRepository
	RateLimiter                ports.DeliveryRateLimiter
	CallSemaphore              ports.PushCallSemaphore
	PushSender                 ports.PushSender
	KafkaConsumer              ports.KafkaTransactionalConsumer
}

// RetryDeliveryService consumes one retry bucket of one Channel. It is not
// safe for concurrent use: the instance owns its consumer assignment and the
// paused-partition state associated with it.
type RetryDeliveryService struct {
	topic         contracts.Topic
	channelID     domain.ChannelID
	campaigns     ports.CampaignRepository
	channels      ports.ChannelRepository
	tenants       ports.TenantRepository
	providers     ports.ProviderRepository
	installations ports.PushInstallationRepository
	rateLimiter   ports.DeliveryRateLimiter
	callSemaphore ports.PushCallSemaphore
	pushSender    ports.PushSender
	kafkaConsumer ports.KafkaTransactionalConsumer
	pausedUntil   map[ports.KafkaPartition]time.Time
}

func NewRetryDeliveryService(params RetryDeliveryServiceParams) (*RetryDeliveryService, error) {
	topic, err := contracts.RetryTopic(params.Bucket, params.ChannelID)
	if err != nil {
		return nil, fmt.Errorf("retry delivery service topic: %w", err)
	}
	return &RetryDeliveryService{
		topic:         topic,
		channelID:     params.ChannelID,
		campaigns:     params.CampaignRepository,
		channels:      params.ChannelRepository,
		tenants:       params.TenantRepository,
		providers:     params.ProviderRepository,
		installations: params.PushInstallationRepository,
		rateLimiter:   params.RateLimiter,
		callSemaphore: params.CallSemaphore,
		pushSender:    params.PushSender,
		kafkaConsumer: params.KafkaConsumer,
		pausedUntil:   make(map[ports.KafkaPartition]time.Time),
	}, nil
}

// Process resumes due partitions, then processes one retry record. A future
// record is sought back and its partition is paused before any offset commit.
func (s *RetryDeliveryService) Process(ctx context.Context) error {
	now := time.Now().UTC()
	if err := s.resumeDuePartitions(ctx, now); err != nil {
		return err
	}

	record, found, err := s.kafkaConsumer.Poll(ctx)
	if err != nil || !found {
		return err
	}
	retry, ok := record.Value.(contracts.RetryWorkV1)
	if !ok {
		return fmt.Errorf("retry work message %T: %w", record.Value, application.ErrValidation)
	}
	if retry.DueAt.IsZero() {
		return fmt.Errorf("retry work due_at: %w", application.ErrValidation)
	}
	if retry.DueAt.After(now) {
		return s.rewindAndPause(ctx, record, retry.DueAt)
	}

	work, err := s.validateWork(retry)
	if err != nil {
		return err
	}
	channel, err := s.channels.FindByID(ctx, s.channelID)
	if err != nil || channel == nil {
		return retryDeliveryDependencyError("channel", err)
	}
	provider, err := s.providers.FindByID(ctx, channel.ProviderID())
	if err != nil || provider == nil {
		return retryDeliveryDependencyError("provider", err)
	}
	if err := validateRetryDeliveryRoute(work, channel, provider); err != nil {
		return err
	}
	tenant, err := s.tenants.FindByID(ctx, channel.TenantID())
	if err != nil || tenant == nil {
		return retryDeliveryDependencyError("tenant", err)
	}

	delivery, err := s.prepare(ctx, work)
	if err != nil {
		return err
	}
	if !delivery.terminalFailure {
		reservation, err := s.rateLimiter.Reserve(ctx, tenant, provider, 1)
		if err != nil {
			return err
		}
		if !reservation.Granted {
			return s.rewindAndPause(ctx, record, reservation.AvailableAt)
		}
		result, err := s.send(ctx, delivery, provider)
		if err != nil {
			return err
		}
		delivery.result = result
	}

	messages, err := s.resultMessages(ctx, delivery, now)
	if err != nil {
		return err
	}
	if err := s.kafkaConsumer.Complete(ctx, messages); err != nil {
		return err
	}
	return nil
}

func (s *RetryDeliveryService) resumeDuePartitions(ctx context.Context, now time.Time) error {
	partitions := make([]ports.KafkaPartition, 0, len(s.pausedUntil))
	for partition, dueAt := range s.pausedUntil {
		if !dueAt.After(now) {
			partitions = append(partitions, partition)
		}
	}
	if len(partitions) == 0 {
		return nil
	}
	sortKafkaPartitions(partitions)
	if err := s.kafkaConsumer.Resume(ctx, partitions); err != nil {
		return err
	}
	for _, partition := range partitions {
		delete(s.pausedUntil, partition)
	}
	return nil
}

func (s *RetryDeliveryService) rewindAndPause(
	ctx context.Context,
	record ports.KafkaRecord,
	until time.Time,
) error {
	partition := partitionFromOffset(record.Offset)
	offsets := ports.KafkaPartitionOffsets{partition: record.Offset.Offset}
	if err := s.kafkaConsumer.Seek(ctx, offsets); err != nil {
		return err
	}
	if err := s.kafkaConsumer.Pause(ctx, []ports.KafkaPartition{partition}); err != nil {
		return err
	}
	s.pausedUntil[partition] = until
	return nil
}

func (s *RetryDeliveryService) validateWork(retry contracts.RetryWorkV1) (contracts.DeliveryWorkV1, error) {
	work := retry.DeliveryWorkV1
	if work.DeliveryID == (uuid.UUID{}) || work.CampaignID == (uuid.UUID{}) || work.TenantID == (uuid.UUID{}) ||
		work.ChannelID != s.channelID || work.PushInstallationID == (uuid.UUID{}) ||
		work.RetryAttempt == 0 || work.RetryAttempt > domain.MaxDeliveryRetryAttempts {
		return contracts.DeliveryWorkV1{}, fmt.Errorf("retry delivery work: %w", application.ErrValidation)
	}
	return work, nil
}

func validateRetryDeliveryRoute(work contracts.DeliveryWorkV1, channel *domain.Channel, provider *domain.Provider) error {
	if channel.Type() != domain.ChannelTypeMobilePush || channel.Status() != domain.ChannelStatusActive ||
		provider.Status() != domain.ConfigurationStatusActive {
		return fmt.Errorf("retry delivery route is disabled: %w", application.ErrConflict)
	}
	if work.TenantID != channel.TenantID() || provider.TenantID() != work.TenantID {
		return fmt.Errorf("retry delivery work route: %w", application.ErrValidation)
	}
	return nil
}

type preparedRetryDelivery struct {
	work            contracts.DeliveryWorkV1
	token           string
	payload         domain.PushPayload
	terminalFailure bool
	result          ports.PushSendResult
}

func (s *RetryDeliveryService) prepare(
	ctx context.Context,
	work contracts.DeliveryWorkV1,
) (preparedRetryDelivery, error) {
	delivery := preparedRetryDelivery{work: work}
	campaign, err := s.campaigns.FindByID(ctx, work.CampaignID)
	if err != nil {
		return preparedRetryDelivery{}, err
	}
	if campaign == nil || campaign.TenantID() != work.TenantID || campaign.ChannelID() != work.ChannelID || string(campaign.Priority()) != work.Priority {
		delivery.terminalFailure = true
		return delivery, nil
	}
	tokens, err := s.installations.ListActiveTokensByIDs(ctx, work.TenantID, []domain.PushInstallationID{work.PushInstallationID})
	if err != nil {
		return preparedRetryDelivery{}, err
	}
	token, found := tokens[work.PushInstallationID]
	if !found || token == "" {
		delivery.terminalFailure = true
		return delivery, nil
	}
	delivery.token = token
	delivery.payload = campaign.PushPayload()
	return delivery, nil
}

func (s *RetryDeliveryService) send(
	ctx context.Context,
	delivery preparedRetryDelivery,
	provider *domain.Provider,
) (ports.PushSendResult, error) {
	if err := s.callSemaphore.Acquire(ctx); err != nil {
		return ports.PushSendResult{}, err
	}
	defer s.callSemaphore.Release()
	return s.pushSender.Send(ctx, ports.PushSendRequest{
		ProviderID: provider.ID(), ProviderType: provider.Type(), EncryptedCredentials: provider.EncryptedCredentials(), Token: delivery.token, Payload: delivery.payload,
	})
}

func (s *RetryDeliveryService) resultMessages(
	ctx context.Context,
	delivery preparedRetryDelivery,
	now time.Time,
) ([]ports.OutboundKafkaMessage, error) {
	work := delivery.work
	if delivery.terminalFailure {
		return terminalProgressMessage(work, 0, 1), nil
	}

	switch delivery.result.Outcome {
	case ports.PushSendOutcomeAccepted:
		return terminalProgressMessage(work, 1, 0), nil
	case ports.PushSendOutcomeInvalidToken:
		if err := s.installations.Deactivate(ctx, work.TenantID, work.PushInstallationID); err != nil {
			return nil, err
		}
		return terminalProgressMessage(work, 0, 1), nil
	case ports.PushSendOutcomeRetryable:
		retry, err := retryMessage(work, delivery.result, now)
		if err != nil {
			return terminalProgressMessage(work, 0, 1), nil
		}
		return []ports.OutboundKafkaMessage{retry}, nil
	case ports.PushSendOutcomeFailed:
		return terminalProgressMessage(work, 0, 1), nil
	default:
		return nil, fmt.Errorf("retry push provider outcome %q: %w", delivery.result.Outcome, application.ErrValidation)
	}
}

func terminalProgressMessage(
	work contracts.DeliveryWorkV1,
	accepted uint64,
	failed uint64,
) []ports.OutboundKafkaMessage {
	return []ports.OutboundKafkaMessage{{
		Topic: contracts.TopicCampaignProgress,
		Key:   []byte(work.CampaignID.String()),
		Value: contracts.CampaignProgressDeltaV1{
			MessageHeaderV1:       contracts.NewMessageHeaderV1(),
			Type:                  contracts.CampaignProgressEventTypeDeliveryDelta,
			CampaignID:            work.CampaignID,
			DeliveryAcceptedDelta: accepted,
			DeliveryFailedDelta:   failed,
		},
	}}
}

func partitionFromOffset(offset ports.KafkaOffset) ports.KafkaPartition {
	return ports.KafkaPartition{Topic: offset.Topic, Partition: offset.Partition}
}

func sortKafkaPartitions(partitions []ports.KafkaPartition) {
	sort.Slice(partitions, func(i, j int) bool {
		if partitions[i].Topic != partitions[j].Topic {
			return partitions[i].Topic < partitions[j].Topic
		}
		return partitions[i].Partition < partitions[j].Partition
	})
}

func retryDeliveryDependencyError(name string, err error) error {
	if err != nil {
		return err
	}
	return fmt.Errorf("retry delivery %s: %w", name, application.ErrValidation)
}
