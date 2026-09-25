package services

import (
	"context"
	"fmt"
	"sort"
	"time"

	"golang.org/x/sync/errgroup"
	"uuid"

	"github.com/superman/pushkin/internal/application"
	"github.com/superman/pushkin/internal/application/ports"
	contracts "github.com/superman/pushkin/internal/contracts/kafka"
	"github.com/superman/pushkin/internal/domain"
)

type DeliveryServiceParams struct {
	ChannelID                  domain.ChannelID
	Priority                   domain.Priority
	BatchSize                  int
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

// DeliveryService processes batches from one priority topic of one Channel.
// It is not safe for concurrent use because one instance owns one Kafka
// consumer assignment.
type DeliveryService struct {
	topic         contracts.Topic
	channelID     domain.ChannelID
	priority      domain.Priority
	batchSize     int
	campaigns     ports.CampaignRepository
	channels      ports.ChannelRepository
	tenants       ports.TenantRepository
	providers     ports.ProviderRepository
	installations ports.PushInstallationRepository
	rateLimiter   ports.DeliveryRateLimiter
	callSemaphore ports.PushCallSemaphore
	pushSender    ports.PushSender
	kafkaConsumer ports.KafkaTransactionalConsumer
	pausedUntil   *time.Time
}

func NewDeliveryService(params DeliveryServiceParams) (*DeliveryService, error) {
	topic, err := contracts.DeliveryTopic(contracts.PriorityV1(params.Priority), params.ChannelID)
	if err != nil {
		return nil, fmt.Errorf("delivery service topic: %w", err)
	}
	return &DeliveryService{
		topic:         topic,
		channelID:     params.ChannelID,
		priority:      params.Priority,
		batchSize:     params.BatchSize,
		campaigns:     params.CampaignRepository,
		channels:      params.ChannelRepository,
		tenants:       params.TenantRepository,
		providers:     params.ProviderRepository,
		installations: params.PushInstallationRepository,
		rateLimiter:   params.RateLimiter,
		callSemaphore: params.CallSemaphore,
		pushSender:    params.PushSender,
		kafkaConsumer: params.KafkaConsumer,
	}, nil
}

// Process resumes a rate-limited topic when due, then processes one processing
// group. When permits are unavailable it seeks every polled record back and
// pauses the whole topic before any input offset is committed.
func (s *DeliveryService) Process(ctx context.Context) error {
	if s.batchSize <= 0 {
		return fmt.Errorf("delivery batch size: %w", application.ErrValidation)
	}
	now := time.Now().UTC()
	if s.pausedUntil != nil && !s.pausedUntil.After(now) {
		if err := s.kafkaConsumer.ResumeAll(ctx); err != nil {
			return err
		}
		s.pausedUntil = nil
	}
	records, err := s.kafkaConsumer.PollMany(ctx, s.batchSize)
	if err != nil || len(records) == 0 {
		return err
	}

	works, err := s.validateWorks(records)
	if err != nil {
		return err
	}
	channel, err := s.channels.FindByID(ctx, s.channelID)
	if err != nil || channel == nil {
		return deliveryDependencyError("channel", err)
	}
	provider, err := s.providers.FindByID(ctx, channel.ProviderID())
	if err != nil || provider == nil {
		return deliveryDependencyError("provider", err)
	}
	if err := validateDeliveryRoute(works, channel, provider); err != nil {
		return err
	}
	tenant, err := s.tenants.FindByID(ctx, channel.TenantID())
	if err != nil || tenant == nil {
		return deliveryDependencyError("tenant", err)
	}

	prepared, err := s.prepare(ctx, works, channel.TenantID())
	if err != nil {
		return err
	}
	if len(prepared.sendable) > 0 {
		reservation, err := s.rateLimiter.Reserve(ctx, tenant, provider, len(prepared.sendable))
		if err != nil {
			return err
		}
		if !reservation.Granted {
			return s.rewindAndPause(ctx, records, reservation.AvailableAt)
		}
		if err := s.send(ctx, prepared.sendable, provider); err != nil {
			return err
		}
	}

	messages, err := s.resultMessages(ctx, works, prepared, now)
	if err != nil {
		return err
	}
	if err := s.kafkaConsumer.Complete(ctx, messages); err != nil {
		return err
	}
	return nil
}

func (s *DeliveryService) rewindAndPause(
	ctx context.Context,
	records []ports.KafkaRecord,
	until time.Time,
) error {
	offsets := deliveryRewindOffsets(records)
	if err := s.kafkaConsumer.Seek(ctx, offsets); err != nil {
		return err
	}
	if err := s.kafkaConsumer.PauseAll(ctx); err != nil {
		return err
	}
	s.pausedUntil = &until
	return nil
}

func deliveryRewindOffsets(records []ports.KafkaRecord) ports.KafkaPartitionOffsets {
	offsets := make(ports.KafkaPartitionOffsets)
	for _, record := range records {
		partition := partitionFromOffset(record.Offset)
		if current, found := offsets[partition]; !found || record.Offset.Offset < current {
			offsets[partition] = record.Offset.Offset
		}
	}
	return offsets
}

type preparedDeliveryBatch struct {
	sendable []preparedDelivery
	failed   map[uuid.UUID]string
}

type preparedDelivery struct {
	work    contracts.DeliveryWorkV1
	token   string
	payload domain.PushPayload
	result  ports.PushSendResult
}

func (s *DeliveryService) validateWorks(records []ports.KafkaRecord) ([]contracts.DeliveryWorkV1, error) {
	works := make([]contracts.DeliveryWorkV1, 0, len(records))
	for _, record := range records {
		work, ok := record.Value.(contracts.DeliveryWorkV1)
		if !ok {
			return nil, fmt.Errorf("delivery work message %T: %w", record.Value, application.ErrValidation)
		}
		if work.DeliveryID == (uuid.UUID{}) || work.CampaignID == (uuid.UUID{}) || work.TenantID == (uuid.UUID{}) ||
			work.NotificationID == (uuid.UUID{}) || work.UserID == "" || work.NotificationCreatedAt.IsZero() ||
			work.ChannelID == (uuid.UUID{}) || work.PushInstallationID == (uuid.UUID{}) ||
			work.RetryAttempt > domain.MaxDeliveryRetryAttempts {
			return nil, fmt.Errorf("delivery work: %w", application.ErrValidation)
		}
		if work.ChannelID != s.channelID || work.Priority != string(s.priority) {
			return nil, fmt.Errorf("delivery work topic: %w", application.ErrValidation)
		}
		works = append(works, work)
	}
	return works, nil
}

func validateDeliveryRoute(works []contracts.DeliveryWorkV1, channel *domain.Channel, provider *domain.Provider) error {
	if channel.Type() != domain.ChannelTypeMobilePush || channel.Status() != domain.ChannelStatusActive ||
		provider.Status() != domain.ConfigurationStatusActive {
		return fmt.Errorf("delivery route is disabled: %w", application.ErrConflict)
	}
	for _, work := range works {
		if work.TenantID != channel.TenantID() || work.ChannelID != channel.ID() || provider.TenantID() != work.TenantID {
			return fmt.Errorf("delivery work route: %w", application.ErrValidation)
		}
	}
	return nil
}

func (s *DeliveryService) prepare(
	ctx context.Context,
	works []contracts.DeliveryWorkV1,
	tenantID domain.TenantID,
) (preparedDeliveryBatch, error) {
	campaignIDs := make([]domain.CampaignID, 0, len(works))
	installationIDs := make([]domain.PushInstallationID, 0, len(works))
	for _, work := range works {
		campaignIDs = append(campaignIDs, work.CampaignID)
		installationIDs = append(installationIDs, work.PushInstallationID)
	}
	campaigns, err := s.campaigns.FindByIDs(ctx, campaignIDs)
	if err != nil {
		return preparedDeliveryBatch{}, err
	}
	tokens, err := s.installations.ListActiveTokensByIDs(ctx, tenantID, installationIDs)
	if err != nil {
		return preparedDeliveryBatch{}, err
	}
	batch := preparedDeliveryBatch{failed: make(map[uuid.UUID]string)}
	for _, work := range works {
		campaign := campaigns[work.CampaignID]
		if campaign == nil || campaign.TenantID() != work.TenantID || campaign.ChannelID() != work.ChannelID || string(campaign.Priority()) != work.Priority {
			batch.failed[work.DeliveryID] = "campaign_unavailable"
			continue
		}
		token, found := tokens[work.PushInstallationID]
		if !found || token == "" {
			batch.failed[work.DeliveryID] = "push_installation_unavailable"
			continue
		}
		batch.sendable = append(batch.sendable, preparedDelivery{work: work, token: token, payload: campaign.PushPayload()})
	}
	return batch, nil
}

func (s *DeliveryService) send(ctx context.Context, deliveries []preparedDelivery, provider *domain.Provider) error {
	var group errgroup.Group
	for index := range deliveries {
		group.Go(func() error {
			delivery := &deliveries[index]
			if err := s.callSemaphore.Acquire(ctx); err != nil {
				return err
			}
			defer s.callSemaphore.Release()
			result, err := s.pushSender.Send(ctx, ports.PushSendRequest{
				ProviderID: provider.ID(), ProviderType: provider.Type(), EncryptedCredentials: provider.EncryptedCredentials(), Token: delivery.token, Payload: delivery.payload,
			})
			if err != nil {
				return err
			}
			delivery.result = result
			return nil
		})
	}
	return group.Wait()
}

func (s *DeliveryService) resultMessages(ctx context.Context, works []contracts.DeliveryWorkV1, prepared preparedDeliveryBatch, now time.Time) ([]ports.OutboundKafkaMessage, error) {
	byID := make(map[uuid.UUID]preparedDelivery, len(prepared.sendable))
	for _, delivery := range prepared.sendable {
		byID[delivery.work.DeliveryID] = delivery
	}
	accepted := make(map[uuid.UUID]uint64)
	failed := make(map[uuid.UUID]uint64)
	notificationIDs := make(map[uuid.UUID]struct{})
	messages := make([]ports.OutboundKafkaMessage, 0, len(works))
	for _, work := range works {
		if _, found := prepared.failed[work.DeliveryID]; found {
			failed[work.CampaignID]++
			continue
		}
		delivery := byID[work.DeliveryID]
		switch delivery.result.Outcome {
		case ports.PushSendOutcomeAccepted:
			accepted[work.CampaignID]++
			if _, emitted := notificationIDs[work.NotificationID]; !emitted {
				notificationMessage, err := acceptedNotificationMessage(work, delivery.payload)
				if err != nil {
					return nil, err
				}
				messages = append(messages, notificationMessage)
				notificationIDs[work.NotificationID] = struct{}{}
			}
		case ports.PushSendOutcomeInvalidToken:
			if err := s.installations.Deactivate(ctx, work.TenantID, work.PushInstallationID); err != nil {
				return nil, err
			}
			failed[work.CampaignID]++
		case ports.PushSendOutcomeRetryable:
			retry, err := retryMessage(work, delivery.result, now)
			if err != nil {
				failed[work.CampaignID]++
				continue
			}
			messages = append(messages, retry)
		case ports.PushSendOutcomeFailed:
			failed[work.CampaignID]++
		default:
			return nil, fmt.Errorf("push provider outcome %q: %w", delivery.result.Outcome, application.ErrValidation)
		}
	}
	for _, campaignID := range sortedCampaignIDs(accepted, failed) {
		messages = append(messages, ports.OutboundKafkaMessage{Topic: contracts.TopicCampaignProgress, Key: []byte(campaignID.String()), Value: contracts.CampaignProgressDeltaV1{MessageHeaderV1: contracts.NewMessageHeaderV1(), Type: contracts.CampaignProgressEventTypeDeliveryDelta, CampaignID: campaignID, DeliveryAcceptedDelta: accepted[campaignID], DeliveryFailedDelta: failed[campaignID]}})
	}
	return messages, nil
}

func acceptedNotificationMessage(work contracts.DeliveryWorkV1, payload domain.PushPayload) (ports.OutboundKafkaMessage, error) {
	notification, err := domain.HydrateNotification(domain.HydrateNotificationParams{
		ID: work.NotificationID, CampaignID: work.CampaignID, TenantID: work.TenantID,
		UserID: domain.UserID(work.UserID), CreatedAt: work.NotificationCreatedAt, Payload: payload,
	})
	if err != nil {
		return ports.OutboundKafkaMessage{}, fmt.Errorf("hydrate notification: %w", err)
	}
	return ports.OutboundKafkaMessage{
		Topic: contracts.TopicNotificationAccepted,
		Key:   []byte(work.TenantID.String() + ":" + work.UserID),
		Value: contracts.NewNotificationAcceptedV1(notification),
	}, nil
}

func retryMessage(work contracts.DeliveryWorkV1, result ports.PushSendResult, now time.Time) (ports.OutboundKafkaMessage, error) {
	attempt := work.RetryAttempt + 1
	bucket, delay, err := retryBucket(attempt)
	if err != nil {
		return ports.OutboundKafkaMessage{}, err
	}
	if result.RetryAfter != nil && *result.RetryAfter > delay {
		delay = *result.RetryAfter
	}
	topic, err := contracts.RetryTopic(bucket, work.ChannelID)
	if err != nil {
		return ports.OutboundKafkaMessage{}, err
	}
	work.RetryAttempt = attempt
	return ports.OutboundKafkaMessage{Topic: topic, Key: []byte(work.DeliveryID.String()), Value: contracts.RetryWorkV1{DeliveryWorkV1: work, DueAt: now.Add(delay)}}, nil
}

func retryBucket(attempt uint) (contracts.RetryBucketV1, time.Duration, error) {
	switch attempt {
	case 1:
		return contracts.RetryBucketOneMinuteV1, time.Minute, nil
	case 2:
		return contracts.RetryBucketFiveMinutesV1, 5 * time.Minute, nil
	case 3:
		return contracts.RetryBucketThirtyMinutesV1, 30 * time.Minute, nil
	default:
		return "", 0, fmt.Errorf("delivery retry attempt %d: %w", attempt, application.ErrConflict)
	}
}

func sortedCampaignIDs(accepted, failed map[uuid.UUID]uint64) []uuid.UUID {
	ids := make(map[uuid.UUID]struct{}, len(accepted)+len(failed))
	for id := range accepted {
		ids[id] = struct{}{}
	}
	for id := range failed {
		ids[id] = struct{}{}
	}
	result := make([]uuid.UUID, 0, len(ids))
	for id := range ids {
		result = append(result, id)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].String() < result[j].String() })
	return result
}

func deliveryDependencyError(name string, err error) error {
	if err != nil {
		return err
	}
	return fmt.Errorf("delivery %s: %w", name, application.ErrValidation)
}
