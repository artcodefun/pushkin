package services

import (
	"context"
	"fmt"
	"time"

	"github.com/superman/pushkin/internal/application"
	"github.com/superman/pushkin/internal/application/ports"
	contracts "github.com/superman/pushkin/internal/contracts/kafka"
	"github.com/superman/pushkin/internal/domain"
)

type BatchedSourceBatchFanoutServiceParams struct {
	CampaignRepository          ports.CampaignRepository
	SourceBatchRepository       ports.SourceBatchRepository
	MobileApplicationRepository ports.MobileApplicationRepository
	PushInstallationRepository  ports.PushInstallationRepository
	KafkaConsumer               ports.KafkaTransactionalConsumer
}

// BatchedSourceBatchFanoutService resolves one durable source batch into individual push
// delivery work records and records the resulting delivery count.
type BatchedSourceBatchFanoutService struct {
	campaignRepository          ports.CampaignRepository
	sourceBatchRepository       ports.SourceBatchRepository
	mobileApplicationRepository ports.MobileApplicationRepository
	pushInstallationRepository  ports.PushInstallationRepository
	kafkaConsumer               ports.KafkaTransactionalConsumer
}

func NewBatchedSourceBatchFanoutService(params BatchedSourceBatchFanoutServiceParams) *BatchedSourceBatchFanoutService {
	return &BatchedSourceBatchFanoutService{
		campaignRepository:          params.CampaignRepository,
		sourceBatchRepository:       params.SourceBatchRepository,
		mobileApplicationRepository: params.MobileApplicationRepository,
		pushInstallationRepository:  params.PushInstallationRepository,
		kafkaConsumer:               params.KafkaConsumer,
	}
}

// Process consumes one source-batch request and atomically emits all derived
// delivery work records, its completion marker, and the input offset.
func (s *BatchedSourceBatchFanoutService) Process(ctx context.Context) error {
	record, found, err := s.kafkaConsumer.Poll(ctx)
	if err != nil || !found {
		return err
	}

	work, ok := record.Value.(contracts.BatchedSourceBatchFanoutV1)
	if !ok {
		return fmt.Errorf("source batch fanout message %T: %w", record.Value, application.ErrValidation)
	}

	batch, err := s.sourceBatchRepository.FindByID(ctx, work.SourceBatchID)
	if err != nil {
		return err
	}
	if batch == nil {
		return fmt.Errorf("source batch %s: %w", work.SourceBatchID, application.ErrValidation)
	}

	campaign, err := s.campaignRepository.FindByID(ctx, batch.CampaignID())
	if err != nil {
		return err
	}
	if campaign == nil {
		return fmt.Errorf("campaign %s: %w", batch.CampaignID(), application.ErrValidation)
	}

	mobileApplicationIDs, err := s.mobileApplicationRepository.ListIDsByChannel(ctx, campaign.ChannelID())
	if err != nil {
		return err
	}

	installationIDsByUser, err := s.pushInstallationRepository.ListActiveIDsByUsers(
		ctx,
		campaign.TenantID(),
		batch.UserIDs(),
		mobileApplicationIDs,
	)
	if err != nil {
		return err
	}

	deliveryTopic, err := contracts.DeliveryTopic(
		contracts.PriorityV1(campaign.Priority()),
		campaign.ChannelID(),
	)
	if err != nil {
		return fmt.Errorf("delivery topic: %w", err)
	}

	messages := make([]ports.OutboundKafkaMessage, 0, len(batch.UserIDs())+1)
	header := contracts.NewMessageHeaderV1()
	deliveryCount := 0
	for _, userID := range batch.UserIDs() {
		createdAt := time.Now().UTC()
		notificationID := domain.NewNotificationID()
		for _, installationID := range installationIDsByUser[userID] {
			work, err := domain.NewDeliveryWork(domain.NewDeliveryWorkParams{
				CampaignID:            campaign.ID(),
				TenantID:              campaign.TenantID(),
				NotificationID:        notificationID,
				UserID:                userID,
				NotificationCreatedAt: createdAt,
				ChannelID:             campaign.ChannelID(),
				PushInstallationID:    installationID,
				Priority:              campaign.Priority(),
			})
			if err != nil {
				return fmt.Errorf("new delivery work: %w", err)
			}
			messages = append(messages, ports.OutboundKafkaMessage{
				Topic: deliveryTopic,
				Key:   []byte(work.ID().String()),
				Value: contracts.NewDeliveryWorkV1(work),
			})
			deliveryCount++
		}
	}
	messages = append(messages, ports.OutboundKafkaMessage{
		Topic: contracts.TopicCampaignProgress,
		Key:   []byte(campaign.ID().String()),
		Value: contracts.BatchedSourceBatchFanoutCompletedV1{
			MessageHeaderV1: header,
			Type:            contracts.CampaignProgressEventTypeSourceBatchFannedOut,
			CampaignID:      campaign.ID(),
			SourceBatchID:   batch.ID(),
			DeliveryCount:   uint64(deliveryCount),
		},
	})

	return s.kafkaConsumer.Complete(ctx, messages)
}
