package services

import (
	"context"
	"fmt"
	"strings"

	"uuid"

	"github.com/superman/pushkin/internal/application"
	"github.com/superman/pushkin/internal/application/ports"
	contracts "github.com/superman/pushkin/internal/contracts/kafka"
	"github.com/superman/pushkin/internal/domain"
)

// NotificationProjectionService materializes accepted notifications. The store
// operation is idempotent before the Kafka offset is
// committed, so a crash can safely replay the record.
type NotificationProjectionService struct {
	notifications ports.NotificationRepository
	consumer      ports.KafkaConsumer
	batchSize     int
}

type NotificationProjectionServiceParams struct {
	Notifications ports.NotificationRepository
	KafkaConsumer ports.KafkaConsumer
	BatchSize     int
}

func NewNotificationProjectionService(params NotificationProjectionServiceParams) *NotificationProjectionService {
	return &NotificationProjectionService{notifications: params.Notifications, consumer: params.KafkaConsumer, batchSize: params.BatchSize}
}

func (s *NotificationProjectionService) Process(ctx context.Context) error {
	if s.batchSize <= 0 {
		return fmt.Errorf("notification projection batch size: %w", application.ErrValidation)
	}
	records, err := s.consumer.PollMany(ctx, s.batchSize)
	if err != nil || len(records) == 0 {
		return err
	}
	notifications := make([]domain.Notification, 0, len(records))
	for _, record := range records {
		notification, err := notificationFromAcceptedRecord(record)
		if err != nil {
			return err
		}
		notifications = append(notifications, notification)
	}
	if err := s.notifications.SaveAcceptedBatch(ctx, notifications); err != nil {
		return err
	}
	return s.consumer.Commit(ctx, records)
}

func notificationFromAcceptedRecord(record ports.KafkaRecord) (domain.Notification, error) {
	message, ok := record.Value.(contracts.NotificationAcceptedV1)
	if !ok {
		return domain.Notification{}, fmt.Errorf("notification accepted message %T: %w", record.Value, application.ErrValidation)
	}
	if message.NotificationID == (uuid.UUID{}) || message.CampaignID == (uuid.UUID{}) || message.TenantID == (uuid.UUID{}) ||
		strings.TrimSpace(message.UserID) == "" || message.CreatedAt.IsZero() {
		return domain.Notification{}, fmt.Errorf("notification accepted message: %w", application.ErrValidation)
	}
	payload, err := message.Payload.PushPayload()
	if err != nil {
		return domain.Notification{}, fmt.Errorf("notification payload: %w", err)
	}
	notification, err := domain.HydrateNotification(domain.HydrateNotificationParams{
		ID: message.NotificationID, CampaignID: message.CampaignID, TenantID: message.TenantID,
		UserID: domain.UserID(message.UserID), CreatedAt: message.CreatedAt, Payload: payload,
	})
	if err != nil {
		return domain.Notification{}, fmt.Errorf("hydrate notification: %w", err)
	}
	return notification, nil
}
