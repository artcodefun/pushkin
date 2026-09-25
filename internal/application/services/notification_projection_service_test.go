package services

import (
	"context"
	"testing"
	"time"

	"uuid"

	"github.com/superman/pushkin/internal/application/ports"
	contracts "github.com/superman/pushkin/internal/contracts/kafka"
	"github.com/superman/pushkin/internal/domain"
)

func TestNotificationProjectionServiceProcessStoresNotificationAndCommitsRecord(t *testing.T) {
	t.Parallel()

	tenantID := uuid.NewV7()
	notificationID := uuid.NewV7()
	record := ports.KafkaRecord{
		Value: contracts.NotificationAcceptedV1{
			MessageHeaderV1: contracts.NewMessageHeaderV1(),
			NotificationID:  notificationID,
			CampaignID:      uuid.NewV7(),
			TenantID:        tenantID,
			UserID:          "user-1",
			CreatedAt:       time.Now().UTC(),
			Payload:         contracts.NotificationPayloadV1{Title: "Title", Data: map[string]string{"kind": "test"}},
		},
		Offset: ports.KafkaOffset{Topic: contracts.TopicNotificationAccepted, Partition: 1, Offset: 42},
	}
	notifications := &notificationRepositoryFake{}
	consumer := &notificationConsumerFake{records: []ports.KafkaRecord{record}}
	service := NewNotificationProjectionService(NotificationProjectionServiceParams{
		Notifications: notifications, KafkaConsumer: consumer, BatchSize: 100,
	})

	if err := service.Process(context.Background()); err != nil {
		t.Fatalf("process notification projection: %v", err)
	}
	if len(notifications.notifications) != 1 || notifications.notifications[0].ID() != notificationID ||
		notifications.notifications[0].TenantID() != tenantID || notifications.notifications[0].Payload().Title() != "Title" {
		t.Fatalf("unexpected persisted notifications: %+v", notifications.notifications)
	}
	if len(consumer.committed) != 1 || consumer.committed[0].Offset != record.Offset {
		t.Fatalf("unexpected committed records: %+v", consumer.committed)
	}
}

type notificationRepositoryFake struct{ notifications []domain.Notification }

func (r *notificationRepositoryFake) SaveAcceptedBatch(_ context.Context, notifications []domain.Notification) error {
	r.notifications = notifications
	return nil
}

func (*notificationRepositoryFake) MarkRead(context.Context, domain.TenantID, domain.UserID, domain.NotificationID, time.Time) error {
	return nil
}

type notificationConsumerFake struct {
	records   []ports.KafkaRecord
	committed []ports.KafkaRecord
}

func (*notificationConsumerFake) Poll(context.Context) (ports.KafkaRecord, bool, error) {
	return ports.KafkaRecord{}, false, nil
}

func (c *notificationConsumerFake) PollMany(context.Context, int) ([]ports.KafkaRecord, error) {
	return c.records, nil
}

func (c *notificationConsumerFake) Commit(_ context.Context, records []ports.KafkaRecord) error {
	c.committed = records
	return nil
}

func (*notificationConsumerFake) Pause(context.Context, []ports.KafkaPartition) error {
	return nil
}

func (*notificationConsumerFake) Resume(context.Context, []ports.KafkaPartition) error {
	return nil
}

func (*notificationConsumerFake) Seek(context.Context, ports.KafkaPartitionOffsets) error {
	return nil
}
