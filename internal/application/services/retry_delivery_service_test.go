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

func TestRetryDeliveryServiceProcessPublishesTerminalResult(t *testing.T) {
	t.Parallel()

	fixture := newRetryDeliveryServiceFixture(t, time.Now().Add(-time.Second), ports.PushSendOutcomeAccepted)
	if err := fixture.service.Process(context.Background()); err != nil {
		t.Fatalf("process retry delivery: %v", err)
	}
	if fixture.limiter.permits != 1 || fixture.sender.calls != 1 {
		t.Fatalf("unexpected rate/send calls: permits=%d calls=%d", fixture.limiter.permits, fixture.sender.calls)
	}
	if fixture.pipeline.pollCalls != 1 || fixture.pipeline.pollManyCalls != 0 {
		t.Fatalf("retry processing must consume one record: poll=%d poll_many=%d", fixture.pipeline.pollCalls, fixture.pipeline.pollManyCalls)
	}
	if len(fixture.pipeline.transaction.messages) != 2 {
		t.Fatalf("expected notification and progress messages, got %d", len(fixture.pipeline.transaction.messages))
	}
	if _, ok := fixture.pipeline.transaction.messages[1].Value.(contracts.NotificationAcceptedV1); !ok {
		t.Fatalf("expected accepted notification, got %T", fixture.pipeline.transaction.messages[1].Value)
	}
	delta, ok := fixture.pipeline.transaction.messages[0].Value.(contracts.CampaignProgressDeltaV1)
	if !ok || delta.DeliveryAcceptedDelta != 1 || delta.DeliveryFailedDelta != 0 {
		t.Fatalf("unexpected progress delta: %#v", fixture.pipeline.transaction.messages[1].Value)
	}
}

func TestRetryDeliveryServiceProcessSchedulesNextRetryDirectly(t *testing.T) {
	t.Parallel()

	fixture := newRetryDeliveryServiceFixture(t, time.Now().Add(-time.Second), ports.PushSendOutcomeRetryable)
	if err := fixture.service.Process(context.Background()); err != nil {
		t.Fatalf("process retry delivery: %v", err)
	}
	if len(fixture.pipeline.transaction.messages) != 1 {
		t.Fatalf("expected next retry, got %d messages", len(fixture.pipeline.transaction.messages))
	}
	retry, ok := fixture.pipeline.transaction.messages[0].Value.(contracts.RetryWorkV1)
	if !ok || retry.RetryAttempt != 2 {
		t.Fatalf("unexpected next retry: %#v", fixture.pipeline.transaction.messages[0].Value)
	}
	expectedTopic, err := contracts.RetryTopic(contracts.RetryBucketFiveMinutesV1, fixture.channel.ID())
	if err != nil {
		t.Fatalf("retry topic: %v", err)
	}
	if fixture.pipeline.transaction.messages[0].Topic != expectedTopic {
		t.Fatalf("unexpected retry topic %q", fixture.pipeline.transaction.messages[0].Topic)
	}
}

func TestRetryDeliveryServiceProcessDefersFuturePartition(t *testing.T) {
	t.Parallel()

	dueAt := time.Now().Add(time.Minute)
	fixture := newRetryDeliveryServiceFixture(t, dueAt, ports.PushSendOutcomeAccepted)
	if err := fixture.service.Process(context.Background()); err != nil {
		t.Fatalf("process retry delivery: %v", err)
	}
	if fixture.sender.calls != 0 || fixture.limiter.permits != 0 || len(fixture.pipeline.transaction.messages) != 0 || len(fixture.pipeline.transaction.offsets) != 0 {
		t.Fatal("future retry must not send, publish, reserve permits, or commit")
	}
	partition := ports.KafkaPartition{Topic: fixture.record.Offset.Topic, Partition: fixture.record.Offset.Partition}
	if len(fixture.controller.seekOffsets) != 1 || fixture.controller.seekOffsets[partition] != fixture.record.Offset.Offset {
		t.Fatalf("unexpected seek offsets: %#v", fixture.controller.seekOffsets)
	}
	if len(fixture.controller.paused) != 1 || fixture.controller.paused[0] != partition {
		t.Fatalf("unexpected paused partitions: %#v", fixture.controller.paused)
	}
}

func TestRetryDeliveryServiceProcessRewindsAndPausesWhenRateLimited(t *testing.T) {
	t.Parallel()

	fixture := newRetryDeliveryServiceFixture(t, time.Now().Add(-time.Second), ports.PushSendOutcomeAccepted)
	fixture.limiter.reservation = ports.DeliveryRateLimitReservation{AvailableAt: time.Now().Add(time.Minute)}
	if err := fixture.service.Process(context.Background()); err != nil {
		t.Fatalf("process retry delivery: %v", err)
	}
	if fixture.sender.calls != 0 || len(fixture.pipeline.transaction.messages) != 0 || len(fixture.pipeline.transaction.offsets) != 0 {
		t.Fatal("rate-limited retry must not send, publish, or commit")
	}
	partition := ports.KafkaPartition{Topic: fixture.record.Offset.Topic, Partition: fixture.record.Offset.Partition}
	if len(fixture.controller.seekOffsets) != 1 || fixture.controller.seekOffsets[partition] != fixture.record.Offset.Offset {
		t.Fatalf("unexpected seek offsets: %#v", fixture.controller.seekOffsets)
	}
}

type retryDeliveryServiceFixture struct {
	service    *RetryDeliveryService
	channel    *domain.Channel
	record     ports.KafkaRecord
	pipeline   *retryDeliveryKafkaPipeline
	controller *retryDeliveryKafkaPipeline
	limiter    *deliveryRateLimiter
	sender     *deliveryPushSender
}

func newRetryDeliveryServiceFixture(t *testing.T, dueAt time.Time, outcome ports.PushSendOutcome) retryDeliveryServiceFixture {
	t.Helper()

	tenantID := uuid.NewV7()
	provider, err := domain.NewProvider(domain.NewProviderParams{
		TenantID: tenantID, Type: domain.ProviderTypeFCM, EncryptedCredentials: "encrypted-credentials", RateLimitQPS: 100, RateLimitBurst: 100,
	})
	if err != nil {
		t.Fatalf("new provider: %v", err)
	}
	tenant, err := domain.NewTenant(domain.NewTenantParams{Name: "tenant", RateLimitPerMinute: 1000})
	if err != nil {
		t.Fatalf("new tenant: %v", err)
	}
	channel, err := domain.NewChannel(domain.NewChannelParams{TenantID: tenantID, Provider: provider, Type: domain.ChannelTypeMobilePush, Key: "customer"})
	if err != nil {
		t.Fatalf("new channel: %v", err)
	}
	if err := channel.Activate(); err != nil {
		t.Fatalf("activate channel: %v", err)
	}
	payload, err := domain.NewPushPayload("Title", "Body", "", nil)
	if err != nil {
		t.Fatalf("new payload: %v", err)
	}
	campaign, err := domain.NewBatchedCampaign(domain.NewBatchedCampaignParams{TenantID: tenantID, ChannelID: channel.ID(), PushPayload: payload, Priority: domain.PriorityNormal})
	if err != nil {
		t.Fatalf("new campaign: %v", err)
	}
	installationID := uuid.NewV7()
	topic, err := contracts.RetryTopic(contracts.RetryBucketOneMinuteV1, channel.ID())
	if err != nil {
		t.Fatalf("retry topic: %v", err)
	}
	record := ports.KafkaRecord{
		Value: contracts.RetryWorkV1{
			DeliveryWorkV1: contracts.DeliveryWorkV1{MessageHeaderV1: contracts.NewMessageHeaderV1(), DeliveryID: uuid.NewV7(), NotificationID: uuid.NewV7(), CampaignID: campaign.ID(), TenantID: tenantID, UserID: "user-1", ChannelID: channel.ID(), PushInstallationID: installationID, Priority: string(domain.PriorityNormal), RetryAttempt: 1, NotificationCreatedAt: time.Now().UTC()},
			DueAt:          dueAt,
		},
		Offset: ports.KafkaOffset{Topic: topic, Partition: 3, Offset: 11},
	}
	pipeline := &retryDeliveryKafkaPipeline{records: []ports.KafkaRecord{record}}
	limiter := &deliveryRateLimiter{reservation: ports.DeliveryRateLimitReservation{Granted: true}}
	sender := &deliveryPushSender{result: ports.PushSendResult{Outcome: outcome}}
	service, err := NewRetryDeliveryService(RetryDeliveryServiceParams{
		ChannelID:                  channel.ID(),
		Bucket:                     contracts.RetryBucketOneMinuteV1,
		CampaignRepository:         &campaignRepositoryFake{campaign: campaign},
		ChannelRepository:          &deliveryChannelRepository{channel: channel},
		TenantRepository:           &deliveryTenantRepository{tenant: tenant},
		ProviderRepository:         &deliveryProviderRepository{provider: provider},
		PushInstallationRepository: &deliveryPushInstallationRepository{tokens: map[domain.PushInstallationID]string{installationID: "token"}},
		RateLimiter:                limiter,
		CallSemaphore:              deliverySemaphore{},
		PushSender:                 sender,
		KafkaConsumer:              pipeline,
	})
	if err != nil {
		t.Fatalf("new retry delivery service: %v", err)
	}
	return retryDeliveryServiceFixture{service: service, channel: channel, record: record, pipeline: pipeline, controller: pipeline, limiter: limiter, sender: sender}
}

type retryDeliveryKafkaPipeline struct {
	retryPartitionController
	records       []ports.KafkaRecord
	pollCalls     int
	pollManyCalls int
	transaction   deliveryKafkaTransaction
}

func (l *retryDeliveryKafkaPipeline) Poll(context.Context) (ports.KafkaRecord, bool, error) {
	l.pollCalls++
	if len(l.records) == 0 {
		return ports.KafkaRecord{}, false, nil
	}
	return l.records[0], true, nil
}

func (l *retryDeliveryKafkaPipeline) PollMany(context.Context, int) ([]ports.KafkaRecord, error) {
	l.pollManyCalls++
	return l.records, nil
}

func (l *retryDeliveryKafkaPipeline) Complete(_ context.Context, messages []ports.OutboundKafkaMessage) error {
	l.transaction.messages = append([]ports.OutboundKafkaMessage(nil), messages...)
	return nil
}

type retryPartitionController struct {
	seekOffsets ports.KafkaPartitionOffsets
	paused      []ports.KafkaPartition
	resumed     []ports.KafkaPartition
}

func (*retryPartitionController) WaitForAssignment(context.Context) ([]ports.KafkaPartition, error) {
	return nil, nil
}

func (*retryPartitionController) PauseAll(context.Context) error { return nil }

func (*retryPartitionController) ResumeAll(context.Context) error { return nil }

func (c *retryPartitionController) Pause(_ context.Context, partitions []ports.KafkaPartition) error {
	c.paused = append(c.paused, partitions...)
	return nil
}

func (c *retryPartitionController) Resume(_ context.Context, partitions []ports.KafkaPartition) error {
	c.resumed = append(c.resumed, partitions...)
	return nil
}

func (c *retryPartitionController) Seek(_ context.Context, offsets ports.KafkaPartitionOffsets) error {
	c.seekOffsets = offsets
	return nil
}
