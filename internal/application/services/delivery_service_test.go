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

func TestDeliveryServiceProcessPublishesAcceptedResultAndProgress(t *testing.T) {
	t.Parallel()

	fixture := newDeliveryServiceFixture(t, ports.PushSendResult{Outcome: ports.PushSendOutcomeAccepted})
	if err := fixture.service.Process(context.Background()); err != nil {
		t.Fatalf("process delivery: %v", err)
	}
	if fixture.limiter.permits != 1 || fixture.sender.calls != 1 {
		t.Fatalf("unexpected rate/send calls: permits=%d calls=%d", fixture.limiter.permits, fixture.sender.calls)
	}
	if len(fixture.pipeline.transaction.messages) != 1 {
		t.Fatalf("expected progress delta, got %d messages", len(fixture.pipeline.transaction.messages))
	}
	delta, ok := fixture.pipeline.transaction.messages[0].Value.(contracts.CampaignProgressDeltaV1)
	if !ok || delta.DeliveryAcceptedDelta != 1 || delta.DeliveryFailedDelta != 0 {
		t.Fatalf("unexpected progress delta: %#v", fixture.pipeline.transaction.messages[1].Value)
	}
}

func TestDeliveryServiceProcessPublishesRetryWithoutTerminalProgress(t *testing.T) {
	t.Parallel()

	fixture := newDeliveryServiceFixture(t, ports.PushSendResult{Outcome: ports.PushSendOutcomeRetryable})
	if err := fixture.service.Process(context.Background()); err != nil {
		t.Fatalf("process delivery: %v", err)
	}
	if len(fixture.pipeline.transaction.messages) != 1 {
		t.Fatalf("expected retry, got %d messages", len(fixture.pipeline.transaction.messages))
	}
	retry, ok := fixture.pipeline.transaction.messages[0].Value.(contracts.RetryWorkV1)
	if !ok {
		t.Fatalf("expected retry work, got %T", fixture.pipeline.transaction.messages[0].Value)
	}
	if retry.RetryAttempt != 1 || fixture.pipeline.transaction.messages[0].Topic == "" {
		t.Fatalf("unexpected retry work: %#v", retry)
	}
}

func TestDeliveryServiceProcessRewindsAndPausesTopicWithoutSendingOrCommitting(t *testing.T) {
	t.Parallel()

	fixture := newDeliveryServiceFixture(t, ports.PushSendResult{Outcome: ports.PushSendOutcomeAccepted})
	pauseUntil := time.Now().Add(time.Minute)
	fixture.limiter.reservation = ports.DeliveryRateLimitReservation{AvailableAt: pauseUntil}

	if err := fixture.service.Process(context.Background()); err != nil {
		t.Fatalf("process delivery: %v", err)
	}
	if fixture.sender.calls != 0 || len(fixture.pipeline.transaction.messages) != 0 || len(fixture.pipeline.transaction.offsets) != 0 {
		t.Fatal("paused delivery must not send, publish, or commit")
	}
	partition := ports.KafkaPartition{Topic: fixture.record.Offset.Topic, Partition: fixture.record.Offset.Partition}
	if len(fixture.controller.seekOffsets) != 1 || fixture.controller.seekOffsets[partition] != fixture.record.Offset.Offset {
		t.Fatalf("unexpected seek offsets: %#v", fixture.controller.seekOffsets)
	}
	if !fixture.controller.pausedAll {
		t.Fatal("delivery topic was not paused")
	}
}

func TestDeliveryServiceProcessResumesTopicWhenPermitsBecomeAvailable(t *testing.T) {
	t.Parallel()

	fixture := newDeliveryServiceFixture(t, ports.PushSendResult{Outcome: ports.PushSendOutcomeAccepted})
	fixture.limiter.reservation = ports.DeliveryRateLimitReservation{AvailableAt: time.Now().Add(time.Minute)}
	if err := fixture.service.Process(context.Background()); err != nil {
		t.Fatalf("pause delivery: %v", err)
	}

	fixture.limiter.reservation = ports.DeliveryRateLimitReservation{Granted: true}
	fixture.pipeline.records = nil
	fixture.service.pausedUntil = timePtr(time.Now().Add(-time.Second))
	if err := fixture.service.Process(context.Background()); err != nil {
		t.Fatalf("resume delivery: %v", err)
	}
	if !fixture.controller.resumedAll {
		t.Fatal("delivery topic was not resumed")
	}
}

func TestDeliveryRewindOffsetsUsesFirstRecordInEachPartition(t *testing.T) {
	t.Parallel()

	topic := contracts.Topic("pushkin.delivery.normal.test")
	offsets := deliveryRewindOffsets([]ports.KafkaRecord{
		{Offset: ports.KafkaOffset{Topic: topic, Partition: 0, Offset: 13}},
		{Offset: ports.KafkaOffset{Topic: topic, Partition: 1, Offset: 8}},
		{Offset: ports.KafkaOffset{Topic: topic, Partition: 0, Offset: 11}},
	})
	if len(offsets) != 2 || offsets[ports.KafkaPartition{Topic: topic, Partition: 0}] != 11 ||
		offsets[ports.KafkaPartition{Topic: topic, Partition: 1}] != 8 {
		t.Fatalf("unexpected rewind offsets: %#v", offsets)
	}
}

func timePtr(value time.Time) *time.Time { return &value }

type deliveryServiceFixture struct {
	service    *DeliveryService
	record     ports.KafkaRecord
	pipeline   *deliveryKafkaPipeline
	controller *deliveryKafkaPipeline
	limiter    *deliveryRateLimiter
	sender     *deliveryPushSender
}

func newDeliveryServiceFixture(t *testing.T, outcome ports.PushSendResult) deliveryServiceFixture {
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
	topic, err := contracts.DeliveryTopic(contracts.PriorityNormalV1, channel.ID())
	if err != nil {
		t.Fatalf("delivery topic: %v", err)
	}
	record := ports.KafkaRecord{
		Value:  contracts.DeliveryWorkV1{MessageHeaderV1: contracts.NewMessageHeaderV1(), DeliveryID: uuid.NewV7(), CampaignID: campaign.ID(), TenantID: tenantID, ChannelID: channel.ID(), PushInstallationID: installationID, Priority: string(domain.PriorityNormal)},
		Offset: ports.KafkaOffset{Topic: topic, Partition: 3, Offset: 11},
	}
	pipeline := &deliveryKafkaPipeline{records: []ports.KafkaRecord{record}}
	limiter := &deliveryRateLimiter{reservation: ports.DeliveryRateLimitReservation{Granted: true}}
	sender := &deliveryPushSender{result: outcome}
	service, err := NewDeliveryService(DeliveryServiceParams{
		ChannelID: channel.ID(), Priority: domain.PriorityNormal, BatchSize: 100,
		CampaignRepository:         &campaignRepositoryFake{campaign: campaign},
		ChannelRepository:          &deliveryChannelRepository{channel: channel},
		TenantRepository:           &deliveryTenantRepository{tenant: tenant},
		ProviderRepository:         &deliveryProviderRepository{provider: provider},
		PushInstallationRepository: &deliveryPushInstallationRepository{tokens: map[domain.PushInstallationID]string{installationID: "token"}},
		RateLimiter:                limiter, CallSemaphore: deliverySemaphore{}, PushSender: sender, KafkaConsumer: pipeline,
	})
	if err != nil {
		t.Fatalf("new delivery service: %v", err)
	}
	return deliveryServiceFixture{service: service, record: record, pipeline: pipeline, controller: pipeline, limiter: limiter, sender: sender}
}

type deliveryChannelRepository struct{ channel *domain.Channel }

type deliveryTenantRepository struct{ tenant *domain.Tenant }

func (r *deliveryTenantRepository) Create(context.Context, *domain.Tenant) error { return nil }

func (r *deliveryTenantRepository) FindByID(context.Context, domain.TenantID) (*domain.Tenant, error) {
	return r.tenant, nil
}

func (r *deliveryChannelRepository) FindByKey(context.Context, domain.TenantID, string) (*domain.Channel, error) {
	return r.channel, nil
}
func (r *deliveryChannelRepository) ListActiveIDs(context.Context) ([]domain.ChannelID, error) {
	if r.channel == nil {
		return nil, nil
	}
	return []domain.ChannelID{r.channel.ID()}, nil
}
func (r *deliveryChannelRepository) FindProvisioningForUpdate(context.Context) (*domain.Channel, error) {
	return nil, ports.ErrNotFound
}
func (r *deliveryChannelRepository) Create(context.Context, *domain.Channel) error { return nil }
func (r *deliveryChannelRepository) FindByID(context.Context, domain.ChannelID) (*domain.Channel, error) {
	return r.channel, nil
}
func (r *deliveryChannelRepository) Update(context.Context, *domain.Channel) error { return nil }
func (r *deliveryChannelRepository) Tx(ports.Transaction) ports.ChannelRepository  { return r }
func (r *deliveryChannelRepository) LinkMobileApplication(context.Context, domain.ChannelID, domain.MobileApplicationID) error {
	return nil
}
func (r *deliveryChannelRepository) UnlinkMobileApplication(context.Context, domain.ChannelID, domain.MobileApplicationID) error {
	return nil
}

type deliveryProviderRepository struct{ provider *domain.Provider }

func (r *deliveryProviderRepository) Create(context.Context, *domain.Provider) error { return nil }
func (r *deliveryProviderRepository) FindByID(context.Context, domain.ProviderID) (*domain.Provider, error) {
	return r.provider, nil
}

type deliveryPushInstallationRepository struct {
	tokens map[domain.PushInstallationID]string
}

func (r *deliveryPushInstallationRepository) Upsert(context.Context, *domain.PushInstallation) error {
	return nil
}
func (r *deliveryPushInstallationRepository) Deactivate(context.Context, domain.TenantID, domain.PushInstallationID) error {
	return nil
}
func (r *deliveryPushInstallationRepository) ListActiveTokensByIDs(context.Context, domain.TenantID, []domain.PushInstallationID) (map[domain.PushInstallationID]string, error) {
	return r.tokens, nil
}
func (r *deliveryPushInstallationRepository) ListActiveIDs(context.Context, domain.TenantID, []domain.UserID, []domain.MobileApplicationID) ([]domain.PushInstallationID, error) {
	return nil, nil
}

func (r *deliveryPushInstallationRepository) ListActiveIDsByUsers(context.Context, domain.TenantID, []domain.UserID, []domain.MobileApplicationID) (map[domain.UserID][]domain.PushInstallationID, error) {
	return nil, nil
}

type deliveryRateLimiter struct {
	permits     int
	reservation ports.DeliveryRateLimitReservation
}

func (l *deliveryRateLimiter) Reserve(_ context.Context, _ *domain.Tenant, _ *domain.Provider, permits int) (ports.DeliveryRateLimitReservation, error) {
	l.permits = permits
	return l.reservation, nil
}

type deliverySemaphore struct{}

func (deliverySemaphore) Acquire(context.Context) error { return nil }
func (deliverySemaphore) Release()                      {}

type deliveryPushSender struct {
	calls  int
	result ports.PushSendResult
}

func (p *deliveryPushSender) Send(context.Context, ports.PushSendRequest) (ports.PushSendResult, error) {
	p.calls++
	return p.result, nil
}

type deliveryKafkaPipeline struct {
	deliveryPartitionController
	records     []ports.KafkaRecord
	transaction deliveryKafkaTransaction
}

func (l *deliveryKafkaPipeline) Poll(context.Context) (ports.KafkaRecord, bool, error) {
	return ports.KafkaRecord{}, false, nil
}
func (l *deliveryKafkaPipeline) PollMany(context.Context, int) ([]ports.KafkaRecord, error) {
	return l.records, nil
}
func (l *deliveryKafkaPipeline) Complete(_ context.Context, messages []ports.OutboundKafkaMessage) error {
	l.transaction.messages = append([]ports.OutboundKafkaMessage(nil), messages...)
	return nil
}

type deliveryKafkaTransaction struct {
	messages []ports.OutboundKafkaMessage
	offsets  ports.KafkaPartitionOffsets
}

type deliveryPartitionController struct {
	seekOffsets ports.KafkaPartitionOffsets
	pausedAll   bool
	resumedAll  bool
}

func (*deliveryPartitionController) WaitForAssignment(context.Context) ([]ports.KafkaPartition, error) {
	return nil, nil
}

func (c *deliveryPartitionController) PauseAll(context.Context) error { c.pausedAll = true; return nil }

func (c *deliveryPartitionController) ResumeAll(context.Context) error {
	c.resumedAll = true
	return nil
}

func (*deliveryPartitionController) Pause(context.Context, []ports.KafkaPartition) error { return nil }

func (*deliveryPartitionController) Resume(context.Context, []ports.KafkaPartition) error { return nil }

func (c *deliveryPartitionController) Seek(_ context.Context, offsets ports.KafkaPartitionOffsets) error {
	c.seekOffsets = offsets
	return nil
}
