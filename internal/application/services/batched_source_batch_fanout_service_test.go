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

func TestBatchedSourceBatchFanoutServiceProcessPublishesDeliveryWorkAndCompletion(t *testing.T) {
	t.Parallel()

	campaign, _ := newCoordinatorCampaign(t)
	batch, err := domain.NewSourceBatch(campaign.ID(), []domain.UserID{"user-1", "user-2"})
	if err != nil {
		t.Fatalf("new source batch: %v", err)
	}
	installationIDs := []domain.PushInstallationID{uuid.NewV7(), uuid.NewV7(), uuid.NewV7()}
	pipeline := &batchedSourceBatchFanoutPipeline{record: ports.KafkaRecord{
		Value: contracts.BatchedSourceBatchFanoutV1{
			MessageHeaderV1: contracts.NewMessageHeaderV1(),
			SourceBatchID:   batch.ID(),
		},
		Offset: ports.KafkaOffset{Topic: contracts.TopicCampaignBatchedSourceBatchFanout, Partition: 2, Offset: 41},
	}}
	mobileApplicationIDs := []domain.MobileApplicationID{uuid.NewV7(), uuid.NewV7()}
	applications := &mobileApplicationRepositoryFake{mobileApplicationIDs: mobileApplicationIDs}
	installations := &pushInstallationRepositoryFake{installationIDs: installationIDs}
	service := NewBatchedSourceBatchFanoutService(BatchedSourceBatchFanoutServiceParams{
		CampaignRepository:          &campaignRepositoryFake{campaign: campaign},
		SourceBatchRepository:       &batchedSourceBatchRepositoryFake{batch: batch},
		MobileApplicationRepository: applications,
		PushInstallationRepository:  installations,
		KafkaConsumer:               pipeline,
	})

	if err := service.Process(context.Background()); err != nil {
		t.Fatalf("process source fanout: %v", err)
	}
	if pipeline.polledTopic != contracts.TopicCampaignBatchedSourceBatchFanout {
		t.Fatalf("unexpected polled topic: %q", pipeline.polledTopic)
	}
	if applications.channelID != campaign.ChannelID() {
		t.Fatalf("unexpected mobile application channel: %s", applications.channelID)
	}
	if installations.tenantID != campaign.TenantID() ||
		len(installations.mobileApplicationIDs) != len(mobileApplicationIDs) {
		t.Fatalf("unexpected push installation scope: tenant=%s applications=%v", installations.tenantID, installations.mobileApplicationIDs)
	}
	if len(pipeline.transaction.messages) != len(installationIDs)+1 {
		t.Fatalf("expected %d messages, got %d", len(installationIDs)+1, len(pipeline.transaction.messages))
	}
	deliveryTopic, err := contracts.DeliveryTopic(contracts.PriorityV1(campaign.Priority()), campaign.ChannelID())
	if err != nil {
		t.Fatalf("delivery topic: %v", err)
	}
	for index, message := range pipeline.transaction.messages[:len(installationIDs)] {
		if message.Topic != deliveryTopic {
			t.Fatalf("delivery %d has topic %q, want %q", index, message.Topic, deliveryTopic)
		}
		work, ok := message.Value.(contracts.DeliveryWorkV1)
		if !ok {
			t.Fatalf("delivery %d has type %T", index, message.Value)
		}
		if work.DeliveryID == uuid.Nil() || work.CampaignID != campaign.ID() ||
			work.PushInstallationID != installationIDs[index] || work.Priority != string(campaign.Priority()) {
			t.Fatalf("unexpected delivery work: %+v", work)
		}
	}
	completed, ok := pipeline.transaction.messages[len(installationIDs)].Value.(contracts.BatchedSourceBatchFanoutCompletedV1)
	if !ok {
		t.Fatalf("completion has type %T", pipeline.transaction.messages[len(installationIDs)].Value)
	}
	if completed.CampaignID != campaign.ID() || completed.SourceBatchID != batch.ID() ||
		completed.DeliveryCount != uint64(len(installationIDs)) {
		t.Fatalf("unexpected fanout completion: %+v", completed)
	}
}

type campaignRepositoryFake struct{ campaign *domain.Campaign }

func (r *campaignRepositoryFake) Create(context.Context, *domain.Campaign) error { return nil }
func (r *campaignRepositoryFake) FindByID(context.Context, domain.CampaignID) (*domain.Campaign, error) {
	return r.campaign, nil
}
func (r *campaignRepositoryFake) FindByIDs(context.Context, []domain.CampaignID) (map[domain.CampaignID]*domain.Campaign, error) {
	return map[domain.CampaignID]*domain.Campaign{r.campaign.ID(): r.campaign}, nil
}
func (r *campaignRepositoryFake) FindByIDsForUpdate(context.Context, []domain.CampaignID) (map[domain.CampaignID]*domain.Campaign, error) {
	return map[domain.CampaignID]*domain.Campaign{r.campaign.ID(): r.campaign}, nil
}
func (r *campaignRepositoryFake) FindStartCandidatesForUpdate(context.Context, time.Time, time.Time, int) ([]*domain.Campaign, error) {
	return nil, nil
}
func (r *campaignRepositoryFake) FindByIDForUpdate(context.Context, domain.CampaignID) (*domain.Campaign, error) {
	return r.campaign, nil
}
func (r *campaignRepositoryFake) Update(context.Context, *domain.Campaign) error { return nil }
func (r *campaignRepositoryFake) UpdateBatch(context.Context, []*domain.Campaign) error {
	return nil
}
func (r *campaignRepositoryFake) Tx(ports.Transaction) ports.CampaignRepository { return r }

type batchedSourceBatchRepositoryFake struct{ batch *domain.SourceBatch }

func (r *batchedSourceBatchRepositoryFake) Create(context.Context, *domain.SourceBatch) error {
	return nil
}
func (r *batchedSourceBatchRepositoryFake) FindByID(context.Context, domain.SourceBatchID) (*domain.SourceBatch, error) {
	return r.batch, nil
}
func (r *batchedSourceBatchRepositoryFake) ListIDsByCampaign(context.Context, domain.CampaignID) ([]domain.SourceBatchID, error) {
	return nil, nil
}
func (r *batchedSourceBatchRepositoryFake) Tx(ports.Transaction) ports.SourceBatchRepository {
	return r
}

type mobileApplicationRepositoryFake struct {
	channelID              domain.ChannelID
	mobileApplicationIDs   []domain.MobileApplicationID
	listIDsByChannelsCalls int
}

func (r *mobileApplicationRepositoryFake) ListIDsByChannel(
	_ context.Context,
	channelID domain.ChannelID,
) ([]domain.MobileApplicationID, error) {
	r.channelID = channelID
	return append([]domain.MobileApplicationID(nil), r.mobileApplicationIDs...), nil
}

func (r *mobileApplicationRepositoryFake) ListIDsByChannels(_ context.Context, channelIDs []domain.ChannelID) (map[domain.ChannelID][]domain.MobileApplicationID, error) {
	r.listIDsByChannelsCalls++
	result := make(map[domain.ChannelID][]domain.MobileApplicationID, len(channelIDs))
	for _, channelID := range channelIDs {
		result[channelID] = r.mobileApplicationIDs
	}
	return result, nil
}

func (r *mobileApplicationRepositoryFake) Create(
	context.Context,
	*domain.MobileApplication,
) error {
	return nil
}

func (r *mobileApplicationRepositoryFake) FindByID(
	context.Context,
	domain.MobileApplicationID,
) (*domain.MobileApplication, error) {
	return nil, nil
}

func (r *mobileApplicationRepositoryFake) FindByPlatformAndPackageName(
	context.Context,
	domain.TenantID,
	domain.MobilePlatform,
	string,
) (*domain.MobileApplication, error) {
	return nil, nil
}

func (r *mobileApplicationRepositoryFake) Update(
	context.Context,
	*domain.MobileApplication,
) error {
	return nil
}

func (r *mobileApplicationRepositoryFake) ListChannelIDs(
	context.Context,
	domain.MobileApplicationID,
) ([]domain.ChannelID, error) {
	return nil, nil
}

type pushInstallationRepositoryFake struct {
	tenantID               domain.TenantID
	mobileApplicationIDs   []domain.MobileApplicationID
	installationIDs        []domain.PushInstallationID
	listActiveMatchesCalls int
}

func (r *pushInstallationRepositoryFake) Deactivate(
	context.Context,
	domain.TenantID,
	domain.PushInstallationID,
) error {
	return nil
}

func (r *pushInstallationRepositoryFake) Upsert(
	context.Context,
	*domain.PushInstallation,
) error {
	return nil
}

func (r *pushInstallationRepositoryFake) ListActiveTokensByIDs(
	context.Context,
	domain.TenantID,
	[]domain.PushInstallationID,
) (map[domain.PushInstallationID]string, error) {
	return nil, nil
}

func (r *pushInstallationRepositoryFake) ListActiveIDs(
	_ context.Context,
	tenantID domain.TenantID,
	_ []domain.UserID,
	mobileApplicationIDs []domain.MobileApplicationID,
) ([]domain.PushInstallationID, error) {
	r.tenantID = tenantID
	r.mobileApplicationIDs = append([]domain.MobileApplicationID(nil), mobileApplicationIDs...)
	return append([]domain.PushInstallationID(nil), r.installationIDs...), nil
}

func (r *pushInstallationRepositoryFake) ListActiveIDsByUsers(_ context.Context, _ domain.TenantID, userIDs []domain.UserID, _ []domain.MobileApplicationID) (map[domain.UserID][]domain.PushInstallationID, error) {
	r.listActiveMatchesCalls++
	result := make(map[domain.UserID][]domain.PushInstallationID, len(userIDs))
	for _, userID := range userIDs {
		result[userID] = append([]domain.PushInstallationID(nil), r.installationIDs...)
	}
	return result, nil
}

type batchedSourceBatchFanoutPipeline struct {
	kafkaPipelineControlFake
	record      ports.KafkaRecord
	polledTopic contracts.Topic
	transaction batchedSourceBatchFanoutTransaction
}

func (l *batchedSourceBatchFanoutPipeline) Poll(_ context.Context) (ports.KafkaRecord, bool, error) {
	l.polledTopic = contracts.TopicCampaignBatchedSourceBatchFanout
	return l.record, true, nil
}
func (l *batchedSourceBatchFanoutPipeline) PollMany(context.Context, int) ([]ports.KafkaRecord, error) {
	return nil, nil
}
func (l *batchedSourceBatchFanoutPipeline) Complete(_ context.Context, messages []ports.OutboundKafkaMessage) error {
	l.transaction.messages = append([]ports.OutboundKafkaMessage(nil), messages...)
	return nil
}

type batchedSourceBatchFanoutTransaction struct {
	messages         []ports.OutboundKafkaMessage
	committedOffsets ports.KafkaPartitionOffsets
}
