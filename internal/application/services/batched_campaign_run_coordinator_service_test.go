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

func TestBatchedCampaignRunCoordinatorServiceProcessPublishesFanOutAndCommitsOffsets(t *testing.T) {
	t.Parallel()

	campaign, runID := newCoordinatorCampaign(t)
	batchIDs := []domain.SourceBatchID{uuid.NewV7(), uuid.NewV7()}
	pipeline := &coordinatorCampaignRunPipeline{
		records: []ports.KafkaRecord{{
			Value: contracts.CampaignRunRequestedV1{
				MessageHeaderV1: contracts.MessageHeaderV1{
					SchemaVersion: contracts.SchemaVersionV1,
				},
				CampaignID: campaign.ID(),
				RunID:      runID,
			},
			Offset: ports.KafkaOffset{
				Topic:     contracts.TopicCampaignBatchedRun,
				Partition: 3,
				Offset:    42,
			},
		}},
	}
	service := NewBatchedCampaignRunCoordinatorService(BatchedCampaignRunCoordinatorServiceParams{
		CampaignRepository:    &coordinatorCampaignRepository{campaign: campaign},
		SourceBatchRepository: &coordinatorSourceBatchRepository{batchIDs: batchIDs},
		TransactionManager:    coordinatorTransactionManager{},
		KafkaConsumer:         pipeline,
	})

	err := service.Process(context.Background())
	if err != nil {
		t.Fatalf("process campaign run: %v", err)
	}
	if pipeline.polledTopic != contracts.TopicCampaignBatchedRun {
		t.Fatalf("unexpected input topic: %q", pipeline.polledTopic)
	}
	if len(pipeline.transaction.messages) != len(batchIDs)+1 {
		t.Fatalf("expected %d outgoing records, got %d", len(batchIDs)+1, len(pipeline.transaction.messages))
	}
	marker, ok := pipeline.transaction.messages[0].Value.(contracts.CampaignRunStartedV1)
	if !ok {
		t.Fatalf("expected CampaignRunStartedV1, got %T", pipeline.transaction.messages[0].Value)
	}
	if marker.Type != contracts.CampaignProgressEventTypeRunStarted ||
		marker.SourceBatchesTotal != uint64(len(batchIDs)) ||
		pipeline.transaction.messages[0].Topic != contracts.TopicCampaignProgress {
		t.Fatalf("unexpected run marker: %+v", marker)
	}
	for _, message := range pipeline.transaction.messages[1:] {
		if message.Topic != contracts.TopicCampaignBatchedSourceBatchFanout {
			t.Fatalf("unexpected fan-out topic: %q", message.Topic)
		}
		if _, ok := message.Value.(contracts.BatchedSourceBatchFanoutV1); !ok {
			t.Fatalf("expected BatchedSourceBatchFanoutV1, got %T", message.Value)
		}
	}
}

func TestBatchedCampaignRunCoordinatorServicePreparesFanoutForCurrentRun(t *testing.T) {
	t.Parallel()

	campaign, runID := newCoordinatorCampaign(t)
	batchIDs := []domain.SourceBatchID{uuid.NewV7(), uuid.NewV7()}
	campaigns := &coordinatorCampaignRepository{campaign: campaign}
	batches := &coordinatorSourceBatchRepository{batchIDs: batchIDs}
	service := NewBatchedCampaignRunCoordinatorService(BatchedCampaignRunCoordinatorServiceParams{
		CampaignRepository:    campaigns,
		SourceBatchRepository: batches,
		TransactionManager:    coordinatorTransactionManager{},
	})

	sourceBatchIDs, err := service.prepareFanout(context.Background(), campaign.ID(), runID)
	if err != nil {
		t.Fatalf("prepare fanout: %v", err)
	}
	if sourceBatchIDs == nil {
		t.Fatal("current run must prepare fan-out")
	}
	if len(sourceBatchIDs) != len(batchIDs) {
		t.Fatalf("unexpected source batch IDs: %v", sourceBatchIDs)
	}
	if campaign.Status() != domain.CampaignStatusStarted {
		t.Fatalf("expected started campaign, got %q", campaign.Status())
	}
	if campaigns.updateCount != 1 {
		t.Fatalf("expected one campaign update, got %d", campaigns.updateCount)
	}
	if batches.campaignID != campaign.ID() {
		t.Fatal("source batches must be loaded for the campaign")
	}
}

func TestBatchedCampaignRunCoordinatorServiceReplaysCurrentRun(t *testing.T) {
	t.Parallel()

	campaign, runID := newCoordinatorCampaign(t)
	batchIDs := []domain.SourceBatchID{uuid.NewV7()}
	campaigns := &coordinatorCampaignRepository{campaign: campaign}
	service := NewBatchedCampaignRunCoordinatorService(BatchedCampaignRunCoordinatorServiceParams{
		CampaignRepository:    campaigns,
		SourceBatchRepository: &coordinatorSourceBatchRepository{batchIDs: batchIDs},
		TransactionManager:    coordinatorTransactionManager{},
	})

	first, err := service.prepareFanout(context.Background(), campaign.ID(), runID)
	if err != nil || first == nil {
		t.Fatalf("first fanout preparation: source_batches=%v err=%v", first, err)
	}
	second, err := service.prepareFanout(context.Background(), campaign.ID(), runID)
	if err != nil || second == nil {
		t.Fatalf("replayed fanout preparation: source_batches=%v err=%v", second, err)
	}
	if campaigns.updateCount != 1 {
		t.Fatalf("replay must not update started campaign: %d updates", campaigns.updateCount)
	}
}

func TestBatchedCampaignRunCoordinatorServicePreparesEmptyFanout(t *testing.T) {
	t.Parallel()

	campaign, runID := newCoordinatorCampaign(t)
	service := NewBatchedCampaignRunCoordinatorService(BatchedCampaignRunCoordinatorServiceParams{
		CampaignRepository:    &coordinatorCampaignRepository{campaign: campaign},
		SourceBatchRepository: &coordinatorSourceBatchRepository{},
		TransactionManager:    coordinatorTransactionManager{},
	})

	sourceBatchIDs, err := service.prepareFanout(context.Background(), campaign.ID(), runID)
	if err != nil {
		t.Fatalf("prepare empty fanout: %v", err)
	}
	if sourceBatchIDs == nil {
		t.Fatal("active empty campaign must return a non-nil source batch list")
	}
	if len(sourceBatchIDs) != 0 {
		t.Fatalf("expected no source batches, got %v", sourceBatchIDs)
	}
}

func TestBatchedCampaignRunCoordinatorServiceIgnoresFencedRun(t *testing.T) {
	t.Parallel()

	campaign, _ := newCoordinatorCampaign(t)
	batches := &coordinatorSourceBatchRepository{batchIDs: []domain.SourceBatchID{uuid.NewV7()}}
	service := NewBatchedCampaignRunCoordinatorService(BatchedCampaignRunCoordinatorServiceParams{
		CampaignRepository:    &coordinatorCampaignRepository{campaign: campaign},
		SourceBatchRepository: batches,
		TransactionManager:    coordinatorTransactionManager{},
	})

	sourceBatchIDs, err := service.prepareFanout(context.Background(), campaign.ID(), uuid.NewV7())
	if err != nil {
		t.Fatalf("coordinate fenced run: %v", err)
	}
	if sourceBatchIDs != nil {
		t.Fatalf("fenced run must be ignored: %v", sourceBatchIDs)
	}
	if batches.called {
		t.Fatal("fenced run must not load source batches")
	}
}

func TestBatchedCampaignRunCoordinatorServiceIgnoresTerminalCampaignRun(t *testing.T) {
	t.Parallel()

	campaign, runID := newCoordinatorCampaign(t)
	now := time.Now().UTC()
	if _, err := campaign.MarkStarted(runID, now); err != nil {
		t.Fatalf("mark campaign started: %v", err)
	}
	if err := campaign.Complete(now); err != nil {
		t.Fatalf("complete campaign: %v", err)
	}
	batches := &coordinatorSourceBatchRepository{batchIDs: []domain.SourceBatchID{uuid.NewV7()}}
	service := NewBatchedCampaignRunCoordinatorService(BatchedCampaignRunCoordinatorServiceParams{
		CampaignRepository:    &coordinatorCampaignRepository{campaign: campaign},
		SourceBatchRepository: batches,
		TransactionManager:    coordinatorTransactionManager{},
	})

	sourceBatchIDs, err := service.prepareFanout(context.Background(), campaign.ID(), runID)
	if err != nil {
		t.Fatalf("coordinate terminal campaign run: %v", err)
	}
	if sourceBatchIDs != nil {
		t.Fatalf("terminal campaign run must be ignored: %v", sourceBatchIDs)
	}
	if batches.called {
		t.Fatal("terminal campaign must not load source batches")
	}
}

type coordinatorCampaignRepository struct {
	campaign    *domain.Campaign
	updateCount int
}

func (r *coordinatorCampaignRepository) Create(context.Context, *domain.Campaign) error { return nil }

func (r *coordinatorCampaignRepository) FindByID(
	context.Context,
	domain.CampaignID,
) (*domain.Campaign, error) {
	return r.campaign, nil
}

func (r *coordinatorCampaignRepository) FindByIDs(
	context.Context,
	[]domain.CampaignID,
) (map[domain.CampaignID]*domain.Campaign, error) {
	return map[domain.CampaignID]*domain.Campaign{r.campaign.ID(): r.campaign}, nil
}

func (r *coordinatorCampaignRepository) FindByIDsForUpdate(
	context.Context,
	[]domain.CampaignID,
) (map[domain.CampaignID]*domain.Campaign, error) {
	return map[domain.CampaignID]*domain.Campaign{r.campaign.ID(): r.campaign}, nil
}

func (r *coordinatorCampaignRepository) FindStartCandidatesForUpdate(
	context.Context,
	time.Time,
	time.Time,
	int,
) ([]*domain.Campaign, error) {
	return nil, nil
}

func (r *coordinatorCampaignRepository) FindByIDForUpdate(
	_ context.Context,
	_ domain.CampaignID,
) (*domain.Campaign, error) {
	return r.campaign, nil
}

func (r *coordinatorCampaignRepository) Update(_ context.Context, _ *domain.Campaign) error {
	r.updateCount++
	return nil
}

func (r *coordinatorCampaignRepository) UpdateBatch(_ context.Context, campaigns []*domain.Campaign) error {
	r.updateCount += len(campaigns)
	return nil
}

func (r *coordinatorCampaignRepository) Tx(ports.Transaction) ports.CampaignRepository { return r }

type coordinatorSourceBatchRepository struct {
	batchIDs   []domain.SourceBatchID
	campaignID domain.CampaignID
	called     bool
}

func (r *coordinatorSourceBatchRepository) Create(context.Context, *domain.SourceBatch) error {
	return nil
}

func (r *coordinatorSourceBatchRepository) FindByID(
	context.Context,
	domain.SourceBatchID,
) (*domain.SourceBatch, error) {
	return nil, nil
}

func (r *coordinatorSourceBatchRepository) ListIDsByCampaign(
	_ context.Context,
	campaignID domain.CampaignID,
) ([]domain.SourceBatchID, error) {
	r.called = true
	r.campaignID = campaignID
	return append([]domain.SourceBatchID(nil), r.batchIDs...), nil
}

func (r *coordinatorSourceBatchRepository) Tx(ports.Transaction) ports.SourceBatchRepository {
	return r
}

type coordinatorTransactionManager struct{}

func (coordinatorTransactionManager) WithTx(_ context.Context, fn func(ports.Transaction) error) error {
	return fn(struct{}{})
}

type coordinatorCampaignRunPipeline struct {
	kafkaPipelineControlFake
	records     []ports.KafkaRecord
	polledTopic contracts.Topic
	transaction coordinatorCampaignRunTransaction
}

func (l *coordinatorCampaignRunPipeline) Poll(_ context.Context) (ports.KafkaRecord, bool, error) {
	l.polledTopic = contracts.TopicCampaignBatchedRun
	if len(l.records) == 0 {
		return ports.KafkaRecord{}, false, nil
	}
	return l.records[0], true, nil
}

func (l *coordinatorCampaignRunPipeline) PollMany(context.Context, int) ([]ports.KafkaRecord, error) {
	return nil, nil
}

func (l *coordinatorCampaignRunPipeline) Complete(_ context.Context, messages []ports.OutboundKafkaMessage) error {
	l.transaction.messages = append([]ports.OutboundKafkaMessage(nil), messages...)
	return nil
}

type coordinatorCampaignRunTransaction struct {
	messages         []ports.OutboundKafkaMessage
	committedOffsets ports.KafkaPartitionOffsets
}

func (t *coordinatorCampaignRunTransaction) Produce(
	_ context.Context,
	messages []ports.OutboundKafkaMessage,
) error {
	t.messages = append([]ports.OutboundKafkaMessage(nil), messages...)
	return nil
}

func newCoordinatorCampaign(t *testing.T) (*domain.Campaign, domain.RunID) {
	t.Helper()
	payload, err := domain.NewPushPayload("Title", "Body", "", nil)
	if err != nil {
		t.Fatalf("new payload: %v", err)
	}
	campaign, err := domain.NewBatchedCampaign(domain.NewBatchedCampaignParams{
		TenantID:    uuid.NewV7(),
		ChannelID:   uuid.NewV7(),
		PushPayload: payload,
		Priority:    domain.PriorityNormal,
	})
	if err != nil {
		t.Fatalf("new campaign: %v", err)
	}
	if err := campaign.RequestStart(); err != nil {
		t.Fatalf("request start: %v", err)
	}
	runID, err := campaign.BeginRun(time.Now().UTC(), time.Minute)
	if err != nil {
		t.Fatalf("begin run: %v", err)
	}
	return campaign, runID
}
