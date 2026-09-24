package services

import (
	"context"
	"testing"
	"time"

	"github.com/superman/pushkin/internal/application/ports"
	contracts "github.com/superman/pushkin/internal/contracts/kafka"
	"github.com/superman/pushkin/internal/domain"
)

func TestCampaignStatsProjectionServiceProcessStoresSnapshotAndCompletesCampaign(t *testing.T) {
	t.Parallel()

	campaign, runID := newCoordinatorCampaign(t)
	if _, err := campaign.MarkStarted(runID, time.Now().UTC()); err != nil {
		t.Fatalf("mark campaign started: %v", err)
	}
	campaignRepository := &coordinatorCampaignRepository{campaign: campaign}
	pipeline := &campaignStatsProjectionPipeline{record: ports.KafkaRecord{
		Value: contracts.CampaignStatsSnapshotV1{
			MessageHeaderV1:        contracts.NewMessageHeaderV1(),
			CampaignID:             campaign.ID(),
			SourceBatchesTotal:     1,
			SourceBatchesFannedOut: 1,
			DeliveryTotal:          2,
			DeliveryProcessed:      2,
			DeliveryAcceptedCount:  1,
			DeliveryFailedCount:    1,
		},
		Offset: ports.KafkaOffset{Topic: contracts.TopicCampaignStats, Partition: 4, Offset: 12},
	}}
	service := NewCampaignStatsProjectionService(CampaignStatsProjectionServiceParams{
		CampaignRepository: campaignRepository,
		TransactionManager: coordinatorTransactionManager{},
		KafkaConsumer:      pipeline,
		BatchSize:          100,
	})

	if err := service.Process(context.Background()); err != nil {
		t.Fatalf("process campaign stats: %v", err)
	}
	if pipeline.polledTopic != contracts.TopicCampaignStats {
		t.Fatalf("unexpected polled topic: %q", pipeline.polledTopic)
	}
	progress := campaign.CurrentProgress()
	if progress == nil || progress.DeliveryAcceptedCount() != 1 || !progress.IsComplete() {
		t.Fatalf("unexpected campaign progress: %+v", progress)
	}
	if campaign.Status() != domain.CampaignStatusCompleted || campaignRepository.updateCount != 1 {
		t.Fatalf("campaign was not completed: status=%s updates=%d", campaign.Status(), campaignRepository.updateCount)
	}
	partition := ports.KafkaPartition{Topic: pipeline.record.Offset.Topic, Partition: pipeline.record.Offset.Partition}
	if len(pipeline.transaction.committedOffsets) != 1 || pipeline.transaction.committedOffsets[partition] != 13 {
		t.Fatalf("stats offset was not committed: %+v", pipeline.transaction.committedOffsets)
	}
}

func TestCampaignStatsProjectionServiceProcessSkipsStaleSnapshotForCompletedCampaign(t *testing.T) {
	t.Parallel()

	campaign, runID := newCoordinatorCampaign(t)
	now := time.Now().UTC()
	if _, err := campaign.MarkStarted(runID, now); err != nil {
		t.Fatalf("mark campaign started: %v", err)
	}
	current := domain.NewEmptyCampaignProgress(1)
	if err := current.RecordSourceBatchFanout(2); err != nil {
		t.Fatalf("record fanout: %v", err)
	}
	current.RecordDeliveryResults(2, 0)
	if err := campaign.ApplyCurrentProgress(current, now); err != nil {
		t.Fatalf("complete campaign: %v", err)
	}

	campaignRepository := &coordinatorCampaignRepository{campaign: campaign}
	pipeline := &campaignStatsProjectionPipeline{record: ports.KafkaRecord{
		Value: contracts.CampaignStatsSnapshotV1{
			MessageHeaderV1:        contracts.NewMessageHeaderV1(),
			CampaignID:             campaign.ID(),
			SourceBatchesTotal:     1,
			SourceBatchesFannedOut: 1,
			DeliveryTotal:          2,
			DeliveryProcessed:      1,
			DeliveryAcceptedCount:  1,
		},
		Offset: ports.KafkaOffset{Topic: contracts.TopicCampaignStats, Partition: 4, Offset: 12},
	}}
	service := NewCampaignStatsProjectionService(CampaignStatsProjectionServiceParams{
		CampaignRepository: campaignRepository,
		TransactionManager: coordinatorTransactionManager{},
		KafkaConsumer:      pipeline,
		BatchSize:          100,
	})

	if err := service.Process(context.Background()); err != nil {
		t.Fatalf("process stale campaign stats: %v", err)
	}
	if campaignRepository.updateCount != 0 {
		t.Fatalf("completed campaign update count = %d, want 0", campaignRepository.updateCount)
	}
	partition := ports.KafkaPartition{Topic: pipeline.record.Offset.Topic, Partition: pipeline.record.Offset.Partition}
	if pipeline.transaction.committedOffsets[partition] != 13 {
		t.Fatalf("stats offset was not committed: %+v", pipeline.transaction.committedOffsets)
	}
}

func TestCampaignStatsProjectionServiceProcessSkipsStaleSnapshotForStartedCampaign(t *testing.T) {
	t.Parallel()

	campaign, runID := newCoordinatorCampaign(t)
	now := time.Now().UTC()
	if _, err := campaign.MarkStarted(runID, now); err != nil {
		t.Fatalf("mark campaign started: %v", err)
	}
	current := domain.NewEmptyCampaignProgress(2)
	if err := current.RecordSourceBatchFanout(1); err != nil {
		t.Fatalf("record fanout: %v", err)
	}
	if err := campaign.ApplyCurrentProgress(current, now); err != nil {
		t.Fatalf("store current progress: %v", err)
	}

	campaignRepository := &coordinatorCampaignRepository{campaign: campaign}
	pipeline := &campaignStatsProjectionPipeline{record: ports.KafkaRecord{
		Value: contracts.CampaignStatsSnapshotV1{
			MessageHeaderV1:    contracts.NewMessageHeaderV1(),
			CampaignID:         campaign.ID(),
			SourceBatchesTotal: 2,
		},
		Offset: ports.KafkaOffset{Topic: contracts.TopicCampaignStats, Partition: 4, Offset: 12},
	}}
	service := NewCampaignStatsProjectionService(CampaignStatsProjectionServiceParams{
		CampaignRepository: campaignRepository,
		TransactionManager: coordinatorTransactionManager{},
		KafkaConsumer:      pipeline,
		BatchSize:          100,
	})

	if err := service.Process(context.Background()); err != nil {
		t.Fatalf("process stale campaign stats: %v", err)
	}
	if campaignRepository.updateCount != 0 {
		t.Fatalf("started campaign update count = %d, want 0", campaignRepository.updateCount)
	}
}

func TestCampaignStatsProjectionServiceProcessCoalescesSnapshotsByCampaign(t *testing.T) {
	t.Parallel()

	campaign, runID := newCoordinatorCampaign(t)
	if _, err := campaign.MarkStarted(runID, time.Now().UTC()); err != nil {
		t.Fatalf("mark campaign started: %v", err)
	}
	campaignRepository := &coordinatorCampaignRepository{campaign: campaign}
	pipeline := &campaignStatsProjectionPipeline{records: []ports.KafkaRecord{
		{Value: contracts.CampaignStatsSnapshotV1{MessageHeaderV1: contracts.NewMessageHeaderV1(), CampaignID: campaign.ID(), SourceBatchesTotal: 1}, Offset: ports.KafkaOffset{Topic: contracts.TopicCampaignStats, Partition: 4, Offset: 12}},
		{Value: contracts.CampaignStatsSnapshotV1{MessageHeaderV1: contracts.NewMessageHeaderV1(), CampaignID: campaign.ID(), SourceBatchesTotal: 1, SourceBatchesFannedOut: 1, DeliveryTotal: 1, DeliveryProcessed: 1, DeliveryAcceptedCount: 1}, Offset: ports.KafkaOffset{Topic: contracts.TopicCampaignStats, Partition: 4, Offset: 13}},
	}}
	service := NewCampaignStatsProjectionService(CampaignStatsProjectionServiceParams{
		CampaignRepository: campaignRepository,
		TransactionManager: coordinatorTransactionManager{},
		KafkaConsumer:      pipeline,
		BatchSize:          100,
	})

	if err := service.Process(context.Background()); err != nil {
		t.Fatalf("process campaign stats: %v", err)
	}
	if campaignRepository.updateCount != 1 || campaign.Status() != domain.CampaignStatusCompleted {
		t.Fatalf("campaign update count = %d, status = %s", campaignRepository.updateCount, campaign.Status())
	}
	partition := ports.KafkaPartition{Topic: contracts.TopicCampaignStats, Partition: 4}
	if pipeline.transaction.committedOffsets[partition] != 14 {
		t.Fatalf("committed offset = %v, want 14", pipeline.transaction.committedOffsets)
	}
}

type campaignStatsProjectionPipeline struct {
	kafkaPipelineControlFake
	record      ports.KafkaRecord
	records     []ports.KafkaRecord
	polledTopic contracts.Topic
	transaction campaignStatsProjectionTransaction
}

func (l *campaignStatsProjectionPipeline) Poll(_ context.Context) (ports.KafkaRecord, bool, error) {
	l.polledTopic = contracts.TopicCampaignStats
	return l.record, true, nil
}

func (l *campaignStatsProjectionPipeline) PollMany(_ context.Context, limit int) ([]ports.KafkaRecord, error) {
	l.polledTopic = contracts.TopicCampaignStats
	if len(l.records) != 0 {
		return l.records, nil
	}
	return []ports.KafkaRecord{l.record}, nil
}

func (l *campaignStatsProjectionPipeline) Commit(_ context.Context, records []ports.KafkaRecord) error {
	l.transaction.committedOffsets = ports.KafkaOffsetMap(records...)
	return nil
}

type campaignStatsProjectionTransaction struct {
	committedOffsets ports.KafkaPartitionOffsets
}
