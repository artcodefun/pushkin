package services

import (
	"context"
	"errors"
	"testing"
	"uuid"

	"github.com/superman/pushkin/internal/application"
	"github.com/superman/pushkin/internal/application/ports"
	contracts "github.com/superman/pushkin/internal/contracts/kafka"
	"github.com/superman/pushkin/internal/domain"
)

func TestCampaignProgressAggregatorServiceProcessRequiresRestore(t *testing.T) {
	t.Parallel()

	service := NewCampaignProgressAggregatorService(CampaignProgressAggregatorServiceParams{
		KafkaConsumer:        &campaignProgressPipeline{},
		CompactedTopicLoader: &campaignStatsSnapshotLoader{},
		BatchSize:            1,
	})

	err := service.Process(context.Background())
	if !errors.Is(err, application.ErrConflict) {
		t.Fatalf("process without restore error = %v, want conflict", err)
	}
}

func TestApplyCampaignProgressMessageRejectsDuplicateRunStarted(t *testing.T) {
	t.Parallel()

	campaignID := uuid.NewV7()
	message := contracts.CampaignRunStartedV1{
		MessageHeaderV1:    contracts.NewMessageHeaderV1(),
		Type:               contracts.CampaignProgressEventTypeRunStarted,
		CampaignID:         campaignID,
		SourceBatchesTotal: 1,
	}
	states := make(map[domain.CampaignID]*domain.CampaignProgress)
	if _, err := applyCampaignProgressMessage(states, message); err != nil {
		t.Fatalf("apply first run marker: %v", err)
	}
	if _, err := applyCampaignProgressMessage(states, message); !errors.Is(err, application.ErrConflict) {
		t.Fatalf("apply duplicate run marker error = %v, want conflict", err)
	}
}

func TestCampaignProgressAggregatorServiceProcessPublishesCompletedSnapshot(t *testing.T) {
	t.Parallel()

	campaignID := uuid.NewV7()
	pipeline := &campaignProgressPipeline{records: []ports.KafkaRecord{
		newCampaignProgressRecord(contracts.CampaignRunStartedV1{
			MessageHeaderV1:    contracts.NewMessageHeaderV1(),
			Type:               contracts.CampaignProgressEventTypeRunStarted,
			CampaignID:         campaignID,
			SourceBatchesTotal: 1,
		}, 10),
		newCampaignProgressRecord(contracts.BatchedSourceBatchFanoutCompletedV1{
			MessageHeaderV1: contracts.NewMessageHeaderV1(),
			Type:            contracts.CampaignProgressEventTypeSourceBatchFannedOut,
			CampaignID:      campaignID,
			SourceBatchID:   uuid.NewV7(),
			DeliveryCount:   2,
		}, 11),
		newCampaignProgressRecord(contracts.CampaignProgressDeltaV1{
			MessageHeaderV1:       contracts.NewMessageHeaderV1(),
			Type:                  contracts.CampaignProgressEventTypeDeliveryDelta,
			CampaignID:            campaignID,
			DeliveryAcceptedDelta: 1,
			DeliveryFailedDelta:   1,
		}, 12),
	}}
	loader := &campaignStatsSnapshotLoader{}
	service := NewCampaignProgressAggregatorService(CampaignProgressAggregatorServiceParams{
		KafkaConsumer:        pipeline,
		CompactedTopicLoader: loader,
		BatchSize:            100,
	})

	if err := service.Restore(context.Background()); err != nil {
		t.Fatalf("restore campaign stats: %v", err)
	}
	if err := service.Process(context.Background()); err != nil {
		t.Fatalf("process campaign progress: %v", err)
	}
	if loader.calls != 1 || loader.topic != contracts.TopicCampaignStats || len(loader.partitions) != 1 || loader.partitions[0] != 1 ||
		pipeline.polledTopic != contracts.TopicCampaignProgress || pipeline.pollLimit != 100 {
		t.Fatalf("unexpected restore/poll behavior: loader=%+v topic=%q limit=%d", loader, pipeline.polledTopic, pipeline.pollLimit)
	}
	if len(pipeline.transaction.messages) != 1 {
		t.Fatalf("expected one stats snapshot, got %d", len(pipeline.transaction.messages))
	}
	snapshot, ok := pipeline.transaction.messages[0].Value.(contracts.CampaignStatsSnapshotV1)
	if !ok {
		t.Fatalf("stats message has type %T", pipeline.transaction.messages[0].Value)
	}
	if pipeline.transaction.messages[0].Topic != contracts.TopicCampaignStats ||
		snapshot.CampaignID != campaignID ||
		snapshot.SourceBatchesTotal != 1 || snapshot.SourceBatchesFannedOut != 1 ||
		snapshot.DeliveryTotal != 2 || snapshot.DeliveryProcessed != 2 ||
		snapshot.DeliveryAcceptedCount != 1 || snapshot.DeliveryFailedCount != 1 {
		t.Fatalf("unexpected stats snapshot: %+v", snapshot)
	}
}

func TestCampaignProgressAggregatorServiceRestoreUsesLastSnapshotInLogOrder(t *testing.T) {
	t.Parallel()

	campaignID := uuid.NewV7()
	loader := &campaignStatsSnapshotLoader{snapshots: []contracts.CampaignStatsSnapshotV1{
		{
			MessageHeaderV1:    contracts.NewMessageHeaderV1(),
			CampaignID:         campaignID,
			SourceBatchesTotal: 1,
		},
		{
			MessageHeaderV1:        contracts.NewMessageHeaderV1(),
			CampaignID:             campaignID,
			SourceBatchesTotal:     1,
			SourceBatchesFannedOut: 1,
			DeliveryTotal:          1,
			DeliveryProcessed:      1,
			DeliveryAcceptedCount:  1,
		},
	}}
	service := NewCampaignProgressAggregatorService(CampaignProgressAggregatorServiceParams{
		KafkaConsumer:        &campaignProgressPipeline{},
		CompactedTopicLoader: loader,
		BatchSize:            1,
	})

	if err := service.Restore(context.Background()); err != nil {
		t.Fatalf("restore campaign stats: %v", err)
	}
	if loader.calls != 1 {
		t.Fatalf("loader calls = %d, want 1", loader.calls)
	}
	if _, found := service.progressByID[campaignID]; found {
		t.Fatal("completed progress must not be restored into the active state map")
	}
}

func TestCampaignProgressAggregatorServiceProcessCopiesOnlyTouchedStateAndRemovesCompleted(t *testing.T) {
	t.Parallel()

	activeCampaignID := uuid.NewV7()
	completedCampaignID := uuid.NewV7()
	active := domain.NewEmptyCampaignProgress(2)
	pipeline := &campaignProgressPipeline{records: []ports.KafkaRecord{
		newCampaignProgressRecord(contracts.BatchedSourceBatchFanoutCompletedV1{
			MessageHeaderV1: contracts.NewMessageHeaderV1(),
			Type:            contracts.CampaignProgressEventTypeSourceBatchFannedOut,
			CampaignID:      activeCampaignID,
			SourceBatchID:   uuid.NewV7(),
			DeliveryCount:   1,
		}, 10),
		newCampaignProgressRecord(contracts.CampaignRunStartedV1{
			MessageHeaderV1:    contracts.NewMessageHeaderV1(),
			Type:               contracts.CampaignProgressEventTypeRunStarted,
			CampaignID:         completedCampaignID,
			SourceBatchesTotal: 1,
		}, 11),
		newCampaignProgressRecord(contracts.InlineCampaignFanoutCompletedV1{
			MessageHeaderV1: contracts.NewMessageHeaderV1(),
			Type:            contracts.CampaignProgressEventTypeSourceBatchFannedOut,
			CampaignID:      completedCampaignID,
			DeliveryCount:   0,
		}, 12),
	}}
	service := NewCampaignProgressAggregatorService(CampaignProgressAggregatorServiceParams{
		KafkaConsumer:        pipeline,
		CompactedTopicLoader: &campaignStatsSnapshotLoader{},
		BatchSize:            100,
	})
	service.restored = true
	service.progressByID[activeCampaignID] = active

	if err := service.Process(context.Background()); err != nil {
		t.Fatalf("process campaign progress: %v", err)
	}
	updated := service.progressByID[activeCampaignID]
	if updated == nil || updated == active || updated.SourceBatchesFannedOut() != 1 {
		t.Fatalf("active state was not copy-on-write updated: updated=%+v original=%+v", updated, active)
	}
	if active.SourceBatchesFannedOut() != 0 {
		t.Fatalf("original active state was mutated: %+v", active)
	}
	if _, found := service.progressByID[completedCampaignID]; found {
		t.Fatal("completed campaign progress must be removed from active state")
	}
	if len(pipeline.transaction.messages) != 2 {
		t.Fatalf("stats snapshots = %d, want 2", len(pipeline.transaction.messages))
	}
}

func TestCampaignProgressAggregatorServiceProcessDoesNotInstallChangesWhenKafkaTransactionFails(t *testing.T) {
	t.Parallel()

	campaignID := uuid.NewV7()
	current := domain.NewEmptyCampaignProgress(1)
	pipeline := &campaignProgressPipeline{
		records: []ports.KafkaRecord{newCampaignProgressRecord(contracts.BatchedSourceBatchFanoutCompletedV1{
			MessageHeaderV1: contracts.NewMessageHeaderV1(),
			Type:            contracts.CampaignProgressEventTypeSourceBatchFannedOut,
			CampaignID:      campaignID,
			SourceBatchID:   uuid.NewV7(),
			DeliveryCount:   1,
		}, 10)},
		completeErr: errors.New("Kafka transaction failed"),
	}
	service := NewCampaignProgressAggregatorService(CampaignProgressAggregatorServiceParams{
		KafkaConsumer:        pipeline,
		CompactedTopicLoader: &campaignStatsSnapshotLoader{},
		BatchSize:            1,
	})
	service.restored = true
	service.progressByID[campaignID] = current

	if err := service.Process(context.Background()); !errors.Is(err, pipeline.completeErr) {
		t.Fatalf("process error = %v, want %v", err, pipeline.completeErr)
	}
	if service.progressByID[campaignID] != current || current.SourceBatchesFannedOut() != 0 {
		t.Fatalf("failed Kafka transaction changed active state: %+v", service.progressByID[campaignID])
	}
}

func TestCampaignProgressAggregatorServiceRestoreReplacesState(t *testing.T) {
	t.Parallel()

	firstCampaignID := uuid.NewV7()
	secondCampaignID := uuid.NewV7()
	loader := &campaignStatsSnapshotLoader{snapshots: []contracts.CampaignStatsSnapshotV1{{
		MessageHeaderV1:    contracts.NewMessageHeaderV1(),
		CampaignID:         firstCampaignID,
		SourceBatchesTotal: 1,
	}}}
	service := NewCampaignProgressAggregatorService(CampaignProgressAggregatorServiceParams{
		KafkaConsumer:        &campaignProgressPipeline{},
		CompactedTopicLoader: loader,
		BatchSize:            1,
	})

	if err := service.Restore(context.Background()); err != nil {
		t.Fatalf("restore initial campaign stats: %v", err)
	}
	loader.snapshots = []contracts.CampaignStatsSnapshotV1{{
		MessageHeaderV1:    contracts.NewMessageHeaderV1(),
		CampaignID:         secondCampaignID,
		SourceBatchesTotal: 2,
	}}
	if err := service.Restore(context.Background()); err != nil {
		t.Fatalf("restore reassigned campaign stats: %v", err)
	}
	if loader.calls != 2 {
		t.Fatalf("loader calls = %d, want 2", loader.calls)
	}
	if _, found := service.progressByID[firstCampaignID]; found {
		t.Fatal("previous assignment state must be removed")
	}
	if progress := service.progressByID[secondCampaignID]; progress == nil || progress.SourceBatchesTotal() != 2 {
		t.Fatalf("unexpected restored progress: %v", progress)
	}
}

func newCampaignProgressRecord(
	value contracts.CampaignProgressMessageV1,
	offset int64,
) ports.KafkaRecord {
	return ports.KafkaRecord{
		Value:  value,
		Offset: ports.KafkaOffset{Topic: contracts.TopicCampaignProgress, Partition: 1, Offset: offset},
	}
}

type campaignStatsSnapshotLoader struct {
	snapshots  []contracts.CampaignStatsSnapshotV1
	calls      int
	topic      contracts.Topic
	partitions []int32
}

func (l *campaignStatsSnapshotLoader) Load(
	_ context.Context,
	topic contracts.Topic,
	partitions []int32,
) ([]ports.KafkaRecord, error) {
	l.calls++
	l.topic = topic
	l.partitions = append([]int32(nil), partitions...)
	records := make([]ports.KafkaRecord, 0, len(l.snapshots))
	for index, snapshot := range l.snapshots {
		records = append(records, ports.KafkaRecord{
			Value:  snapshot,
			Offset: ports.KafkaOffset{Topic: topic, Partition: partitions[0], Offset: int64(index)},
		})
	}
	return records, nil
}

type campaignProgressPipeline struct {
	kafkaPipelineControlFake
	records     []ports.KafkaRecord
	polledTopic contracts.Topic
	pollLimit   int
	transaction campaignProgressTransaction
	completeErr error
}

func (*campaignProgressPipeline) WaitForAssignment(context.Context) ([]ports.KafkaPartition, error) {
	return []ports.KafkaPartition{{Topic: contracts.TopicCampaignProgress, Partition: 1}}, nil
}

func (l *campaignProgressPipeline) Poll(context.Context) (ports.KafkaRecord, bool, error) {
	return ports.KafkaRecord{}, false, nil
}

func (l *campaignProgressPipeline) PollMany(_ context.Context, limit int) ([]ports.KafkaRecord, error) {
	l.polledTopic = contracts.TopicCampaignProgress
	l.pollLimit = limit
	records := append([]ports.KafkaRecord(nil), l.records...)
	l.records = nil
	return records, nil
}

func (l *campaignProgressPipeline) Complete(_ context.Context, messages []ports.OutboundKafkaMessage) error {
	l.transaction.messages = append([]ports.OutboundKafkaMessage(nil), messages...)
	return l.completeErr
}

type campaignProgressTransaction struct {
	messages         []ports.OutboundKafkaMessage
	committedOffsets ports.KafkaPartitionOffsets
}

func (t *campaignProgressTransaction) Produce(_ context.Context, messages []ports.OutboundKafkaMessage) error {
	t.messages = append([]ports.OutboundKafkaMessage(nil), messages...)
	return nil
}
