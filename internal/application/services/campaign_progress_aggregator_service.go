package services

import (
	"context"
	"fmt"
	"sort"

	"github.com/superman/pushkin/internal/application"
	"github.com/superman/pushkin/internal/application/ports"
	contracts "github.com/superman/pushkin/internal/contracts/kafka"
	"github.com/superman/pushkin/internal/domain"
)

type CampaignProgressAggregatorServiceParams struct {
	KafkaConsumer        ports.KafkaTransactionalConsumer
	CompactedTopicLoader ports.CompactedTopicLoader
	BatchSize            int
}

// CampaignProgressAggregatorService reduces progress records for one assigned
// Kafka partition. Its state is restored from the compacted stats topic before
// it consumes new records for that partition. Restore and Process must not be
// called concurrently on one service instance because they mutate its local map.
type CampaignProgressAggregatorService struct {
	kafkaConsumer        ports.KafkaTransactionalConsumer
	compactedTopicLoader ports.CompactedTopicLoader
	batchSize            int
	restored             bool
	progressByID         map[domain.CampaignID]*domain.CampaignProgress
}

func NewCampaignProgressAggregatorService(
	params CampaignProgressAggregatorServiceParams,
) *CampaignProgressAggregatorService {
	return &CampaignProgressAggregatorService{
		kafkaConsumer:        params.KafkaConsumer,
		compactedTopicLoader: params.CompactedTopicLoader,
		batchSize:            params.BatchSize,
		progressByID:         make(map[domain.CampaignID]*domain.CampaignProgress),
	}
}

// Restore waits for the pipeline assignment, then loads the latest compacted
// snapshot for every campaign in its progress partitions. Stats and progress
// topics must use the same partition count and campaign_id key. It may be
// called again after a rebalance; every successful call replaces local state.
func (s *CampaignProgressAggregatorService) Restore(ctx context.Context) error {
	if s.compactedTopicLoader == nil {
		return fmt.Errorf("compacted topic loader: %w", application.ErrValidation)
	}
	assignment, err := s.kafkaConsumer.WaitForAssignment(ctx)
	if err != nil {
		return fmt.Errorf("wait for campaign progress assignment: %w", err)
	}
	partitions := make([]int32, 0, len(assignment))
	for _, partition := range assignment {
		if partition.Topic != contracts.TopicCampaignProgress || partition.Partition < 0 {
			return fmt.Errorf("campaign progress assignment: %w", application.ErrValidation)
		}
		partitions = append(partitions, partition.Partition)
	}
	records, err := s.compactedTopicLoader.Load(ctx, contracts.TopicCampaignStats, partitions)
	if err != nil {
		return err
	}

	states := make(map[domain.CampaignID]*domain.CampaignProgress, len(records))
	for _, record := range records {
		snapshot, ok := record.Value.(contracts.CampaignStatsSnapshotV1)
		if !ok {
			return fmt.Errorf("campaign stats snapshot message %T: %w", record.Value, application.ErrValidation)
		}
		progress, err := campaignProgressFromSnapshot(snapshot)
		if err != nil {
			return err
		}
		if progress.IsComplete() {
			delete(states, snapshot.CampaignID)
		} else {
			states[snapshot.CampaignID] = progress
		}
	}
	s.progressByID = states
	s.restored = true
	return nil
}

// Process reduces up to BatchSize progress records. It commits output snapshots
// and input offsets atomically, then installs the touched in-memory states.
func (s *CampaignProgressAggregatorService) Process(ctx context.Context) error {
	if s.batchSize <= 0 {
		return fmt.Errorf("campaign progress batch size: %w", application.ErrValidation)
	}
	if !s.restored {
		return fmt.Errorf("campaign progress aggregator is not restored: %w", application.ErrConflict)
	}

	records, err := s.kafkaConsumer.PollMany(ctx, s.batchSize)
	if err != nil || len(records) == 0 {
		return err
	}

	changes := campaignProgressChanges{
		current: s.progressByID,
		next:    make(map[domain.CampaignID]*domain.CampaignProgress),
	}
	for _, record := range records {
		message, ok := record.Value.(contracts.CampaignProgressMessageV1)
		if !ok {
			return fmt.Errorf("campaign progress message %T: %w", record.Value, application.ErrValidation)
		}
		if _, err := changes.apply(message); err != nil {
			return err
		}
	}

	messages := campaignStatsMessages(changes.next)
	if err := s.kafkaConsumer.Complete(ctx, messages); err != nil {
		return err
	}
	for campaignID, progress := range changes.next {
		if progress.IsComplete() {
			delete(s.progressByID, campaignID)
			continue
		}
		s.progressByID[campaignID] = progress
	}
	return nil
}

// campaignProgressChanges copies only progress states touched by one Kafka
// transaction. Copying every active campaign for each batch makes processing
// cost grow with historical active state rather than with batch size.
type campaignProgressChanges struct {
	current map[domain.CampaignID]*domain.CampaignProgress
	next    map[domain.CampaignID]*domain.CampaignProgress
}

func (c *campaignProgressChanges) apply(message contracts.CampaignProgressMessageV1) (domain.CampaignID, error) {
	switch value := message.(type) {
	case contracts.CampaignRunStartedV1:
		if value.Type != contracts.CampaignProgressEventTypeRunStarted {
			return value.CampaignID, fmt.Errorf("campaign run started type: %w", application.ErrValidation)
		}
		if c.contains(value.CampaignID) {
			return value.CampaignID, fmt.Errorf("campaign %s progress already exists: %w", value.CampaignID, application.ErrConflict)
		}
		c.next[value.CampaignID] = domain.NewEmptyCampaignProgress(value.SourceBatchesTotal)
		return value.CampaignID, nil

	case contracts.BatchedSourceBatchFanoutCompletedV1:
		if value.Type != contracts.CampaignProgressEventTypeSourceBatchFannedOut {
			return value.CampaignID, fmt.Errorf("source batch fanout type: %w", application.ErrValidation)
		}
		progress, err := c.progress(value.CampaignID)
		if err != nil {
			return value.CampaignID, err
		}
		if err := progress.RecordSourceBatchFanout(value.DeliveryCount); err != nil {
			return value.CampaignID, err
		}
		return value.CampaignID, nil

	case contracts.InlineCampaignFanoutCompletedV1:
		if value.Type != contracts.CampaignProgressEventTypeSourceBatchFannedOut {
			return value.CampaignID, fmt.Errorf("inline campaign fanout type: %w", application.ErrValidation)
		}
		progress, err := c.progress(value.CampaignID)
		if err != nil {
			return value.CampaignID, err
		}
		if err := progress.RecordSourceBatchFanout(value.DeliveryCount); err != nil {
			return value.CampaignID, err
		}
		return value.CampaignID, nil

	case contracts.CampaignProgressDeltaV1:
		if value.Type != contracts.CampaignProgressEventTypeDeliveryDelta {
			return value.CampaignID, fmt.Errorf("campaign progress delta type: %w", application.ErrValidation)
		}
		progress, err := c.progress(value.CampaignID)
		if err != nil {
			return value.CampaignID, err
		}
		if err := progress.RecordDeliveryResults(value.DeliveryAcceptedDelta, value.DeliveryFailedDelta); err != nil {
			return value.CampaignID, err
		}
		return value.CampaignID, nil

	default:
		return domain.CampaignID{}, fmt.Errorf("unsupported campaign progress message %T: %w", message, application.ErrValidation)
	}
}

func (c *campaignProgressChanges) contains(campaignID domain.CampaignID) bool {
	if _, found := c.next[campaignID]; found {
		return true
	}
	_, found := c.current[campaignID]
	return found
}

func (c *campaignProgressChanges) progress(campaignID domain.CampaignID) (*domain.CampaignProgress, error) {
	if progress, found := c.next[campaignID]; found {
		return progress, nil
	}
	progress, found := c.current[campaignID]
	if !found {
		return nil, fmt.Errorf("campaign %s progress has not started: %w", campaignID, application.ErrValidation)
	}
	copy := progress.Copy()
	c.next[campaignID] = copy
	return copy, nil
}

func applyCampaignProgressMessage(
	states map[domain.CampaignID]*domain.CampaignProgress,
	message contracts.CampaignProgressMessageV1,
) (domain.CampaignID, error) {
	changes := campaignProgressChanges{current: states, next: states}
	return changes.apply(message)
}

func campaignProgressFromSnapshot(
	snapshot contracts.CampaignStatsSnapshotV1,
) (*domain.CampaignProgress, error) {
	progress, err := domain.HydrateCampaignProgress(domain.HydrateCampaignProgressParams{
		SourceBatchesTotal:     snapshot.SourceBatchesTotal,
		SourceBatchesFannedOut: snapshot.SourceBatchesFannedOut,
		DeliveryTotal:          snapshot.DeliveryTotal,
		DeliveryProcessed:      snapshot.DeliveryProcessed,
		DeliveryAcceptedCount:  snapshot.DeliveryAcceptedCount,
		DeliveryFailedCount:    snapshot.DeliveryFailedCount,
	})
	if err != nil {
		return nil, fmt.Errorf("campaign stats snapshot %s: %w", snapshot.CampaignID, err)
	}
	return progress, nil
}

func campaignStatsMessages(changed map[domain.CampaignID]*domain.CampaignProgress) []ports.OutboundKafkaMessage {
	campaignIDs := make([]domain.CampaignID, 0, len(changed))
	for campaignID := range changed {
		campaignIDs = append(campaignIDs, campaignID)
	}
	sort.Slice(campaignIDs, func(left, right int) bool {
		return campaignIDs[left].String() < campaignIDs[right].String()
	})

	messages := make([]ports.OutboundKafkaMessage, 0, len(campaignIDs))
	for _, campaignID := range campaignIDs {
		progress := changed[campaignID]
		messages = append(messages, ports.OutboundKafkaMessage{
			Topic: contracts.TopicCampaignStats,
			Key:   []byte(campaignID.String()),
			Value: contracts.CampaignStatsSnapshotV1{
				MessageHeaderV1:        contracts.NewMessageHeaderV1(),
				CampaignID:             campaignID,
				SourceBatchesTotal:     progress.SourceBatchesTotal(),
				SourceBatchesFannedOut: progress.SourceBatchesFannedOut(),
				DeliveryTotal:          progress.DeliveryTotal(),
				DeliveryProcessed:      progress.DeliveryProcessed(),
				DeliveryAcceptedCount:  progress.DeliveryAcceptedCount(),
				DeliveryFailedCount:    progress.DeliveryFailedCount(),
			},
		})
	}
	return messages
}
