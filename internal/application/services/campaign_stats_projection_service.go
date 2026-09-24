package services

import (
	"context"
	"fmt"
	"sort"
	"time"
	"uuid"

	"github.com/superman/pushkin/internal/application"
	"github.com/superman/pushkin/internal/application/ports"
	contracts "github.com/superman/pushkin/internal/contracts/kafka"
	"github.com/superman/pushkin/internal/domain"
)

type CampaignStatsProjectionServiceParams struct {
	CampaignRepository ports.CampaignRepository
	TransactionManager ports.TransactionManager
	KafkaConsumer      ports.KafkaConsumer
	BatchSize          int
}

// CampaignStatsProjectionService applies one absolute stats snapshot to its
// Campaign row. One service instance processes records sequentially; Process
// must not be called concurrently on the same instance.
type CampaignStatsProjectionService struct {
	campaignRepository ports.CampaignRepository
	transactionManager ports.TransactionManager
	kafkaConsumer      ports.KafkaConsumer
	batchSize          int
}

func NewCampaignStatsProjectionService(
	params CampaignStatsProjectionServiceParams,
) *CampaignStatsProjectionService {
	return &CampaignStatsProjectionService{
		campaignRepository: params.CampaignRepository,
		transactionManager: params.TransactionManager,
		kafkaConsumer:      params.KafkaConsumer,
		batchSize:          params.BatchSize,
	}
}

// Process persists the latest snapshot for every campaign in one Kafka batch
// before committing its offsets. A crash after PostgreSQL commit causes safe
// replay because every snapshot is absolute.
func (s *CampaignStatsProjectionService) Process(ctx context.Context) error {
	if s.batchSize <= 0 {
		return fmt.Errorf("campaign stats batch size: %w", application.ErrValidation)
	}
	records, err := s.kafkaConsumer.PollMany(ctx, s.batchSize)
	if err != nil || len(records) == 0 {
		return err
	}
	snapshots, err := latestCampaignStatsSnapshots(records)
	if err != nil {
		return err
	}

	if err := s.transactionManager.WithTx(ctx, func(tx ports.Transaction) error {
		campaignRepository := s.campaignRepository.Tx(tx)
		campaigns, err := campaignRepository.FindByIDsForUpdate(ctx, campaignStatsSnapshotIDs(snapshots))
		if err != nil {
			return err
		}
		updates := make([]*domain.Campaign, 0, len(snapshots))
		for campaignID, snapshot := range snapshots {
			campaign := campaigns[campaignID]
			if campaign == nil {
				return fmt.Errorf("campaign %s: %w", campaignID, application.ErrValidation)
			}
			progress, err := campaignProgressFromSnapshot(snapshot)
			if err != nil {
				return err
			}
			current := campaign.CurrentProgress()
			if current != nil && current.IsMonotonicFrom(progress) {
				continue
			}
			if err := campaign.ApplyCurrentProgress(progress, time.Now().UTC()); err != nil {
				return err
			}
			updates = append(updates, campaign)
		}
		return campaignRepository.UpdateBatch(ctx, updates)
	}); err != nil {
		return err
	}

	return s.kafkaConsumer.Commit(ctx, records)
}

func latestCampaignStatsSnapshots(records []ports.KafkaRecord) (map[domain.CampaignID]contracts.CampaignStatsSnapshotV1, error) {
	snapshots := make(map[domain.CampaignID]contracts.CampaignStatsSnapshotV1, len(records))
	for _, record := range records {
		snapshot, ok := record.Value.(contracts.CampaignStatsSnapshotV1)
		if !ok {
			return nil, fmt.Errorf("campaign stats message %T: %w", record.Value, application.ErrValidation)
		}
		if err := validateCampaignStatsSnapshot(snapshot); err != nil {
			return nil, err
		}
		snapshots[snapshot.CampaignID] = snapshot
	}
	return snapshots, nil
}

func campaignStatsSnapshotIDs(snapshots map[domain.CampaignID]contracts.CampaignStatsSnapshotV1) []domain.CampaignID {
	ids := make([]domain.CampaignID, 0, len(snapshots))
	for campaignID := range snapshots {
		ids = append(ids, campaignID)
	}
	sort.Slice(ids, func(left, right int) bool { return ids[left].String() < ids[right].String() })
	return ids
}

func validateCampaignStatsSnapshot(snapshot contracts.CampaignStatsSnapshotV1) error {
	if snapshot.CampaignID == uuid.Nil() {
		return fmt.Errorf("campaign stats campaign_id: %w", application.ErrValidation)
	}
	return nil
}
