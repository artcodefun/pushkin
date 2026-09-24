package services

import (
	"context"
	"fmt"
	"time"

	"github.com/superman/pushkin/internal/application"
	"github.com/superman/pushkin/internal/application/ports"
	contracts "github.com/superman/pushkin/internal/contracts/kafka"
	"github.com/superman/pushkin/internal/domain"
)

type CampaignSchedulerServiceParams struct {
	CampaignRepository ports.CampaignRepository
	TransactionManager ports.TransactionManager
	KafkaProducer      ports.KafkaProducer
	StartingTimeout    time.Duration
}

type CampaignSchedulerService struct {
	campaignRepository ports.CampaignRepository
	transactionManager ports.TransactionManager
	kafkaProducer      ports.KafkaProducer
	startingTimeout    time.Duration
}

func NewCampaignSchedulerService(params CampaignSchedulerServiceParams) *CampaignSchedulerService {
	return &CampaignSchedulerService{
		campaignRepository: params.CampaignRepository,
		transactionManager: params.TransactionManager,
		kafkaProducer:      params.KafkaProducer,
		startingTimeout:    params.StartingTimeout,
	}
}

type CampaignSchedulerResult struct {
	PublishedRuns     int
	BatchLimitReached bool
}

// ProcessDue starts due scheduled campaigns, fresh immediate campaigns with no
// run attempt, and runs that have stayed in STARTING past StartingTimeout.
func (s *CampaignSchedulerService) ProcessDue(
	ctx context.Context,
	limit int,
) (CampaignSchedulerResult, error) {
	if limit <= 0 {
		return CampaignSchedulerResult{}, fmt.Errorf("%w: limit must be positive", application.ErrValidation)
	}
	if s.startingTimeout <= 0 {
		return CampaignSchedulerResult{}, fmt.Errorf("%w: starting timeout must be positive", application.ErrValidation)
	}
	now := time.Now().UTC()
	var messages []ports.OutboundKafkaMessage
	batchLimitReached := false
	err := s.transactionManager.WithTx(ctx, func(tx ports.Transaction) error {
		campaignRepository := s.campaignRepository.Tx(tx)
		campaigns, err := campaignRepository.FindStartCandidatesForUpdate(
			ctx,
			now,
			now.Add(-s.startingTimeout),
			limit,
		)
		if err != nil {
			return err
		}
		batchLimitReached = len(campaigns) == limit
		started := make([]*domain.Campaign, 0, len(campaigns))
		messages = make([]ports.OutboundKafkaMessage, 0, len(campaigns))
		for _, campaign := range campaigns {
			if !campaign.CanBeginRun(now, s.startingTimeout) {
				continue
			}
			runID, err := campaign.BeginRun(now, s.startingTimeout)
			if err != nil {
				return err
			}
			started = append(started, campaign)
			topic, err := contracts.CampaignRunTopic(contracts.CampaignRecipientModeV1(campaign.RecipientMode()))
			if err != nil {
				return fmt.Errorf("campaign run topic: %w", err)
			}
			messages = append(messages, ports.OutboundKafkaMessage{
				Topic: topic,
				Key:   []byte(campaign.ID().String()),
				Value: contracts.CampaignRunRequestedV1{
					MessageHeaderV1: contracts.NewMessageHeaderV1(),
					CampaignID:      campaign.ID(),
					RunID:           runID,
				},
			})
		}
		return campaignRepository.UpdateBatch(ctx, started)
	})
	if err != nil {
		return CampaignSchedulerResult{}, err
	}
	if len(messages) == 0 {
		return CampaignSchedulerResult{BatchLimitReached: batchLimitReached}, nil
	}
	if err := s.kafkaProducer.Produce(ctx, messages); err != nil {
		return CampaignSchedulerResult{}, err
	}
	return CampaignSchedulerResult{PublishedRuns: len(messages), BatchLimitReached: batchLimitReached}, nil
}
