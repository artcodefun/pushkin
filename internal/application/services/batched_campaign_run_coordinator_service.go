package services

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/superman/pushkin/internal/application"
	"github.com/superman/pushkin/internal/application/ports"
	contracts "github.com/superman/pushkin/internal/contracts/kafka"
	"github.com/superman/pushkin/internal/domain"
)

type BatchedCampaignRunCoordinatorServiceParams struct {
	CampaignRepository    ports.CampaignRepository
	SourceBatchRepository ports.SourceBatchRepository
	TransactionManager    ports.TransactionManager
	KafkaConsumer         ports.KafkaTransactionalConsumer
}

type BatchedCampaignRunCoordinatorService struct {
	campaignRepository    ports.CampaignRepository
	sourceBatchRepository ports.SourceBatchRepository
	transactionManager    ports.TransactionManager
	kafkaConsumer         ports.KafkaTransactionalConsumer
}

func NewBatchedCampaignRunCoordinatorService(
	params BatchedCampaignRunCoordinatorServiceParams,
) *BatchedCampaignRunCoordinatorService {
	return &BatchedCampaignRunCoordinatorService{
		campaignRepository:    params.CampaignRepository,
		sourceBatchRepository: params.SourceBatchRepository,
		transactionManager:    params.TransactionManager,
		kafkaConsumer:         params.KafkaConsumer,
	}
}

// Process consumes one CampaignRunRequestedV1 record and atomically emits
// its fan-out records with the consumed offsets in one Kafka transaction.
func (s *BatchedCampaignRunCoordinatorService) Process(ctx context.Context) error {
	record, found, err := s.kafkaConsumer.Poll(ctx)
	if err != nil {
		return err
	}
	if !found {
		return nil
	}

	request, ok := record.Value.(contracts.CampaignRunRequestedV1)
	if !ok {
		return fmt.Errorf("campaign run message %T: %w", record.Value, application.ErrValidation)
	}
	sourceBatchIDs, err := s.prepareFanout(ctx, request.CampaignID, request.RunID)
	if err != nil {
		return err
	}

	messages := make([]ports.OutboundKafkaMessage, 0)
	if sourceBatchIDs != nil {
		input := request
		header := contracts.NewMessageHeaderV1()
		messages = append(messages, ports.OutboundKafkaMessage{
			Topic: contracts.TopicCampaignProgress,
			Key:   []byte(input.CampaignID.String()),
			Value: contracts.CampaignRunStartedV1{
				MessageHeaderV1:    header,
				Type:               contracts.CampaignProgressEventTypeRunStarted,
				CampaignID:         input.CampaignID,
				SourceBatchesTotal: uint64(len(sourceBatchIDs)),
			},
		})
		for _, sourceBatchID := range sourceBatchIDs {
			messages = append(messages, ports.OutboundKafkaMessage{
				Topic: contracts.TopicCampaignBatchedSourceBatchFanout,
				Key:   []byte(sourceBatchID.String()),
				Value: contracts.BatchedSourceBatchFanoutV1{
					MessageHeaderV1: header,
					SourceBatchID:   sourceBatchID,
				},
			})
		}
	}

	err = s.kafkaConsumer.Complete(ctx, messages)
	if err != nil {
		return err
	}

	return nil
}

// prepareFanout accepts the current fenced run, marks it started, and returns
// its immutable source batches. A nil result means the run is fenced or no
// longer active; a non-nil empty result represents an active empty campaign.
func (s *BatchedCampaignRunCoordinatorService) prepareFanout(
	ctx context.Context,
	campaignID domain.CampaignID,
	runID domain.RunID,
) ([]domain.SourceBatchID, error) {
	var sourceBatchIDs []domain.SourceBatchID
	now := time.Now().UTC()

	err := s.transactionManager.WithTx(ctx, func(tx ports.Transaction) error {
		campaignRepository := s.campaignRepository.Tx(tx)
		campaign, err := campaignRepository.FindByIDForUpdate(ctx, campaignID)
		if err != nil {
			return err
		}

		changed, err := campaign.MarkStarted(runID, now)
		if err != nil {
			if errors.Is(err, domain.ErrRunMismatch) {
				return nil
			}
			if errors.Is(err, domain.ErrInvalidTransition) && campaign.Status().IsTerminal() {
				return nil
			}
			return err
		}
		if changed {
			if err := campaignRepository.Update(ctx, campaign); err != nil {
				return err
			}
		}

		batchIDs, err := s.sourceBatchRepository.Tx(tx).ListIDsByCampaign(ctx, campaign.ID())
		if err != nil {
			return err
		}
		sourceBatchIDs = make([]domain.SourceBatchID, len(batchIDs))
		copy(sourceBatchIDs, batchIDs)
		return nil
	})
	if err != nil {
		return nil, err
	}

	return sourceBatchIDs, nil
}
