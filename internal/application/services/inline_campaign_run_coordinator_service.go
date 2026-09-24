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

// InlineCampaignRunCoordinatorService starts small campaigns and publishes one
// inline fanout request per campaign in the same Kafka transaction.
type InlineCampaignRunCoordinatorService struct {
	campaignRepository ports.CampaignRepository
	transactionManager ports.TransactionManager
	kafkaConsumer      ports.KafkaTransactionalConsumer
	batchSize          int
}

type InlineCampaignRunCoordinatorServiceParams struct {
	CampaignRepository ports.CampaignRepository
	TransactionManager ports.TransactionManager
	KafkaConsumer      ports.KafkaTransactionalConsumer
	BatchSize          int
}

func NewInlineCampaignRunCoordinatorService(params InlineCampaignRunCoordinatorServiceParams) *InlineCampaignRunCoordinatorService {
	return &InlineCampaignRunCoordinatorService{
		campaignRepository: params.CampaignRepository,
		transactionManager: params.TransactionManager,
		kafkaConsumer:      params.KafkaConsumer,
		batchSize:          params.BatchSize,
	}
}

func (s *InlineCampaignRunCoordinatorService) Process(ctx context.Context) error {
	records, err := s.kafkaConsumer.PollMany(ctx, s.batchSize)
	if err != nil || len(records) == 0 {
		return err
	}

	requests, err := inlineCampaignRunRequests(records)
	if err != nil {
		return err
	}
	campaigns, err := s.prepareFanouts(ctx, requests)
	if err != nil {
		return err
	}

	messages := make([]ports.OutboundKafkaMessage, 0, len(campaigns)*2)
	for _, campaign := range campaigns {
		header := contracts.NewMessageHeaderV1()
		messages = append(messages,
			ports.OutboundKafkaMessage{
				Topic: contracts.TopicCampaignProgress,
				Key:   []byte(campaign.ID().String()),
				Value: contracts.CampaignRunStartedV1{
					MessageHeaderV1:    header,
					Type:               contracts.CampaignProgressEventTypeRunStarted,
					CampaignID:         campaign.ID(),
					SourceBatchesTotal: 1,
				},
			},
			ports.OutboundKafkaMessage{
				Topic: contracts.TopicCampaignInlineFanout,
				Key:   []byte(campaign.ID().String()),
				Value: contracts.InlineCampaignFanoutV1{
					MessageHeaderV1: header,
					CampaignID:      campaign.ID(),
					Recipients:      userIDsToStrings(campaign.InlineRecipients()),
				},
			},
		)
	}
	return s.kafkaConsumer.Complete(ctx, messages)
}

func inlineCampaignRunRequests(records []ports.KafkaRecord) ([]contracts.CampaignRunRequestedV1, error) {
	requests := make([]contracts.CampaignRunRequestedV1, 0, len(records))
	for _, record := range records {
		request, ok := record.Value.(contracts.CampaignRunRequestedV1)
		if !ok {
			return nil, fmt.Errorf("inline campaign run message %T: %w", record.Value, application.ErrValidation)
		}
		requests = append(requests, request)
	}
	return requests, nil
}

func (s *InlineCampaignRunCoordinatorService) prepareFanouts(
	ctx context.Context,
	requests []contracts.CampaignRunRequestedV1,
) ([]*domain.Campaign, error) {
	result := make([]*domain.Campaign, 0, len(requests))
	err := s.transactionManager.WithTx(ctx, func(tx ports.Transaction) error {
		campaignRepository := s.campaignRepository.Tx(tx)
		requestsByCampaign, campaignIDs := inlineCampaignRunRequestsByCampaign(requests)
		campaignsByID, err := campaignRepository.FindByIDsForUpdate(ctx, campaignIDs)
		if err != nil {
			return err
		}
		changedCampaigns := make([]*domain.Campaign, 0, len(requests))
		for _, campaignID := range campaignIDs {
			request := requestsByCampaign[campaignID]
			campaign := campaignsByID[campaignID]
			if campaign == nil {
				return fmt.Errorf("inline campaign %s: %w", campaignID, ports.ErrNotFound)
			}
			if campaign.RecipientMode() != domain.CampaignRecipientModeInline {
				return fmt.Errorf("campaign %s is not inline: %w", campaignID, application.ErrValidation)
			}
			changed, err := campaign.MarkStarted(request.RunID, time.Now().UTC())
			if err != nil {
				if errors.Is(err, domain.ErrRunMismatch) {
					continue
				}
				if errors.Is(err, domain.ErrInvalidTransition) && campaign.Status().IsTerminal() {
					continue
				}
				return err
			}
			if changed {
				changedCampaigns = append(changedCampaigns, campaign)
			}
			result = append(result, campaign)
		}
		return campaignRepository.UpdateBatch(ctx, changedCampaigns)
	})
	return result, err
}

func inlineCampaignRunRequestsByCampaign(
	requests []contracts.CampaignRunRequestedV1,
) (map[domain.CampaignID]contracts.CampaignRunRequestedV1, []domain.CampaignID) {
	requestsByCampaign := make(map[domain.CampaignID]contracts.CampaignRunRequestedV1, len(requests))
	campaignIDs := make([]domain.CampaignID, 0, len(requests))
	for _, request := range requests {
		if _, found := requestsByCampaign[request.CampaignID]; found {
			continue
		}
		requestsByCampaign[request.CampaignID] = request
		campaignIDs = append(campaignIDs, request.CampaignID)
	}
	return requestsByCampaign, campaignIDs
}

func userIDsToStrings(ids []domain.UserID) []string {
	result := make([]string, len(ids))
	for index, id := range ids {
		result[index] = string(id)
	}
	return result
}
