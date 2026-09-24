package services

import (
	"context"
	"fmt"

	"github.com/superman/pushkin/internal/application"
	"github.com/superman/pushkin/internal/application/ports"
	contracts "github.com/superman/pushkin/internal/contracts/kafka"
	"github.com/superman/pushkin/internal/domain"
)

// InlineCampaignFanoutService expands a bounded batch of inline campaigns
// into delivery work and one progress completion marker per campaign.
type InlineCampaignFanoutService struct {
	campaignRepository          ports.CampaignRepository
	mobileApplicationRepository ports.MobileApplicationRepository
	pushInstallationRepository  ports.PushInstallationRepository
	kafkaConsumer               ports.KafkaTransactionalConsumer
	batchSize                   int
}

type InlineCampaignFanoutServiceParams struct {
	CampaignRepository          ports.CampaignRepository
	MobileApplicationRepository ports.MobileApplicationRepository
	PushInstallationRepository  ports.PushInstallationRepository
	KafkaConsumer               ports.KafkaTransactionalConsumer
	BatchSize                   int
}

func NewInlineCampaignFanoutService(params InlineCampaignFanoutServiceParams) *InlineCampaignFanoutService {
	return &InlineCampaignFanoutService{
		campaignRepository:          params.CampaignRepository,
		mobileApplicationRepository: params.MobileApplicationRepository,
		pushInstallationRepository:  params.PushInstallationRepository,
		kafkaConsumer:               params.KafkaConsumer,
		batchSize:                   params.BatchSize,
	}
}

func (s *InlineCampaignFanoutService) Process(ctx context.Context) error {
	records, err := s.kafkaConsumer.PollMany(ctx, s.batchSize)
	if err != nil || len(records) == 0 {
		return err
	}
	campaigns, err := s.loadCampaigns(ctx, records)
	if err != nil {
		return err
	}
	installationsByCampaign, err := s.loadInstallations(ctx, campaigns)
	if err != nil {
		return err
	}
	messages := make([]ports.OutboundKafkaMessage, 0, len(campaigns)*2)
	for _, campaign := range campaigns {
		installationIDs := installationsByCampaign[campaign.ID()]
		deliveryTopic, err := contracts.DeliveryTopic(contracts.PriorityV1(campaign.Priority()), campaign.ChannelID())
		if err != nil {
			return fmt.Errorf("delivery topic: %w", err)
		}
		header := contracts.NewMessageHeaderV1()
		for _, installationID := range installationIDs {
			delivery, err := domain.NewDeliveryWork(domain.NewDeliveryWorkParams{CampaignID: campaign.ID(), TenantID: campaign.TenantID(), ChannelID: campaign.ChannelID(), PushInstallationID: installationID, Priority: campaign.Priority()})
			if err != nil {
				return fmt.Errorf("new delivery work: %w", err)
			}
			messages = append(messages, ports.OutboundKafkaMessage{Topic: deliveryTopic, Key: []byte(delivery.ID().String()), Value: contracts.NewDeliveryWorkV1(delivery)})
		}
		messages = append(messages, ports.OutboundKafkaMessage{Topic: contracts.TopicCampaignProgress, Key: []byte(campaign.ID().String()), Value: contracts.InlineCampaignFanoutCompletedV1{MessageHeaderV1: header, Type: contracts.CampaignProgressEventTypeSourceBatchFannedOut, CampaignID: campaign.ID(), DeliveryCount: uint64(len(installationIDs))}})
	}
	return s.kafkaConsumer.Complete(ctx, messages)
}

func (s *InlineCampaignFanoutService) loadCampaigns(ctx context.Context, records []ports.KafkaRecord) ([]*domain.Campaign, error) {
	requests := make(map[domain.CampaignID]contracts.InlineCampaignFanoutV1, len(records))
	ids := make([]domain.CampaignID, 0, len(records))
	for _, record := range records {
		work, ok := record.Value.(contracts.InlineCampaignFanoutV1)
		if !ok {
			return nil, fmt.Errorf("inline campaign fanout message %T: %w", record.Value, application.ErrValidation)
		}
		if previous, found := requests[work.CampaignID]; found {
			if !equalStringSlices(previous.Recipients, work.Recipients) {
				return nil, fmt.Errorf("inline campaign %s recipients: %w", work.CampaignID, application.ErrValidation)
			}
			continue
		}
		requests[work.CampaignID] = work
		ids = append(ids, work.CampaignID)
	}
	loaded, err := s.campaignRepository.FindByIDs(ctx, ids)
	if err != nil {
		return nil, err
	}
	campaigns := make([]*domain.Campaign, 0, len(ids))
	for _, id := range ids {
		campaign := loaded[id]
		if campaign == nil || campaign.RecipientMode() != domain.CampaignRecipientModeInline || !equalUserIDs(campaign.InlineRecipients(), requests[id].Recipients) {
			return nil, fmt.Errorf("inline campaign %s: %w", id, application.ErrValidation)
		}
		campaigns = append(campaigns, campaign)
	}
	return campaigns, nil
}

func (s *InlineCampaignFanoutService) loadInstallations(ctx context.Context, campaigns []*domain.Campaign) (map[domain.CampaignID][]domain.PushInstallationID, error) {
	channelUsers := make(map[domain.ChannelID]map[domain.UserID]struct{})
	channelTenants := make(map[domain.ChannelID]domain.TenantID)
	channelIDs := make([]domain.ChannelID, 0, len(campaigns))
	for _, campaign := range campaigns {
		users, found := channelUsers[campaign.ChannelID()]
		if !found {
			users = make(map[domain.UserID]struct{})
			channelUsers[campaign.ChannelID()] = users
			channelTenants[campaign.ChannelID()] = campaign.TenantID()
			channelIDs = append(channelIDs, campaign.ChannelID())
		}
		for _, userID := range campaign.InlineRecipients() {
			users[userID] = struct{}{}
		}
	}
	applications, err := s.mobileApplicationRepository.ListIDsByChannels(ctx, channelIDs)
	if err != nil {
		return nil, err
	}
	installationsByChannelUser := make(map[domain.ChannelID]map[domain.UserID][]domain.PushInstallationID, len(channelIDs))
	for _, channelID := range channelIDs {
		userIDs := make([]domain.UserID, 0, len(channelUsers[channelID]))
		for userID := range channelUsers[channelID] {
			userIDs = append(userIDs, userID)
		}
		installationIDsByUser, err := s.pushInstallationRepository.ListActiveIDsByUsers(ctx, channelTenants[channelID], userIDs, applications[channelID])
		if err != nil {
			return nil, err
		}
		installationsByChannelUser[channelID] = installationIDsByUser
	}
	result := make(map[domain.CampaignID][]domain.PushInstallationID, len(campaigns))
	for _, campaign := range campaigns {
		for _, userID := range campaign.InlineRecipients() {
			result[campaign.ID()] = append(result[campaign.ID()], installationsByChannelUser[campaign.ChannelID()][userID]...)
		}
	}
	return result, nil
}

func equalUserIDs(ids []domain.UserID, values []string) bool {
	if len(ids) != len(values) {
		return false
	}
	for index, id := range ids {
		if string(id) != values[index] {
			return false
		}
	}
	return true
}

func equalStringSlices(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
