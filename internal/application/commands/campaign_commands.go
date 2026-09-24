package commands

import (
	"context"
	"fmt"
	"time"

	"github.com/superman/pushkin/internal/application"
	"github.com/superman/pushkin/internal/application/ports"
	contracts "github.com/superman/pushkin/internal/contracts/kafka"
	"github.com/superman/pushkin/internal/domain"
)

var _ application.CampaignCommands = (*CampaignCommands)(nil)

type CampaignCommandsParams struct {
	ChannelRepository       ports.ChannelRepository
	CampaignRepository      ports.CampaignRepository
	SourceBatchRepository   ports.SourceBatchRepository
	TransactionManager      ports.TransactionManager
	KafkaProducer           ports.KafkaProducer
	SourceBatchMaxSize      int
	CampaignStartingTimeout time.Duration
}

type CampaignCommands struct {
	channelRepository       ports.ChannelRepository
	campaignRepository      ports.CampaignRepository
	sourceBatchRepository   ports.SourceBatchRepository
	transactionManager      ports.TransactionManager
	kafkaProducer           ports.KafkaProducer
	sourceBatchMaxSize      int
	campaignStartingTimeout time.Duration
}

func NewCampaignCommands(params CampaignCommandsParams) *CampaignCommands {
	return &CampaignCommands{
		channelRepository:       params.ChannelRepository,
		campaignRepository:      params.CampaignRepository,
		sourceBatchRepository:   params.SourceBatchRepository,
		transactionManager:      params.TransactionManager,
		kafkaProducer:           params.KafkaProducer,
		sourceBatchMaxSize:      params.SourceBatchMaxSize,
		campaignStartingTimeout: params.CampaignStartingTimeout,
	}
}

func (c *CampaignCommands) CreateCampaign(
	ctx context.Context,
	command application.CreateCampaignCommand,
) (application.CreateCampaignResult, error) {
	channel, err := c.channelRepository.FindByKey(ctx, command.TenantID, command.ChannelKey)
	if err != nil {
		return application.CreateCampaignResult{}, err
	}
	if channel.TenantID() != command.TenantID {
		return application.CreateCampaignResult{}, fmt.Errorf("%w: channel belongs to another tenant", application.ErrNotAuthorized)
	}
	if channel.Status() != domain.ChannelStatusActive {
		return application.CreateCampaignResult{}, fmt.Errorf("%w: channel is disabled", application.ErrConflict)
	}

	pushPayload, err := domain.NewPushPayload(command.Title, command.Body, command.ImageURL, command.Data)
	if err != nil {
		return application.CreateCampaignResult{}, err
	}
	campaign, err := domain.NewBatchedCampaign(domain.NewBatchedCampaignParams{
		TenantID:    command.TenantID,
		ChannelID:   channel.ID(),
		PushPayload: pushPayload,
		Priority:    command.Priority,
	})
	if err != nil {
		return application.CreateCampaignResult{}, err
	}
	if err := c.campaignRepository.Create(ctx, campaign); err != nil {
		return application.CreateCampaignResult{}, err
	}

	return application.CreateCampaignResult{CampaignID: campaign.ID()}, nil
}

func (c *CampaignCommands) CreateInlineCampaign(
	ctx context.Context,
	command application.CreateInlineCampaignCommand,
) (application.CreateInlineCampaignResult, error) {
	channel, err := c.channelRepository.FindByKey(ctx, command.TenantID, command.ChannelKey)
	if err != nil {
		return application.CreateInlineCampaignResult{}, err
	}
	if channel.TenantID() != command.TenantID {
		return application.CreateInlineCampaignResult{}, fmt.Errorf("%w: channel belongs to another tenant", application.ErrNotAuthorized)
	}
	if channel.Status() != domain.ChannelStatusActive {
		return application.CreateInlineCampaignResult{}, fmt.Errorf("%w: channel is disabled", application.ErrConflict)
	}

	pushPayload, err := domain.NewPushPayload(command.Title, command.Body, command.ImageURL, command.Data)
	if err != nil {
		return application.CreateInlineCampaignResult{}, err
	}
	now := time.Now().UTC()
	campaign, err := domain.NewInlineCampaign(domain.NewInlineCampaignParams{
		TenantID: command.TenantID, ChannelID: channel.ID(), PushPayload: pushPayload, Priority: command.Priority,
		Recipients: command.UserIDs, ScheduledAt: command.ScheduledAt, Now: now,
	})
	if err != nil {
		return application.CreateInlineCampaignResult{}, err
	}
	var runID domain.RunID
	if campaign.Status() == domain.CampaignStatusStarting {
		if c.kafkaProducer == nil || c.campaignStartingTimeout <= 0 {
			return application.CreateInlineCampaignResult{}, fmt.Errorf("%w: immediate inline campaign publishing is unavailable", application.ErrUnavailable)
		}
		runID, err = campaign.BeginRun(now, c.campaignStartingTimeout)
		if err != nil {
			return application.CreateInlineCampaignResult{}, err
		}
	}
	if err := c.campaignRepository.Create(ctx, campaign); err != nil {
		return application.CreateInlineCampaignResult{}, err
	}
	if campaign.Status() == domain.CampaignStatusStarting {
		topic, err := contracts.CampaignRunTopic(contracts.CampaignRecipientModeV1(campaign.RecipientMode()))
		if err != nil {
			return application.CreateInlineCampaignResult{}, fmt.Errorf("campaign run topic: %w", err)
		}
		if err := c.kafkaProducer.Produce(ctx, []ports.OutboundKafkaMessage{{
			Topic: topic,
			Key:   []byte(campaign.ID().String()),
			Value: contracts.CampaignRunRequestedV1{
				MessageHeaderV1: contracts.NewMessageHeaderV1(),
				CampaignID:      campaign.ID(),
				RunID:           runID,
			},
		}}); err != nil {
			return application.CreateInlineCampaignResult{}, err
		}
	}
	return application.CreateInlineCampaignResult{CampaignID: campaign.ID(), Status: campaign.Status()}, nil
}

func (c *CampaignCommands) AddCampaignRecipients(
	ctx context.Context,
	command application.AddCampaignRecipientsCommand,
) (application.AddCampaignRecipientsResult, error) {
	if len(command.UserIDs) > c.sourceBatchMaxSize {
		return application.AddCampaignRecipientsResult{}, fmt.Errorf(
			"%w: source batch contains %d user_ids; maximum is %d",
			application.ErrValidation,
			len(command.UserIDs),
			c.sourceBatchMaxSize,
		)
	}

	var result application.AddCampaignRecipientsResult
	err := c.transactionManager.WithTx(ctx, func(tx ports.Transaction) error {
		campaignRepository := c.campaignRepository.Tx(tx)
		campaign, err := campaignRepository.FindByIDForUpdate(ctx, command.CampaignID)
		if err != nil {
			return err
		}
		if campaign.TenantID() != command.TenantID {
			return fmt.Errorf("%w: campaign belongs to another tenant", application.ErrNotAuthorized)
		}
		if !campaign.CanAddRecipients() {
			return fmt.Errorf("%w: campaign does not accept recipients", application.ErrConflict)
		}

		batch, err := domain.NewSourceBatch(campaign.ID(), command.UserIDs)
		if err != nil {
			return err
		}
		if err := c.sourceBatchRepository.Tx(tx).Create(ctx, batch); err != nil {
			return err
		}
		result = application.AddCampaignRecipientsResult{SourceBatchID: batch.ID()}
		return nil
	})
	if err != nil {
		return application.AddCampaignRecipientsResult{}, err
	}

	return result, nil
}

func (c *CampaignCommands) StartCampaign(
	ctx context.Context,
	command application.StartCampaignCommand,
) (application.StartCampaignResult, error) {
	now := time.Now().UTC()
	var result application.StartCampaignResult

	err := c.transactionManager.WithTx(ctx, func(tx ports.Transaction) error {
		campaignRepository := c.campaignRepository.Tx(tx)
		campaign, err := campaignRepository.FindByIDForUpdate(ctx, command.CampaignID)
		if err != nil {
			return err
		}
		if campaign.TenantID() != command.TenantID {
			return fmt.Errorf("%w: campaign belongs to another tenant", application.ErrNotAuthorized)
		}

		if command.ScheduledAt != nil && command.ScheduledAt.After(now) {
			if err := campaign.Schedule(*command.ScheduledAt, now); err != nil {
				return err
			}
		} else if err := campaign.RequestStart(); err != nil {
			return err
		}
		if err := campaignRepository.Update(ctx, campaign); err != nil {
			return err
		}
		result = application.StartCampaignResult{
			CampaignID: campaign.ID(),
			Status:     campaign.Status(),
		}
		return nil
	})
	if err != nil {
		return application.StartCampaignResult{}, err
	}
	return result, nil
}
