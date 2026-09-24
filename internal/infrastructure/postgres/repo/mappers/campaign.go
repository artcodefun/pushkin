package mappers

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/superman/pushkin/internal/domain"
	"github.com/superman/pushkin/internal/infrastructure/postgres/gen"
)

// CampaignsToBatchUpdateJSON encodes the persisted mutable state of campaigns
// for the PostgreSQL bulk update query.
func CampaignsToBatchUpdateJSON(campaigns []*domain.Campaign) ([]byte, error) {
	updates := make([]campaignBatchUpdate, 0, len(campaigns))
	for _, campaign := range campaigns {
		updates = append(updates, campaignBatchUpdateFromDomain(campaign))
	}
	return json.Marshal(updates)
}

type campaignBatchUpdate struct {
	ID                     domain.CampaignID `json:"id"`
	Status                 string            `json:"status"`
	ScheduledAt            *time.Time        `json:"scheduled_at"`
	RunID                  *domain.RunID     `json:"run_id"`
	RunAttemptedAt         *time.Time        `json:"run_attempted_at"`
	StartedAt              *time.Time        `json:"started_at"`
	CompletedAt            *time.Time        `json:"completed_at"`
	FailedAt               *time.Time        `json:"failed_at"`
	FailureReason          string            `json:"failure_reason"`
	SourceBatchesTotal     int64             `json:"source_batches_total"`
	SourceBatchesFannedOut int64             `json:"source_batches_fanned_out"`
	DeliveryTotal          int64             `json:"delivery_total"`
	DeliveryProcessed      int64             `json:"delivery_processed"`
	DeliveryAcceptedCount  int64             `json:"delivery_accepted_count"`
	DeliveryFailedCount    int64             `json:"delivery_failed_count"`
}

func campaignBatchUpdateFromDomain(campaign *domain.Campaign) campaignBatchUpdate {
	update := campaignBatchUpdate{
		ID:             campaign.ID(),
		Status:         string(campaign.Status()),
		ScheduledAt:    campaign.ScheduledAt(),
		RunID:          campaign.RunID(),
		RunAttemptedAt: campaign.RunAttemptedAt(),
		StartedAt:      campaign.StartedAt(),
		CompletedAt:    campaign.CompletedAt(),
		FailedAt:       campaign.FailedAt(),
		FailureReason:  campaign.FailureReason(),
	}
	progress := campaign.CurrentProgress()
	if progress == nil {
		return update
	}
	update.SourceBatchesTotal = int64(progress.SourceBatchesTotal())
	update.SourceBatchesFannedOut = int64(progress.SourceBatchesFannedOut())
	update.DeliveryTotal = int64(progress.DeliveryTotal())
	update.DeliveryProcessed = int64(progress.DeliveryProcessed())
	update.DeliveryAcceptedCount = int64(progress.DeliveryAcceptedCount())
	update.DeliveryFailedCount = int64(progress.DeliveryFailedCount())
	return update
}

func CampaignFromRow(row gen.Campaign) (*domain.Campaign, error) {
	payloadData := make(map[string]string)
	if len(row.Data) > 0 {
		if err := json.Unmarshal(row.Data, &payloadData); err != nil {
			return nil, fmt.Errorf("campaign %s payload data: %w", row.ID, err)
		}
	}
	pushPayload, err := domain.NewPushPayload(row.Title, row.Body, row.ImageUrl, payloadData)
	if err != nil {
		return nil, err
	}

	progress, err := campaignProgressFromRow(row)
	if err != nil {
		return nil, err
	}
	inlineRecipients := make([]domain.UserID, 0)
	if err := json.Unmarshal(row.InlineRecipients, &inlineRecipients); err != nil {
		return nil, fmt.Errorf("campaign %s inline recipients: %w", row.ID, err)
	}
	return domain.HydrateCampaign(domain.HydrateCampaignParams{
		ID:               row.ID,
		TenantID:         row.TenantID,
		ChannelID:        row.ChannelID,
		PushPayload:      pushPayload,
		Priority:         domain.Priority(row.Priority),
		RecipientMode:    domain.CampaignRecipientMode(row.RecipientMode),
		InlineRecipients: inlineRecipients,
		Status:           domain.CampaignStatus(row.Status),
		ScheduledAt:      row.ScheduledAt,
		RunID:            row.RunID,
		RunAttemptedAt:   row.RunAttemptedAt,
		StartedAt:        row.StartedAt,
		CompletedAt:      row.CompletedAt,
		FailedAt:         row.FailedAt,
		FailureReason:    row.FailureReason,
		CurrentProgress:  progress,
	})
}

func campaignProgressFromRow(row gen.Campaign) (*domain.CampaignProgress, error) {
	counts := []int64{
		row.SourceBatchesTotal,
		row.SourceBatchesFannedOut,
		row.DeliveryTotal,
		row.DeliveryProcessed,
		row.DeliveryAcceptedCount,
		row.DeliveryFailedCount,
	}
	for _, count := range counts {
		if count < 0 {
			return nil, fmt.Errorf("campaign %s has negative progress", row.ID)
		}
	}
	if row.SourceBatchesTotal == 0 && row.SourceBatchesFannedOut == 0 &&
		row.DeliveryTotal == 0 && row.DeliveryProcessed == 0 &&
		row.DeliveryAcceptedCount == 0 && row.DeliveryFailedCount == 0 {
		return nil, nil
	}
	return domain.HydrateCampaignProgress(domain.HydrateCampaignProgressParams{
		SourceBatchesTotal:     uint64(row.SourceBatchesTotal),
		SourceBatchesFannedOut: uint64(row.SourceBatchesFannedOut),
		DeliveryTotal:          uint64(row.DeliveryTotal),
		DeliveryProcessed:      uint64(row.DeliveryProcessed),
		DeliveryAcceptedCount:  uint64(row.DeliveryAcceptedCount),
		DeliveryFailedCount:    uint64(row.DeliveryFailedCount),
	})
}
