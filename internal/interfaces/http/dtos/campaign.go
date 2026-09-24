package dtos

import (
	"time"

	"github.com/superman/pushkin/internal/application/queries/readmodels"
)

type CreateCampaignRequest struct {
	ChannelKey   string            `json:"channel_key" binding:"required"`
	Priority     string            `json:"priority" binding:"required,oneof=critical high normal"`
	Notification Notification      `json:"notification" binding:"required"`
	Data         map[string]string `json:"data"`
}

type Notification struct {
	Title string `json:"title" binding:"required"`
	Body  string `json:"body" binding:"required"`
	Image string `json:"image"`
}

type CreateCampaignResponse struct {
	CampaignID string `json:"campaign_id"`
}

type CreateInlineCampaignRequest struct {
	ChannelKey   string            `json:"channel_key" binding:"required"`
	Priority     string            `json:"priority" binding:"required,oneof=critical high normal"`
	Notification Notification      `json:"notification" binding:"required"`
	Data         map[string]string `json:"data"`
	UserIDs      []string          `json:"user_ids" binding:"required,min=1,max=100"`
	ScheduledAt  *time.Time        `json:"scheduled_at"`
}

type CreateInlineCampaignResponse struct {
	CampaignID string `json:"campaign_id"`
	Status     string `json:"status"`
}

type AddCampaignRecipientsRequest struct {
	UserIDs []string `json:"user_ids" binding:"required,min=1"`
}

type AddCampaignRecipientsResponse struct {
	SourceBatchID string `json:"source_batch_id"`
}

type StartCampaignRequest struct {
	ScheduledAt *time.Time `json:"scheduled_at"`
}

type StartCampaignResponse struct {
	CampaignID string `json:"campaign_id"`
	Status     string `json:"status"`
}

type CampaignResponse struct {
	CampaignID            string            `json:"campaign_id"`
	ChannelID             string            `json:"channel_id"`
	Priority              string            `json:"priority"`
	Status                string            `json:"status"`
	Notification          Notification      `json:"notification"`
	Data                  map[string]string `json:"data"`
	ScheduledAt           *time.Time        `json:"scheduled_at,omitempty"`
	StartedAt             *time.Time        `json:"started_at,omitempty"`
	DeliveryTotal         uint64            `json:"delivery_total"`
	DeliveryProcessed     uint64            `json:"delivery_processed"`
	DeliveryAcceptedCount uint64            `json:"delivery_accepted_count"`
	DeliveryFailedCount   uint64            `json:"delivery_failed_count"`
}

func NewCampaignResponse(campaign *readmodels.Campaign) CampaignResponse {
	return CampaignResponse{
		CampaignID: campaign.ID.String(), ChannelID: campaign.ChannelID.String(), Priority: string(campaign.Priority), Status: string(campaign.Status),
		Notification: Notification{Title: campaign.Title, Body: campaign.Body, Image: campaign.ImageURL}, Data: campaign.Data,
		ScheduledAt: campaign.ScheduledAt, StartedAt: campaign.StartedAt, DeliveryTotal: campaign.DeliveryTotal,
		DeliveryProcessed: campaign.DeliveryProcessed, DeliveryAcceptedCount: campaign.DeliveryAcceptedCount, DeliveryFailedCount: campaign.DeliveryFailedCount,
	}
}
