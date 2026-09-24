package handlers

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/superman/pushkin/internal/application"
	"github.com/superman/pushkin/internal/domain"
	"github.com/superman/pushkin/internal/interfaces/http/dtos"
	"github.com/superman/pushkin/internal/interfaces/http/middleware"
)

type CampaignHandler struct {
	commands application.CampaignCommands
	queries  application.CampaignQueries
}

func NewCampaignHandler(commands application.CampaignCommands, queries application.CampaignQueries) *CampaignHandler {
	return &CampaignHandler{commands: commands, queries: queries}
}

func (h *CampaignHandler) Create(c *gin.Context) {
	var request dtos.CreateCampaignRequest
	if !bindJSON(c, &request) {
		return
	}
	priority, err := domain.ParsePriority(request.Priority)
	if err != nil {
		writeStatus(c, http.StatusBadRequest)
		return
	}
	result, err := h.commands.CreateCampaign(c.Request.Context(), application.CreateCampaignCommand{
		TenantID: middleware.TenantID(c), ChannelKey: request.ChannelKey, Priority: priority,
		Title: request.Notification.Title, Body: request.Notification.Body, ImageURL: request.Notification.Image, Data: request.Data,
	})
	if writeError(c, err) {
		return
	}
	c.JSON(http.StatusCreated, dtos.CreateCampaignResponse{CampaignID: result.CampaignID.String()})
}

func (h *CampaignHandler) CreateInline(c *gin.Context) {
	var request dtos.CreateInlineCampaignRequest
	if !bindJSON(c, &request) {
		return
	}
	priority, err := domain.ParsePriority(request.Priority)
	if err != nil {
		writeStatus(c, http.StatusBadRequest)
		return
	}
	userIDs := make([]domain.UserID, len(request.UserIDs))
	for index, userID := range request.UserIDs {
		userIDs[index] = domain.UserID(userID)
	}
	result, err := h.commands.CreateInlineCampaign(c.Request.Context(), application.CreateInlineCampaignCommand{
		TenantID: middleware.TenantID(c), ChannelKey: request.ChannelKey, Priority: priority,
		Title: request.Notification.Title, Body: request.Notification.Body, ImageURL: request.Notification.Image, Data: request.Data,
		UserIDs: userIDs, ScheduledAt: request.ScheduledAt,
	})
	if writeError(c, err) {
		return
	}
	c.JSON(http.StatusCreated, dtos.CreateInlineCampaignResponse{CampaignID: result.CampaignID.String(), Status: string(result.Status)})
}

func (h *CampaignHandler) AddRecipients(c *gin.Context) {
	var request dtos.AddCampaignRecipientsRequest
	if !bindJSON(c, &request) {
		return
	}
	campaignID, ok := pathUUID(c, "campaign_id")
	if !ok {
		return
	}
	userIDs := make([]domain.UserID, len(request.UserIDs))
	for index, userID := range request.UserIDs {
		userIDs[index] = domain.UserID(userID)
	}
	result, err := h.commands.AddCampaignRecipients(c.Request.Context(), application.AddCampaignRecipientsCommand{
		TenantID: middleware.TenantID(c), CampaignID: campaignID, UserIDs: userIDs,
	})
	if writeError(c, err) {
		return
	}
	c.JSON(http.StatusCreated, dtos.AddCampaignRecipientsResponse{SourceBatchID: result.SourceBatchID.String()})
}

func (h *CampaignHandler) Start(c *gin.Context) {
	var request dtos.StartCampaignRequest
	if !bindJSON(c, &request) {
		return
	}
	campaignID, ok := pathUUID(c, "campaign_id")
	if !ok {
		return
	}
	result, err := h.commands.StartCampaign(c.Request.Context(), application.StartCampaignCommand{
		TenantID: middleware.TenantID(c), CampaignID: campaignID, ScheduledAt: request.ScheduledAt,
	})
	if writeError(c, err) {
		return
	}
	c.JSON(http.StatusOK, dtos.StartCampaignResponse{CampaignID: result.CampaignID.String(), Status: string(result.Status)})
}

func (h *CampaignHandler) Get(c *gin.Context) {
	campaignID, ok := pathUUID(c, "campaign_id")
	if !ok {
		return
	}
	campaign, err := h.queries.GetCampaign(c.Request.Context(), middleware.TenantID(c), campaignID)
	if writeError(c, err) {
		return
	}
	if campaign == nil {
		writeStatus(c, http.StatusNotFound)
		return
	}
	c.JSON(http.StatusOK, dtos.NewCampaignResponse(campaign))
}
