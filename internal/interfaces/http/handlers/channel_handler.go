package handlers

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/superman/pushkin/internal/application"
	"github.com/superman/pushkin/internal/domain"
	"github.com/superman/pushkin/internal/interfaces/http/dtos"
	"github.com/superman/pushkin/internal/interfaces/http/middleware"
)

type ChannelHandler struct {
	commands application.ChannelCommands
	queries  application.ChannelQueries
}

func NewChannelHandler(commands application.ChannelCommands, queries application.ChannelQueries) *ChannelHandler {
	return &ChannelHandler{commands: commands, queries: queries}
}

func (h *ChannelHandler) Create(c *gin.Context) {
	var request dtos.CreateChannelRequest
	if !bindJSON(c, &request) {
		return
	}
	providerID, ok := parseUUID(c, request.ProviderID)
	if !ok {
		return
	}
	result, err := h.commands.CreateChannel(c.Request.Context(), application.CreateChannelCommand{
		TenantID: middleware.TenantID(c), ProviderID: providerID, Type: domain.ChannelType(request.Type), Key: request.Key,
	})
	if writeError(c, err) {
		return
	}
	c.JSON(http.StatusCreated, dtos.CreateChannelResponse{ChannelID: result.ChannelID.String()})
}

func (h *ChannelHandler) Get(c *gin.Context) {
	channelID, ok := pathUUID(c, "channel_id")
	if !ok {
		return
	}
	channel, err := h.queries.GetChannel(c.Request.Context(), middleware.TenantID(c), channelID)
	if writeError(c, err) {
		return
	}
	if channel == nil {
		writeStatus(c, http.StatusNotFound)
		return
	}
	c.JSON(http.StatusOK, dtos.NewChannelResponse(channel))
}

func (h *ChannelHandler) List(c *gin.Context) {
	channels, err := h.queries.ListChannels(c.Request.Context(), middleware.TenantID(c))
	if writeError(c, err) {
		return
	}
	c.JSON(http.StatusOK, dtos.NewListChannelsResponse(channels))
}

func (h *ChannelHandler) LinkMobileApplication(c *gin.Context) {
	channelID, ok := pathUUID(c, "channel_id")
	if !ok {
		return
	}
	mobileApplicationID, ok := pathUUID(c, "mobile_application_id")
	if !ok {
		return
	}
	err := h.commands.LinkMobileApplicationToChannel(c.Request.Context(), application.LinkMobileApplicationToChannelCommand{
		TenantID: middleware.TenantID(c), ChannelID: channelID, MobileApplicationID: mobileApplicationID,
	})
	if writeError(c, err) {
		return
	}
	writeStatus(c, http.StatusNoContent)
}

func (h *ChannelHandler) UnlinkMobileApplication(c *gin.Context) {
	channelID, ok := pathUUID(c, "channel_id")
	if !ok {
		return
	}
	mobileApplicationID, ok := pathUUID(c, "mobile_application_id")
	if !ok {
		return
	}
	err := h.commands.UnlinkMobileApplicationFromChannel(c.Request.Context(), application.LinkMobileApplicationToChannelCommand{
		TenantID: middleware.TenantID(c), ChannelID: channelID, MobileApplicationID: mobileApplicationID,
	})
	if writeError(c, err) {
		return
	}
	writeStatus(c, http.StatusNoContent)
}
