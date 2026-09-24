package handlers

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/superman/pushkin/internal/application"
	"github.com/superman/pushkin/internal/domain"
	"github.com/superman/pushkin/internal/interfaces/http/dtos"
	"github.com/superman/pushkin/internal/interfaces/http/middleware"
)

type MobileApplicationHandler struct {
	commands application.MobileApplicationCommands
	queries  application.MobileApplicationQueries
}

func NewMobileApplicationHandler(commands application.MobileApplicationCommands, queries application.MobileApplicationQueries) *MobileApplicationHandler {
	return &MobileApplicationHandler{commands: commands, queries: queries}
}

func (h *MobileApplicationHandler) Create(c *gin.Context) {
	var request dtos.CreateMobileApplicationRequest
	if !bindJSON(c, &request) {
		return
	}
	providerID, ok := parseUUID(c, request.ProviderID)
	if !ok {
		return
	}
	result, err := h.commands.CreateMobileApplication(c.Request.Context(), application.CreateMobileApplicationCommand{
		TenantID: middleware.TenantID(c), ProviderID: providerID, Platform: domain.MobilePlatform(request.Platform), PackageName: request.PackageName,
	})
	if writeError(c, err) {
		return
	}
	c.JSON(http.StatusCreated, dtos.CreateMobileApplicationResponse{MobileApplicationID: result.MobileApplicationID.String()})
}

func (h *MobileApplicationHandler) Get(c *gin.Context) {
	mobileApplicationID, ok := pathUUID(c, "mobile_application_id")
	if !ok {
		return
	}
	mobileApplication, err := h.queries.GetMobileApplication(c.Request.Context(), middleware.TenantID(c), mobileApplicationID)
	if writeError(c, err) {
		return
	}
	if mobileApplication == nil {
		writeStatus(c, http.StatusNotFound)
		return
	}
	c.JSON(http.StatusOK, dtos.NewMobileApplicationResponse(mobileApplication))
}

func (h *MobileApplicationHandler) List(c *gin.Context) {
	mobileApplications, err := h.queries.ListMobileApplications(c.Request.Context(), middleware.TenantID(c))
	if writeError(c, err) {
		return
	}
	c.JSON(http.StatusOK, dtos.NewListMobileApplicationsResponse(mobileApplications))
}

func (h *MobileApplicationHandler) ConnectProvider(c *gin.Context) {
	mobileApplicationID, ok := pathUUID(c, "mobile_application_id")
	if !ok {
		return
	}
	var request dtos.ConnectMobileApplicationProviderRequest
	if !bindJSON(c, &request) {
		return
	}
	providerID, ok := parseUUID(c, request.ProviderID)
	if !ok {
		return
	}
	err := h.commands.ConnectMobileApplicationProvider(c.Request.Context(), application.ConnectMobileApplicationProviderCommand{
		TenantID: middleware.TenantID(c), MobileApplicationID: mobileApplicationID, ProviderID: providerID,
	})
	if writeError(c, err) {
		return
	}
	writeStatus(c, http.StatusNoContent)
}

func (h *MobileApplicationHandler) DisconnectProvider(c *gin.Context) {
	mobileApplicationID, ok := pathUUID(c, "mobile_application_id")
	if !ok {
		return
	}
	err := h.commands.DisconnectMobileApplicationProvider(c.Request.Context(), application.DisconnectMobileApplicationProviderCommand{
		TenantID: middleware.TenantID(c), MobileApplicationID: mobileApplicationID,
	})
	if writeError(c, err) {
		return
	}
	writeStatus(c, http.StatusNoContent)
}
