package handlers

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/superman/pushkin/internal/application"
	"github.com/superman/pushkin/internal/interfaces/http/dtos"
)

type TenantAPIKeyHandler struct {
	commands application.TenantAPIKeyCommands
}

func NewTenantAPIKeyHandler(commands application.TenantAPIKeyCommands) *TenantAPIKeyHandler {
	return &TenantAPIKeyHandler{commands: commands}
}

func (h *TenantAPIKeyHandler) Issue(c *gin.Context) {
	var request dtos.IssueTenantAPIKeyRequest
	if !bindJSON(c, &request) {
		return
	}
	tenantID, ok := parseUUID(c, request.TenantID)
	if !ok {
		return
	}
	result, err := h.commands.IssueTenantAPIKey(c.Request.Context(), application.IssueTenantAPIKeyCommand{
		TenantID: tenantID, Name: request.Name,
	})
	if writeError(c, err) {
		return
	}
	c.JSON(http.StatusCreated, dtos.IssueTenantAPIKeyResponse{APIKeyID: result.APIKeyID.String(), APIKey: result.APIKey})
}

func (h *TenantAPIKeyHandler) Revoke(c *gin.Context) {
	apiKeyID, ok := pathUUID(c, "api_key_id")
	if !ok {
		return
	}
	if writeError(c, h.commands.RevokeTenantAPIKey(c.Request.Context(), application.RevokeTenantAPIKeyCommand{APIKeyID: apiKeyID})) {
		return
	}
	writeStatus(c, http.StatusNoContent)
}
