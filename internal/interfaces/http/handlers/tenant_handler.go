package handlers

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/superman/pushkin/internal/application"
	"github.com/superman/pushkin/internal/interfaces/http/dtos"
)

type TenantHandler struct {
	commands application.TenantCommands
	queries  application.TenantQueries
}

func NewTenantHandler(commands application.TenantCommands, queries application.TenantQueries) *TenantHandler {
	return &TenantHandler{commands: commands, queries: queries}
}

func (h *TenantHandler) Create(c *gin.Context) {
	var request dtos.CreateTenantRequest
	if !bindJSON(c, &request) {
		return
	}
	result, err := h.commands.CreateTenant(c.Request.Context(), application.CreateTenantCommand{
		Name: request.Name, RateLimitPerMinute: request.RateLimitPerMinute,
	})
	if writeError(c, err) {
		return
	}
	c.JSON(http.StatusCreated, dtos.CreateTenantResponse{TenantID: result.TenantID.String()})
}

func (h *TenantHandler) List(c *gin.Context) {
	tenants, err := h.queries.ListTenants(c.Request.Context())
	if writeError(c, err) {
		return
	}
	c.JSON(http.StatusOK, dtos.NewListTenantsResponse(tenants))
}
