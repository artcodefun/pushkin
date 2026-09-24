package handlers

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/superman/pushkin/internal/application"
	"github.com/superman/pushkin/internal/domain"
	"github.com/superman/pushkin/internal/interfaces/http/dtos"
	"github.com/superman/pushkin/internal/interfaces/http/middleware"
)

type ProviderHandler struct {
	commands application.ProviderCommands
	queries  application.ProviderQueries
}

func NewProviderHandler(commands application.ProviderCommands, queries application.ProviderQueries) *ProviderHandler {
	return &ProviderHandler{commands: commands, queries: queries}
}

func (h *ProviderHandler) Create(c *gin.Context) {
	var request dtos.CreateProviderRequest
	if !bindJSON(c, &request) {
		return
	}
	result, err := h.commands.CreateProvider(c.Request.Context(), application.CreateProviderCommand{
		TenantID: middleware.TenantID(c), Type: domain.ProviderType(request.Type), Credentials: []byte(request.Credentials),
		RateLimitQPS: request.RateLimitQPS, RateLimitBurst: request.RateLimitBurst,
	})
	if writeError(c, err) {
		return
	}
	c.JSON(http.StatusCreated, dtos.CreateProviderResponse{ProviderID: result.ProviderID.String()})
}

func (h *ProviderHandler) Get(c *gin.Context) {
	providerID, ok := pathUUID(c, "provider_id")
	if !ok {
		return
	}
	provider, err := h.queries.GetProvider(c.Request.Context(), middleware.TenantID(c), providerID)
	if writeError(c, err) {
		return
	}
	if provider == nil {
		writeStatus(c, http.StatusNotFound)
		return
	}
	c.JSON(http.StatusOK, dtos.NewProviderResponse(provider))
}

func (h *ProviderHandler) List(c *gin.Context) {
	providers, err := h.queries.ListProviders(c.Request.Context(), middleware.TenantID(c))
	if writeError(c, err) {
		return
	}
	c.JSON(http.StatusOK, dtos.NewListProvidersResponse(providers))
}
