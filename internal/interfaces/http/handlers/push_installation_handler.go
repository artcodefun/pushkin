package handlers

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/superman/pushkin/internal/application"
	"github.com/superman/pushkin/internal/domain"
	"github.com/superman/pushkin/internal/interfaces/http/dtos"
	"github.com/superman/pushkin/internal/interfaces/http/middleware"
)

type PushInstallationHandler struct {
	commands application.PushInstallationCommands
}

func NewPushInstallationHandler(commands application.PushInstallationCommands) *PushInstallationHandler {
	return &PushInstallationHandler{commands: commands}
}

func (h *PushInstallationHandler) Register(c *gin.Context) {
	var request dtos.RegisterPushInstallationRequest
	if !bindJSON(c, &request) {
		return
	}
	if writeError(c, h.commands.RegisterPushInstallation(c.Request.Context(), application.RegisterPushInstallationCommand{
		TenantID: middleware.TenantID(c), UserID: domain.UserID(request.UserID), Platform: domain.MobilePlatform(request.Platform),
		PackageName: request.PackageName, InstallationID: request.InstallationID, Token: request.Token,
	})) {
		return
	}
	writeStatus(c, http.StatusNoContent)
}
