package handlers

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/superman/pushkin/internal/application"
	"github.com/superman/pushkin/internal/domain"
	"github.com/superman/pushkin/internal/interfaces/http/dtos"
	"github.com/superman/pushkin/internal/interfaces/http/middleware"
)

type NotificationHandler struct {
	commands application.NotificationCommands
	queries  application.NotificationQueries
}

func NewNotificationHandler(commands application.NotificationCommands, queries application.NotificationQueries) *NotificationHandler {
	return &NotificationHandler{commands: commands, queries: queries}
}

func (h *NotificationHandler) List(c *gin.Context) {
	limit, err := optionalPositiveQueryInt(c, "limit")
	if err != nil {
		writeStatus(c, http.StatusBadRequest)
		return
	}
	page, err := h.queries.ListNotifications(c.Request.Context(), middleware.TenantID(c), domain.UserID(c.Param("user_id")), c.Query("cursor"), limit)
	if writeError(c, err) {
		return
	}
	c.JSON(http.StatusOK, dtos.NewNotificationPageResponse(page))
}

func (h *NotificationHandler) MarkRead(c *gin.Context) {
	notificationID, ok := pathUUID(c, "notification_id")
	if !ok {
		return
	}
	if writeError(c, h.commands.MarkNotificationRead(c.Request.Context(), application.MarkNotificationReadCommand{
		TenantID: middleware.TenantID(c), UserID: domain.UserID(c.Param("user_id")), NotificationID: notificationID,
	})) {
		return
	}
	writeStatus(c, http.StatusNoContent)
}
