package handlers

import (
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"
	"uuid"

	"github.com/superman/pushkin/internal/application"
	"github.com/superman/pushkin/internal/domain"
)

func bindJSON(c *gin.Context, request any) bool {
	if err := c.ShouldBindJSON(request); err != nil {
		writeStatus(c, http.StatusBadRequest)
		return false
	}
	return true
}

func pathUUID(c *gin.Context, name string) (uuid.UUID, bool) {
	return parseUUID(c, c.Param(name))
}

func parseUUID(c *gin.Context, value string) (uuid.UUID, bool) {
	id, err := uuid.Parse(value)
	if err != nil {
		writeStatus(c, http.StatusBadRequest)
		return uuid.Nil(), false
	}
	return id, true
}

func writeError(c *gin.Context, err error) bool {
	if err == nil {
		return false
	}
	writeStatus(c, statusForError(err))
	return true
}

func writeStatus(c *gin.Context, status int) {
	c.Status(status)
	c.Writer.WriteHeaderNow()
}

func statusForError(err error) int {
	switch {
	case errors.Is(err, application.ErrNotFound):
		return http.StatusNotFound
	case errors.Is(err, application.ErrNotAuthorized):
		return http.StatusUnauthorized
	case errors.Is(err, application.ErrValidation):
		return http.StatusBadRequest
	case errors.Is(err, application.ErrConflict):
		return http.StatusConflict
	case errors.Is(err, application.ErrUnavailable):
		return http.StatusServiceUnavailable
	case errors.Is(err, domain.ErrInvalidArgument):
		return http.StatusBadRequest
	case errors.Is(err, domain.ErrInvalidTransition), errors.Is(err, domain.ErrRunMismatch):
		return http.StatusConflict
	default:
		return http.StatusInternalServerError
	}
}
