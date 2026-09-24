package handlers

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"

	"github.com/superman/pushkin/internal/application"
	"github.com/superman/pushkin/internal/domain"
)

func TestWriteErrorMapsAllApplicationAndDomainErrors(t *testing.T) {
	testCases := []struct {
		name       string
		err        error
		wantStatus int
	}{
		{name: "application not found", err: application.ErrNotFound, wantStatus: http.StatusNotFound},
		{name: "application not authorized", err: application.ErrNotAuthorized, wantStatus: http.StatusUnauthorized},
		{name: "application validation", err: application.ErrValidation, wantStatus: http.StatusBadRequest},
		{name: "application conflict", err: application.ErrConflict, wantStatus: http.StatusConflict},
		{name: "application unavailable", err: application.ErrUnavailable, wantStatus: http.StatusServiceUnavailable},
		{name: "domain invalid argument", err: domain.ErrInvalidArgument, wantStatus: http.StatusBadRequest},
		{name: "domain invalid transition", err: domain.ErrInvalidTransition, wantStatus: http.StatusConflict},
		{name: "domain run mismatch", err: domain.ErrRunMismatch, wantStatus: http.StatusConflict},
		{name: "wrapped domain error", err: fmt.Errorf("operation: %w", domain.ErrInvalidArgument), wantStatus: http.StatusBadRequest},
		{name: "unexpected error", err: errors.New("unexpected"), wantStatus: http.StatusInternalServerError},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			response := httptest.NewRecorder()
			context, _ := gin.CreateTestContext(response)
			if !writeError(context, testCase.err) {
				t.Fatal("writeError returned false")
			}
			if response.Code != testCase.wantStatus {
				t.Fatalf("status = %d, want %d", response.Code, testCase.wantStatus)
			}
		})
	}
}

func TestWriteErrorReturnsFalseWithoutError(t *testing.T) {
	response := httptest.NewRecorder()
	context, _ := gin.CreateTestContext(response)
	if writeError(context, nil) {
		t.Fatal("writeError returned true")
	}
}
