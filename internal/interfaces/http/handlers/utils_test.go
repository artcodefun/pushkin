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

func TestOptionalPositiveQueryInt(t *testing.T) {
	testCases := []struct {
		query   string
		want    int
		wantErr bool
	}{
		{query: "", want: 0},
		{query: "?limit=25", want: 25},
		{query: "?limit=0", wantErr: true},
		{query: "?limit=invalid", wantErr: true},
	}

	for _, testCase := range testCases {
		t.Run(testCase.query, func(t *testing.T) {
			context, _ := gin.CreateTestContext(httptest.NewRecorder())
			context.Request = httptest.NewRequest(http.MethodGet, "/notifications"+testCase.query, nil)
			value, err := optionalPositiveQueryInt(context, "limit")
			if (err != nil) != testCase.wantErr || value != testCase.want {
				t.Fatalf("optionalPositiveQueryInt() = %d, %v; want %d, error=%t", value, err, testCase.want, testCase.wantErr)
			}
		})
	}
}
