package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestFakeFCMHandlerAcceptsPush(t *testing.T) {
	t.Parallel()

	request := httptest.NewRequest(http.MethodPost, "/send", strings.NewReader(`{"token":"push-token"}`))
	response := httptest.NewRecorder()
	server := newFakeFCM(0, failurePercentage, func() (int, error) { return 5, nil })
	server.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusAccepted)
	}
}

func TestFakeFCMHandlerRejectsEmptyToken(t *testing.T) {
	t.Parallel()

	request := httptest.NewRequest(http.MethodPost, "/send", strings.NewReader(`{"token":""}`))
	response := httptest.NewRecorder()
	server := newFakeFCM(0, failurePercentage, func() (int, error) { return 5, nil })
	server.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusBadRequest)
	}
}

func TestFakeFCMHandlerReturnsServiceUnavailableForSampledFailure(t *testing.T) {
	t.Parallel()

	request := httptest.NewRequest(http.MethodPost, "/send", strings.NewReader(`{"token":"push-token"}`))
	response := httptest.NewRecorder()
	server := newFakeFCM(0, failurePercentage, func() (int, error) { return 0, nil })
	server.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusServiceUnavailable)
	}
}

func TestFakeFCMStatsStartAtZeroAndTrackOutcomes(t *testing.T) {
	t.Parallel()

	server := newFakeFCM(0, failurePercentage, func() (int, error) { return 5, nil })
	handler := server.Handler()
	stats := httptest.NewRecorder()
	handler.ServeHTTP(stats, httptest.NewRequest(http.MethodGet, "/stats", nil))
	if stats.Body.String() != "{\"received_count\":0,\"accepted_count\":0,\"failed_count\":0,\"invalid_request_count\":0,\"active_count\":0,\"max_active_count\":0,\"completed_count\":0,\"max_completed_per_second\":0,\"average_completed_per_second\":0}\n" {
		t.Fatalf("initial stats = %s", stats.Body.String())
	}

	accepted := httptest.NewRecorder()
	handler.ServeHTTP(accepted, httptest.NewRequest(http.MethodPost, "/send", strings.NewReader(`{"token":"push-token"}`)))
	invalid := httptest.NewRecorder()
	handler.ServeHTTP(invalid, httptest.NewRequest(http.MethodPost, "/send", strings.NewReader(`{"token":""}`)))
	stats = httptest.NewRecorder()
	handler.ServeHTTP(stats, httptest.NewRequest(http.MethodGet, "/stats", nil))
	if stats.Body.String() != "{\"received_count\":1,\"accepted_count\":1,\"failed_count\":0,\"invalid_request_count\":1,\"active_count\":0,\"max_active_count\":1,\"completed_count\":1,\"max_completed_per_second\":1,\"average_completed_per_second\":0}\n" {
		t.Fatalf("final stats = %s", stats.Body.String())
	}
}

func TestFakeFCMAverageCompletedPerSecond(t *testing.T) {
	t.Parallel()

	server := newFakeFCM(0, 0, func() (int, error) { return 100, nil })
	server.completed.Store(20)
	server.firstCompletionUnixNano.Store(time.Unix(100, 0).UnixNano())
	server.lastCompletionUnixNano.Store(time.Unix(102, 0).UnixNano())

	if average := server.averageCompletedPerSecond(); average != 10 {
		t.Fatalf("average completed per second = %v, want 10", average)
	}
}

func TestDelayFromEnv(t *testing.T) {
	t.Setenv("FAKE_FCM_RESPONSE_DELAY", "25ms")
	delay, err := delayFromEnv()
	if err != nil {
		t.Fatalf("delay from env: %v", err)
	}
	if delay != 25*time.Millisecond {
		t.Fatalf("delay = %s, want 25ms", delay)
	}
}

func TestFailurePercentageFromEnv(t *testing.T) {
	t.Setenv("FAKE_FCM_FAILURE_PERCENTAGE", "0")
	percentage, err := failurePercentageFromEnv()
	if err != nil {
		t.Fatalf("failure percentage from env: %v", err)
	}
	if percentage != 0 {
		t.Fatalf("failure percentage = %d, want 0", percentage)
	}

	t.Setenv("FAKE_FCM_FAILURE_PERCENTAGE", "101")
	if _, err := failurePercentageFromEnv(); err == nil {
		t.Fatal("failure percentage above 100 was accepted")
	}
}
