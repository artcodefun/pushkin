package testsender

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"uuid"

	"github.com/superman/pushkin/internal/application/ports"
	"github.com/superman/pushkin/internal/domain"
)

func TestSenderPostsPushRequest(t *testing.T) {
	t.Parallel()

	requests := make(chan sendRequest, 1)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost || request.URL.Path != "/send" {
			writer.WriteHeader(http.StatusNotFound)
			return
		}
		var value sendRequest
		if err := json.NewDecoder(request.Body).Decode(&value); err != nil {
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		requests <- value
		writer.WriteHeader(http.StatusAccepted)
	}))
	t.Cleanup(server.Close)

	sender, err := NewSender(SenderParams{HTTPClient: server.Client(), BaseURL: server.URL})
	if err != nil {
		t.Fatalf("new sender: %v", err)
	}
	payload, err := domain.NewPushPayload("Title", "Body", "", map[string]string{"key": "value"})
	if err != nil {
		t.Fatalf("new payload: %v", err)
	}
	result, err := sender.Send(context.Background(), ports.PushSendRequest{ProviderID: uuid.NewV7(), ProviderType: domain.ProviderTypeFCM, Token: "token", Payload: payload})
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	if result.Outcome != ports.PushSendOutcomeAccepted {
		t.Fatalf("unexpected result: %+v", result)
	}
	request := <-requests
	if request.Token != "token" || request.Payload.Title != "Title" || request.Payload.Data["key"] != "value" {
		t.Fatalf("unexpected test sender request: %+v", request)
	}
}

func TestSenderMapsServiceUnavailableToRetryable(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusServiceUnavailable)
	}))
	t.Cleanup(server.Close)
	sender, err := NewSender(SenderParams{HTTPClient: server.Client(), BaseURL: server.URL})
	if err != nil {
		t.Fatalf("new sender: %v", err)
	}
	payload, err := domain.NewPushPayload("Title", "", "", nil)
	if err != nil {
		t.Fatalf("new payload: %v", err)
	}
	result, err := sender.Send(context.Background(), ports.PushSendRequest{ProviderType: domain.ProviderTypeFCM, Token: "token", Payload: payload})
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	if result.Outcome != ports.PushSendOutcomeRetryable || result.FailureReason != "test_sender_http_503" {
		t.Fatalf("unexpected result: %+v", result)
	}
}
