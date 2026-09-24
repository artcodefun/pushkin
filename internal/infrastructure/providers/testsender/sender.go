package testsender

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/superman/pushkin/internal/application/ports"
	"github.com/superman/pushkin/internal/domain"
)

type SenderParams struct {
	HTTPClient *http.Client
	BaseURL    string
}

// Sender sends push requests to the local test sender service. It exists only
// for deterministic non-production pipeline testing.
type Sender struct {
	httpClient *http.Client
	baseURL    string
}

func NewSender(params SenderParams) (*Sender, error) {
	if params.HTTPClient == nil {
		return nil, fmt.Errorf("test sender HTTP client must not be nil")
	}
	baseURL := strings.TrimRight(params.BaseURL, "/")
	if baseURL == "" {
		return nil, fmt.Errorf("test sender URL must not be empty")
	}
	if _, err := url.ParseRequestURI(baseURL); err != nil {
		return nil, fmt.Errorf("parse test sender URL: %w", err)
	}
	return &Sender{httpClient: params.HTTPClient, baseURL: baseURL}, nil
}

func (s *Sender) Send(ctx context.Context, request ports.PushSendRequest) (ports.PushSendResult, error) {
	if request.ProviderType != domain.ProviderTypeFCM {
		return ports.PushSendResult{}, fmt.Errorf("unsupported provider type %q", request.ProviderType)
	}
	if strings.TrimSpace(request.Token) == "" {
		return ports.PushSendResult{}, fmt.Errorf("test sender token must not be empty")
	}
	body, err := json.Marshal(sendRequest{Token: request.Token, Payload: payloadFrom(request.Payload)})
	if err != nil {
		return ports.PushSendResult{}, fmt.Errorf("marshal test sender request: %w", err)
	}
	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, s.baseURL+"/send", bytes.NewReader(body))
	if err != nil {
		return ports.PushSendResult{}, fmt.Errorf("create test sender request: %w", err)
	}
	httpRequest.Header.Set("Content-Type", "application/json")
	response, err := s.httpClient.Do(httpRequest)
	if err != nil {
		return ports.PushSendResult{}, fmt.Errorf("send test push: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode >= http.StatusOK && response.StatusCode < http.StatusMultipleChoices {
		return ports.PushSendResult{Outcome: ports.PushSendOutcomeAccepted}, nil
	}
	if response.StatusCode == http.StatusTooManyRequests || response.StatusCode >= http.StatusInternalServerError {
		return ports.PushSendResult{
			Outcome:       ports.PushSendOutcomeRetryable,
			FailureReason: "test_sender_http_" + strconv.Itoa(response.StatusCode),
		}, nil
	}
	return ports.PushSendResult{
		Outcome:       ports.PushSendOutcomeFailed,
		FailureReason: "test_sender_http_" + strconv.Itoa(response.StatusCode),
	}, nil
}

type sendRequest struct {
	Token   string       `json:"token"`
	Payload payloadValue `json:"payload"`
}

type payloadValue struct {
	Title string            `json:"title,omitempty"`
	Body  string            `json:"body,omitempty"`
	Data  map[string]string `json:"data,omitempty"`
	Image string            `json:"image,omitempty"`
}

func payloadFrom(payload domain.PushPayload) payloadValue {
	return payloadValue{Title: payload.Title(), Body: payload.Body(), Data: payload.Data(), Image: payload.ImageURL()}
}

var _ ports.PushSender = (*Sender)(nil)
