package fcm

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"

	"github.com/superman/pushkin/internal/application/ports"
	"github.com/superman/pushkin/internal/domain"
)

const defaultBaseURL = "https://fcm.googleapis.com"

const firebaseMessagingScope = "https://www.googleapis.com/auth/firebase.messaging"

var _ ports.PushSender = (*Sender)(nil)

type SenderParams struct {
	HTTPClient        *http.Client
	CredentialsCipher ports.CredentialsCipher
	BaseURL           string
}

// Sender sends FCM HTTP v1 requests.
type Sender struct {
	httpClient  *http.Client
	cipher      ports.CredentialsCipher
	baseURL     string
	mu          sync.Mutex
	credentials map[[sha256.Size]byte]cachedCredentials
}

type cachedCredentials struct {
	projectID string
	tokens    oauth2.TokenSource
}

func NewSender(params SenderParams) (*Sender, error) {
	if params.HTTPClient == nil {
		return nil, fmt.Errorf("FCM HTTP client must not be nil")
	}
	if params.CredentialsCipher == nil {
		return nil, fmt.Errorf("FCM credentials cipher must not be nil")
	}
	baseURL := strings.TrimRight(params.BaseURL, "/")
	if baseURL == "" {
		baseURL = defaultBaseURL
	}
	if _, err := url.ParseRequestURI(baseURL); err != nil {
		return nil, fmt.Errorf("parse FCM base URL: %w", err)
	}
	return &Sender{
		httpClient:  params.HTTPClient,
		cipher:      params.CredentialsCipher,
		baseURL:     baseURL,
		credentials: make(map[[sha256.Size]byte]cachedCredentials),
	}, nil
}

func (p *Sender) Send(ctx context.Context, request ports.PushSendRequest) (ports.PushSendResult, error) {
	if request.ProviderType != domain.ProviderTypeFCM {
		return ports.PushSendResult{}, fmt.Errorf("unsupported provider type %q", request.ProviderType)
	}
	if strings.TrimSpace(request.Token) == "" {
		return ports.PushSendResult{}, fmt.Errorf("FCM token must not be empty")
	}
	credentials, err := p.accessToken(ctx, request.EncryptedCredentials)
	if err != nil {
		return ports.PushSendResult{}, fmt.Errorf("resolve FCM access token: %w", err)
	}
	if strings.TrimSpace(credentials.projectID) == "" || strings.TrimSpace(credentials.value) == "" {
		return ports.PushSendResult{}, fmt.Errorf("FCM credentials returned empty project ID or token")
	}

	body, err := json.Marshal(sendRequest{Message: messageFrom(request)})
	if err != nil {
		return ports.PushSendResult{}, fmt.Errorf("marshal FCM request: %w", err)
	}
	httpRequest, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		p.baseURL+"/v1/projects/"+url.PathEscape(credentials.projectID)+"/messages:send",
		bytes.NewReader(body),
	)
	if err != nil {
		return ports.PushSendResult{}, fmt.Errorf("create FCM request: %w", err)
	}
	httpRequest.Header.Set("Authorization", "Bearer "+credentials.value)
	httpRequest.Header.Set("Content-Type", "application/json")

	response, err := p.httpClient.Do(httpRequest)
	if err != nil {
		return ports.PushSendResult{}, fmt.Errorf("send FCM request: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode >= http.StatusOK && response.StatusCode < http.StatusMultipleChoices {
		return ports.PushSendResult{Outcome: ports.PushSendOutcomeAccepted}, nil
	}

	return resultFromResponse(response)
}

type accessToken struct {
	projectID string
	value     string
}

func (p *Sender) accessToken(ctx context.Context, encryptedCredentials string) (accessToken, error) {
	key := sha256.Sum256([]byte(encryptedCredentials))
	p.mu.Lock()
	credentials, found := p.credentials[key]
	p.mu.Unlock()
	if !found {
		plaintext, err := p.cipher.Decrypt(ctx, encryptedCredentials)
		if err != nil {
			return accessToken{}, fmt.Errorf("decrypt FCM credentials: %w", err)
		}
		googleCredentials, err := google.CredentialsFromJSON(context.WithoutCancel(ctx), plaintext, firebaseMessagingScope)
		if err != nil {
			return accessToken{}, fmt.Errorf("parse FCM credentials: %w", err)
		}
		credentials = cachedCredentials{projectID: googleCredentials.ProjectID, tokens: googleCredentials.TokenSource}
		p.mu.Lock()
		if existing, exists := p.credentials[key]; exists {
			credentials = existing
		} else {
			p.credentials[key] = credentials
		}
		p.mu.Unlock()
	}
	token, err := credentials.tokens.Token()
	if err != nil {
		return accessToken{}, fmt.Errorf("request FCM OAuth token: %w", err)
	}
	return accessToken{projectID: credentials.projectID, value: token.AccessToken}, nil
}

type sendRequest struct {
	Message message `json:"message"`
}

type message struct {
	Token        string            `json:"token"`
	Notification *notification     `json:"notification,omitempty"`
	Data         map[string]string `json:"data,omitempty"`
}

type notification struct {
	Title string `json:"title,omitempty"`
	Body  string `json:"body,omitempty"`
	Image string `json:"image,omitempty"`
}

func messageFrom(request ports.PushSendRequest) message {
	payload := request.Payload
	result := message{Token: request.Token, Data: payload.Data()}
	if payload.Title() != "" || payload.Body() != "" || payload.ImageURL() != "" {
		result.Notification = &notification{
			Title: payload.Title(),
			Body:  payload.Body(),
			Image: payload.ImageURL(),
		}
	}
	return result
}

type errorResponse struct {
	Error struct {
		Status  string        `json:"status"`
		Details []errorDetail `json:"details"`
	} `json:"error"`
}

type errorDetail struct {
	Type      string `json:"@type"`
	ErrorCode string `json:"errorCode"`
}

func resultFromResponse(response *http.Response) (ports.PushSendResult, error) {
	responseBody, err := io.ReadAll(io.LimitReader(response.Body, 64<<10))
	if err != nil {
		return ports.PushSendResult{}, fmt.Errorf("read FCM response: %w", err)
	}
	var payload errorResponse
	_ = json.Unmarshal(responseBody, &payload)
	reason := fcmFailureReason(response.StatusCode, payload.Error.Status)

	if hasFCMError(payload.Error.Details, "UNREGISTERED") || hasFCMError(payload.Error.Details, "INVALID_ARGUMENT") {
		return ports.PushSendResult{Outcome: ports.PushSendOutcomeInvalidToken, FailureReason: reason}, nil
	}
	if response.StatusCode == http.StatusTooManyRequests || response.StatusCode == http.StatusRequestTimeout || response.StatusCode >= http.StatusInternalServerError {
		return ports.PushSendResult{
			Outcome:       ports.PushSendOutcomeRetryable,
			FailureReason: reason,
			RetryAfter:    retryAfter(response.Header.Get("Retry-After"), time.Now()),
		}, nil
	}
	return ports.PushSendResult{Outcome: ports.PushSendOutcomeFailed, FailureReason: reason}, nil
}

func hasFCMError(details []errorDetail, code string) bool {
	for _, detail := range details {
		if detail.Type == "type.googleapis.com/google.firebase.fcm.v1.FcmError" && detail.ErrorCode == code {
			return true
		}
	}
	return false
}

func fcmFailureReason(statusCode int, status string) string {
	if status != "" {
		return "fcm_" + strings.ToLower(status)
	}
	return "fcm_http_" + strconv.Itoa(statusCode)
}

func retryAfter(value string, now time.Time) *time.Duration {
	if seconds, err := strconv.Atoi(value); err == nil && seconds >= 0 {
		duration := time.Duration(seconds) * time.Second
		return &duration
	}
	if retryAt, err := http.ParseTime(value); err == nil {
		duration := retryAt.Sub(now)
		if duration < 0 {
			duration = 0
		}
		return &duration
	}
	return nil
}
