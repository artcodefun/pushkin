package fcm

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/superman/pushkin/internal/application/ports"
	"github.com/superman/pushkin/internal/domain"
)

func TestSenderSendBuildsFCMRequest(t *testing.T) {
	t.Parallel()

	requestErrors := make(chan error, 1)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/v1/projects/example-project/messages:send" {
			requestErrors <- fmt.Errorf("path = %q", request.URL.Path)
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		if authorization := request.Header.Get("Authorization"); authorization != "Bearer access-token" {
			requestErrors <- fmt.Errorf("authorization = %q", authorization)
			writer.WriteHeader(http.StatusUnauthorized)
			return
		}
		var requestBody sendRequest
		if err := json.NewDecoder(request.Body).Decode(&requestBody); err != nil {
			requestErrors <- fmt.Errorf("decode request: %w", err)
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		if requestBody.Message.Token != "device-token" || requestBody.Message.Notification == nil || requestBody.Message.Notification.Image != "https://example.test/image.png" {
			requestErrors <- fmt.Errorf("unexpected message: %+v", requestBody.Message)
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		if requestBody.Message.Data["campaign"] != "spring" {
			requestErrors <- fmt.Errorf("unexpected data: %+v", requestBody.Message.Data)
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		writer.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	sender := newTestSender(t, server.URL)
	result, err := sender.Send(context.Background(), testRequest(t))
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	if result.Outcome != ports.PushSendOutcomeAccepted {
		t.Fatalf("outcome = %q", result.Outcome)
	}
	select {
	case err := <-requestErrors:
		t.Fatal(err)
	default:
	}
}

func TestSenderSendMapsFCMOutcomes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		statusCode     int
		responseBody   string
		retryAfter     string
		wantOutcome    ports.PushSendOutcome
		wantRetryAfter *time.Duration
	}{
		{
			name:         "unregistered token",
			statusCode:   http.StatusNotFound,
			responseBody: `{"error":{"status":"NOT_FOUND","details":[{"@type":"type.googleapis.com/google.firebase.fcm.v1.FcmError","errorCode":"UNREGISTERED"}]}}`,
			wantOutcome:  ports.PushSendOutcomeInvalidToken,
		},
		{
			name:         "invalid token",
			statusCode:   http.StatusBadRequest,
			responseBody: `{"error":{"status":"INVALID_ARGUMENT","details":[{"@type":"type.googleapis.com/google.firebase.fcm.v1.FcmError","errorCode":"INVALID_ARGUMENT"}]}}`,
			wantOutcome:  ports.PushSendOutcomeInvalidToken,
		},
		{
			name:           "rate limited",
			statusCode:     http.StatusTooManyRequests,
			responseBody:   `{"error":{"status":"RESOURCE_EXHAUSTED"}}`,
			retryAfter:     "3",
			wantOutcome:    ports.PushSendOutcomeRetryable,
			wantRetryAfter: durationPointer(3 * time.Second),
		},
		{
			name:         "server failure",
			statusCode:   http.StatusServiceUnavailable,
			responseBody: `{"error":{"status":"UNAVAILABLE"}}`,
			wantOutcome:  ports.PushSendOutcomeRetryable,
		},
		{
			name:         "permanent failure",
			statusCode:   http.StatusForbidden,
			responseBody: `{"error":{"status":"PERMISSION_DENIED"}}`,
			wantOutcome:  ports.PushSendOutcomeFailed,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
				if test.retryAfter != "" {
					writer.Header().Set("Retry-After", test.retryAfter)
				}
				writer.WriteHeader(test.statusCode)
				_, _ = writer.Write([]byte(test.responseBody))
			}))
			defer server.Close()

			sender := newTestSender(t, server.URL)
			result, err := sender.Send(context.Background(), testRequest(t))
			if err != nil {
				t.Fatalf("send: %v", err)
			}
			if result.Outcome != test.wantOutcome {
				t.Fatalf("outcome = %q, want %q", result.Outcome, test.wantOutcome)
			}
			if !equalDurationPointers(result.RetryAfter, test.wantRetryAfter) {
				t.Fatalf("retry after = %v, want %v", result.RetryAfter, test.wantRetryAfter)
			}
		})
	}
}

func TestSenderSendReturnsTransportError(t *testing.T) {
	t.Parallel()

	sender, err := NewSender(SenderParams{
		HTTPClient:        &http.Client{Transport: failingRoundTripper{}},
		CredentialsCipher: testCredentialsCipher{plaintext: testServiceAccountCredentials(t)},
	})
	if err != nil {
		t.Fatalf("new sender: %v", err)
	}
	if _, err := sender.Send(context.Background(), testRequest(t)); err == nil || !strings.Contains(err.Error(), "send FCM request") {
		t.Fatalf("send error = %v", err)
	}
}

func newTestSender(t *testing.T, baseURL string) *Sender {
	t.Helper()
	sender, err := NewSender(SenderParams{
		HTTPClient:        &http.Client{},
		CredentialsCipher: testCredentialsCipher{plaintext: testServiceAccountCredentials(t)},
		BaseURL:           baseURL,
	})
	if err != nil {
		t.Fatalf("new sender: %v", err)
	}
	return sender
}

func testRequest(t *testing.T) ports.PushSendRequest {
	t.Helper()
	payload, err := domain.NewPushPayload("Title", "Body", "https://example.test/image.png", map[string]string{"campaign": "spring"})
	if err != nil {
		t.Fatalf("new payload: %v", err)
	}
	return ports.PushSendRequest{ProviderType: domain.ProviderTypeFCM, EncryptedCredentials: "encrypted-credentials", Token: "device-token", Payload: payload}
}

type testCredentialsCipher struct {
	plaintext []byte
}

func (testCredentialsCipher) Encrypt(context.Context, []byte) (string, error) { return "", nil }

func (c testCredentialsCipher) Decrypt(context.Context, string) ([]byte, error) {
	return append([]byte(nil), c.plaintext...), nil
}

func testServiceAccountCredentials(t *testing.T) []byte {
	t.Helper()
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate private key: %v", err)
	}
	privateKeyDER, err := x509.MarshalPKCS8PrivateKey(privateKey)
	if err != nil {
		t.Fatalf("marshal private key: %v", err)
	}
	tokenServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost {
			writer.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		writer.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(writer, `{"access_token":"access-token","token_type":"Bearer","expires_in":3600}`)
	}))
	t.Cleanup(tokenServer.Close)
	credentials, err := json.Marshal(map[string]string{
		"type":         "service_account",
		"project_id":   "example-project",
		"private_key":  string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: privateKeyDER})),
		"client_email": "pushkin@example-project.iam.gserviceaccount.com",
		"token_uri":    tokenServer.URL,
	})
	if err != nil {
		t.Fatalf("marshal service-account credentials: %v", err)
	}
	return credentials
}

type failingRoundTripper struct{}

func (failingRoundTripper) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, context.DeadlineExceeded
}

func durationPointer(value time.Duration) *time.Duration { return &value }

func equalDurationPointers(actual, expected *time.Duration) bool {
	if actual == nil || expected == nil {
		return actual == expected
	}
	return *actual == *expected
}
