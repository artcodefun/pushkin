package ports

import (
	"context"
	"time"

	"github.com/superman/pushkin/internal/domain"
)

// PushSender sends one resolved push request and maps provider-specific
// outcomes into PushSendResult before it returns to application code.
type PushSender interface {
	Send(ctx context.Context, request PushSendRequest) (PushSendResult, error)
}

type PushSendRequest struct {
	ProviderID           domain.ProviderID
	ProviderType         domain.ProviderType
	EncryptedCredentials string
	Token                string
	Payload              domain.PushPayload
}

type PushSendOutcome string

const (
	PushSendOutcomeAccepted     PushSendOutcome = "accepted"
	PushSendOutcomeInvalidToken PushSendOutcome = "invalid_token"
	PushSendOutcomeRetryable    PushSendOutcome = "retryable"
	PushSendOutcomeFailed       PushSendOutcome = "failed"
)

type PushSendResult struct {
	Outcome       PushSendOutcome
	FailureReason string
	RetryAfter    *time.Duration
}

// DeliveryRateLimiter atomically reserves permits from the tenant and provider
// buckets. When Granted is false, the assigned Kafka partition must be paused
// until AvailableAt while its consumer continues heartbeating.
type DeliveryRateLimiter interface {
	Reserve(
		ctx context.Context,
		tenant *domain.Tenant,
		provider *domain.Provider,
		permits int,
	) (reservation DeliveryRateLimitReservation, err error)
}

type DeliveryRateLimitReservation struct {
	Granted     bool
	AvailableAt time.Time
}

// PushCallSemaphore bounds all in-flight provider calls in one pod. Bootstrap
// creates one instance and shares it between delivery workers of every channel.
type PushCallSemaphore interface {
	Acquire(ctx context.Context) error
	Release()
}
