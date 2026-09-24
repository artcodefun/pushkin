package providers

import (
	"context"
	"fmt"

	"github.com/superman/pushkin/internal/application/ports"
	"github.com/superman/pushkin/internal/domain"
)

// ProxySender routes a delivery request to the sender for its provider type.
// Bootstrap decides which concrete implementation handles each supported type.
type ProxySender struct {
	fcm ports.PushSender
}

func NewProxySender(fcm ports.PushSender) (*ProxySender, error) {
	if fcm == nil {
		return nil, fmt.Errorf("FCM sender must not be nil")
	}
	return &ProxySender{fcm: fcm}, nil
}

func (s *ProxySender) Send(ctx context.Context, request ports.PushSendRequest) (ports.PushSendResult, error) {
	switch request.ProviderType {
	case domain.ProviderTypeFCM:
		return s.fcm.Send(ctx, request)
	default:
		return ports.PushSendResult{}, fmt.Errorf("unsupported provider type %q", request.ProviderType)
	}
}

var _ ports.PushSender = (*ProxySender)(nil)
