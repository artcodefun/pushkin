package providers

import (
	"context"
	"errors"
	"testing"

	"github.com/superman/pushkin/internal/application/ports"
	"github.com/superman/pushkin/internal/domain"
)

func TestProxySenderRoutesFCMRequests(t *testing.T) {
	t.Parallel()

	fcm := &senderStub{result: ports.PushSendResult{Outcome: ports.PushSendOutcomeAccepted}}
	sender, err := NewProxySender(fcm)
	if err != nil {
		t.Fatalf("new proxy sender: %v", err)
	}
	request := ports.PushSendRequest{ProviderType: domain.ProviderTypeFCM}
	result, err := sender.Send(context.Background(), request)
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	if !fcm.called || fcm.request.ProviderType != request.ProviderType || result.Outcome != ports.PushSendOutcomeAccepted {
		t.Fatalf("unexpected routed request=%+v result=%+v", fcm.request, result)
	}
}

func TestProxySenderRejectsUnsupportedProviderType(t *testing.T) {
	t.Parallel()

	sender, err := NewProxySender(&senderStub{})
	if err != nil {
		t.Fatalf("new proxy sender: %v", err)
	}
	_, err = sender.Send(context.Background(), ports.PushSendRequest{ProviderType: "sms"})
	if err == nil {
		t.Fatal("send error = nil")
	}
}

func TestProxySenderPropagatesSenderError(t *testing.T) {
	t.Parallel()

	want := errors.New("send failed")
	sender, err := NewProxySender(&senderStub{err: want})
	if err != nil {
		t.Fatalf("new proxy sender: %v", err)
	}
	_, err = sender.Send(context.Background(), ports.PushSendRequest{ProviderType: domain.ProviderTypeFCM})
	if !errors.Is(err, want) {
		t.Fatalf("send error = %v, want %v", err, want)
	}
}

type senderStub struct {
	request ports.PushSendRequest
	result  ports.PushSendResult
	err     error
	called  bool
}

func (s *senderStub) Send(_ context.Context, request ports.PushSendRequest) (ports.PushSendResult, error) {
	s.called = true
	s.request = request
	return s.result, s.err
}
