package domain

import (
	"errors"
	"testing"
	"uuid"
)

func TestDeliveryRetryKeepsDeliveryID(t *testing.T) {
	t.Parallel()

	work, err := NewDeliveryWork(NewDeliveryWorkParams{
		CampaignID:         uuid.NewV7(),
		TenantID:           uuid.NewV7(),
		ChannelID:          uuid.NewV7(),
		PushInstallationID: uuid.NewV7(),
		Priority:           PriorityNormal,
	})
	if err != nil {
		t.Fatalf("new delivery: %v", err)
	}
	id := work.ID()
	for range MaxDeliveryRetryAttempts {
		if err := work.IncreaseRetry(); err != nil {
			t.Fatalf("increase retry: %v", err)
		}
	}
	if work.ID() != id || work.RetryAttempt() != MaxDeliveryRetryAttempts {
		t.Fatal("retry changed delivery identity or count")
	}
	if err := work.IncreaseRetry(); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("expected retry budget exhaustion, got %v", err)
	}
}

func newTestDeliveryWork(t *testing.T) DeliveryWork {
	t.Helper()
	work, err := NewDeliveryWork(NewDeliveryWorkParams{
		CampaignID:         uuid.NewV7(),
		TenantID:           uuid.NewV7(),
		ChannelID:          uuid.NewV7(),
		PushInstallationID: uuid.NewV7(),
		Priority:           PriorityNormal,
	})
	if err != nil {
		t.Fatalf("new delivery work: %v", err)
	}
	return work
}
