package domain

import (
	"testing"
	"time"
	"uuid"
)

func TestNewNotificationIDCreatesInternalIdentity(t *testing.T) {
	t.Parallel()

	if id := NewNotificationID(); id == uuid.Nil() {
		t.Fatal("notification ID must not be nil")
	}
}

func TestNewNotificationKeepsReferenceAndPayload(t *testing.T) {
	t.Parallel()

	payload, err := NewPushPayload("title", "body", "", map[string]string{"kind": "test"})
	if err != nil {
		t.Fatalf("new push payload: %v", err)
	}
	notification, err := NewNotification(NewNotificationParams{
		CampaignID: uuid.NewV7(), TenantID: uuid.NewV7(), UserID: "user-1", CreatedAt: time.Now(), Payload: payload,
	})
	if err != nil {
		t.Fatalf("new notification: %v", err)
	}
	if notification.ID() == uuid.Nil() || notification.UserID() != "user-1" || notification.Payload().Title() != "title" {
		t.Fatalf("unexpected notification: %+v", notification)
	}
}
