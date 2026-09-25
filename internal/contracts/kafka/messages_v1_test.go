package kafka

import (
	"testing"
	"time"
	"uuid"

	"github.com/superman/pushkin/internal/domain"
)

func TestDeliveryTopic(t *testing.T) {
	t.Parallel()

	channelID := uuid.NewV7()
	topic, err := DeliveryTopic(PriorityHighV1, channelID)
	if err != nil {
		t.Fatalf("delivery topic: %v", err)
	}
	want := Topic("pushkin.delivery.high." + channelID.String())
	if topic != want {
		t.Fatalf("unexpected topic: got %q want %q", topic, want)
	}
	if _, err := DeliveryTopic("unknown", channelID); err == nil {
		t.Fatal("invalid priority must be rejected")
	}
}

func TestNewDeliveryWorkV1IncludesNotificationIdentity(t *testing.T) {
	t.Parallel()

	campaignID := uuid.NewV7()
	tenantID := uuid.NewV7()
	work, err := domain.NewDeliveryWork(domain.NewDeliveryWorkParams{
		CampaignID: campaignID, TenantID: tenantID, NotificationID: domain.NewNotificationID(), UserID: "user-1", NotificationCreatedAt: time.Now(), ChannelID: uuid.NewV7(),
		PushInstallationID: uuid.NewV7(), Priority: domain.PriorityHigh,
	})
	if err != nil {
		t.Fatalf("new delivery work: %v", err)
	}
	message := NewDeliveryWorkV1(work)
	if message.NotificationID != work.NotificationID() || message.UserID != string(work.UserID()) || !message.NotificationCreatedAt.Equal(work.NotificationCreatedAt()) {
		t.Fatalf("unexpected notification delivery work: %+v", message)
	}
}

func TestRetryTopic(t *testing.T) {
	t.Parallel()

	channelID := uuid.NewV7()
	topic, err := RetryTopic(RetryBucketFiveMinutesV1, channelID)
	if err != nil {
		t.Fatalf("retry topic: %v", err)
	}
	want := Topic("pushkin.retry.5m." + channelID.String())
	if topic != want {
		t.Fatalf("unexpected topic: got %q want %q", topic, want)
	}
	if _, err := RetryTopic("10m", channelID); err == nil {
		t.Fatal("invalid retry bucket must be rejected")
	}
}

func TestCampaignRunTopic(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mode CampaignRecipientModeV1
		want Topic
	}{
		{mode: CampaignRecipientModeBatchedV1, want: TopicCampaignBatchedRun},
		{mode: CampaignRecipientModeInlineV1, want: TopicCampaignInlineRun},
	}
	for _, test := range tests {
		topic, err := CampaignRunTopic(test.mode)
		if err != nil {
			t.Fatalf("campaign run topic for %q: %v", test.mode, err)
		}
		if topic != test.want {
			t.Fatalf("campaign run topic for %q = %q, want %q", test.mode, topic, test.want)
		}
	}
	if _, err := CampaignRunTopic(CampaignRecipientModeV1("unknown")); err == nil {
		t.Fatal("invalid campaign mode must be rejected")
	}
}

func TestNewMessageHeaderV1(t *testing.T) {
	t.Parallel()

	header := NewMessageHeaderV1()
	if header.SchemaVersion != SchemaVersionV1 {
		t.Fatalf("unexpected message header: %+v", header)
	}
}

func TestNewDeliveryWorkV1(t *testing.T) {
	t.Parallel()

	campaignID := uuid.NewV7()
	tenantID := uuid.NewV7()
	work, err := domain.NewDeliveryWork(domain.NewDeliveryWorkParams{
		CampaignID:            campaignID,
		TenantID:              tenantID,
		NotificationID:        domain.NewNotificationID(),
		UserID:                "user-1",
		NotificationCreatedAt: time.Now(),
		ChannelID:             uuid.NewV7(),
		PushInstallationID:    uuid.NewV7(),
		Priority:              domain.PriorityHigh,
	})
	if err != nil {
		t.Fatalf("new delivery work: %v", err)
	}

	message := NewDeliveryWorkV1(work)
	if message.MessageHeaderV1 != NewMessageHeaderV1() ||
		message.DeliveryID != work.ID() ||
		message.CampaignID != work.CampaignID() ||
		message.TenantID != work.TenantID() ||
		message.NotificationID != work.NotificationID() ||
		message.UserID != string(work.UserID()) ||
		message.ChannelID != work.ChannelID() ||
		message.PushInstallationID != work.PushInstallationID() ||
		message.Priority != string(work.Priority()) ||
		message.RetryAttempt != work.RetryAttempt() {
		t.Fatalf("unexpected delivery work message: %+v", message)
	}
}
