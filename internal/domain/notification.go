package domain

import (
	"time"

	"uuid"
)

// Notification is one logical notification addressed to a user. A fanout may
// create multiple DeliveryWork values for the user's installations, but all
// retain this identity through retries.
type Notification struct {
	id         NotificationID
	campaignID CampaignID
	tenantID   TenantID
	userID     UserID
	createdAt  time.Time
	payload    PushPayload
}

// NewNotificationID allocates the stable identity shared by all delivery
// attempts for one user notification.
func NewNotificationID() NotificationID { return uuid.NewV7() }

type NewNotificationParams struct {
	CampaignID CampaignID
	TenantID   TenantID
	UserID     UserID
	CreatedAt  time.Time
	Payload    PushPayload
}

func NewNotification(params NewNotificationParams) (Notification, error) {
	notification := Notification{
		id: NewNotificationID(), campaignID: params.CampaignID, tenantID: params.TenantID,
		userID: params.UserID, createdAt: params.CreatedAt.UTC(), payload: params.Payload,
	}
	if err := validateNotification(notification); err != nil {
		return Notification{}, err
	}
	return notification, nil
}

func validateNotification(notification Notification) error {
	if err := requireUUID("notification_id", notification.ID()); err != nil {
		return err
	}
	if err := requireUUID("campaign_id", notification.CampaignID()); err != nil {
		return err
	}
	if err := requireUUID("tenant_id", notification.TenantID()); err != nil {
		return err
	}
	if err := requireIdentifier("user_id", string(notification.UserID())); err != nil {
		return err
	}
	if notification.CreatedAt().IsZero() {
		return ErrInvalidArgument
	}
	return notification.Payload().validate()
}

func (n Notification) ID() NotificationID     { return n.id }
func (n Notification) CampaignID() CampaignID { return n.campaignID }
func (n Notification) TenantID() TenantID     { return n.tenantID }
func (n Notification) UserID() UserID         { return n.userID }
func (n Notification) CreatedAt() time.Time   { return n.createdAt }
func (n Notification) Payload() PushPayload   { return n.payload }

type HydrateNotificationParams struct {
	ID         NotificationID
	CampaignID CampaignID
	TenantID   TenantID
	UserID     UserID
	CreatedAt  time.Time
	Payload    PushPayload
}

// HydrateNotification reconstructs a notification previously created during
// fanout and carried by a durable Kafka record.
func HydrateNotification(params HydrateNotificationParams) (Notification, error) {
	notification := Notification{
		id: params.ID, campaignID: params.CampaignID, tenantID: params.TenantID,
		userID: params.UserID, createdAt: params.CreatedAt.UTC(), payload: params.Payload,
	}
	if err := validateNotification(notification); err != nil {
		return Notification{}, err
	}
	return notification, nil
}
