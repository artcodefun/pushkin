package readmodels

import (
	"time"
	"uuid"
)

// Notification is the user-facing projection of one provider-accepted
// logical notification. It is intentionally independent from device delivery
// attempts and campaign progress accounting.
type Notification struct {
	ID         uuid.UUID
	CampaignID uuid.UUID
	CreatedAt  time.Time
	Title      string
	Body       string
	ImageURL   string
	Data       map[string]string
	Read       bool
}

type NotificationPage struct {
	Notifications []Notification
	NextCursor    string
}
