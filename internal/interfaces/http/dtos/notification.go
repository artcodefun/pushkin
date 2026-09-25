package dtos

import (
	"time"

	"github.com/superman/pushkin/internal/application/queries/readmodels"
)

type NotificationResponse struct {
	NotificationID string            `json:"notification_id"`
	CampaignID     string            `json:"campaign_id"`
	CreatedAt      time.Time         `json:"created_at"`
	Notification   Notification      `json:"notification"`
	Data           map[string]string `json:"data"`
	Read           bool              `json:"read"`
}

type NotificationPageResponse struct {
	Notifications []NotificationResponse `json:"notifications"`
	NextCursor    string                 `json:"next_cursor,omitempty"`
}

func NewNotificationResponse(notification readmodels.Notification) NotificationResponse {
	return NotificationResponse{
		NotificationID: notification.ID.String(), CampaignID: notification.CampaignID.String(), CreatedAt: notification.CreatedAt,
		Notification: Notification{Title: notification.Title, Body: notification.Body, Image: notification.ImageURL},
		Data:         notification.Data, Read: notification.Read,
	}
}

func NewNotificationPageResponse(page readmodels.NotificationPage) NotificationPageResponse {
	response := NotificationPageResponse{Notifications: make([]NotificationResponse, len(page.Notifications)), NextCursor: page.NextCursor}
	for index, notification := range page.Notifications {
		response.Notifications[index] = NewNotificationResponse(notification)
	}
	return response
}
