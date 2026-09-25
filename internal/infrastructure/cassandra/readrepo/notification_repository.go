package readrepo

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"
	"uuid"

	"github.com/apache/cassandra-gocql-driver/v2"

	"github.com/superman/pushkin/internal/application/ports"
	"github.com/superman/pushkin/internal/application/queries/readmodels"
	"github.com/superman/pushkin/internal/domain"
)

const listNotifications = `SELECT created_at, notification_id, campaign_id, title, body, image_url, data
FROM notifications_by_user WHERE tenant_id = ? AND user_id = ?`

const findReadState = `SELECT read_at FROM notification_read_state_by_user
WHERE tenant_id = ? AND user_id = ? AND notification_id = ?`

type NotificationRepository struct{ session *gocql.Session }

func NewNotificationRepository(session *gocql.Session) *NotificationRepository {
	return &NotificationRepository{session: session}
}

func (r *NotificationRepository) List(ctx context.Context, tenantID domain.TenantID, userID domain.UserID, cursor string, limit int) (readmodels.NotificationPage, error) {
	pageState, err := decodePageState(cursor)
	if err != nil {
		return readmodels.NotificationPage{}, err
	}
	iter := r.session.Query(listNotifications, tenantID.String(), string(userID)).PageSize(limit).PageState(pageState).IterContext(ctx)
	page := readmodels.NotificationPage{Notifications: make([]readmodels.Notification, 0, limit)}
	for {
		notification, err := scanNotification(iter)
		if err != nil {
			if err == gocql.ErrNotFound {
				break
			}
			iter.Close()
			return readmodels.NotificationPage{}, err
		}
		page.Notifications = append(page.Notifications, notification)
	}
	nextState := iter.PageState()
	if err := iter.Close(); err != nil {
		return readmodels.NotificationPage{}, fmt.Errorf("list notifications: %w", err)
	}
	if err := r.attachReadState(ctx, tenantID, userID, page.Notifications); err != nil {
		return readmodels.NotificationPage{}, err
	}
	page.NextCursor = encodePageState(nextState)
	return page, nil
}

func scanNotification(iter *gocql.Iter) (readmodels.Notification, error) {
	var notification readmodels.Notification
	var notificationID, campaignID, encodedData string
	if !iter.Scan(&notification.CreatedAt, &notificationID, &campaignID, &notification.Title, &notification.Body, &notification.ImageURL, &encodedData) {
		return readmodels.Notification{}, gocql.ErrNotFound
	}
	var err error
	notification.ID, err = uuid.Parse(notificationID)
	if err != nil {
		return readmodels.Notification{}, fmt.Errorf("decode notification ID: %w", err)
	}
	notification.CampaignID, err = uuid.Parse(campaignID)
	if err != nil {
		return readmodels.Notification{}, fmt.Errorf("decode notification campaign ID: %w", err)
	}
	if err := json.Unmarshal([]byte(encodedData), &notification.Data); err != nil {
		return readmodels.Notification{}, fmt.Errorf("decode notification data: %w", err)
	}
	return notification, nil
}

func (r *NotificationRepository) attachReadState(ctx context.Context, tenantID domain.TenantID, userID domain.UserID, notifications []readmodels.Notification) error {
	for index := range notifications {
		var readAt time.Time
		err := r.session.Query(findReadState, tenantID.String(), string(userID), notifications[index].ID.String()).ScanContext(ctx, &readAt)
		if err == nil {
			notifications[index].Read = true
			continue
		}
		if err != gocql.ErrNotFound {
			return fmt.Errorf("read notification state: %w", err)
		}
	}
	return nil
}

func encodePageState(state []byte) string {
	if len(state) == 0 {
		return ""
	}
	return base64.RawURLEncoding.EncodeToString(state)
}

func decodePageState(cursor string) ([]byte, error) {
	if cursor == "" {
		return nil, nil
	}
	state, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return nil, fmt.Errorf("notification cursor: %w", err)
	}
	return state, nil
}

var _ ports.NotificationReadRepository = (*NotificationRepository)(nil)
