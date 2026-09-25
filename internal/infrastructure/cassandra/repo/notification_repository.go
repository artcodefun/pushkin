package repo

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/apache/cassandra-gocql-driver/v2"
	"golang.org/x/sync/errgroup"

	"github.com/superman/pushkin/internal/application/ports"
	"github.com/superman/pushkin/internal/domain"
)

const notificationTTL = 90 * 24 * time.Hour

const insertNotification = `INSERT INTO notifications_by_user
(tenant_id, user_id, created_at, notification_id, campaign_id, title, body, image_url, data)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) USING TTL ?`

const insertReadState = `INSERT INTO notification_read_state_by_user
(tenant_id, user_id, notification_id, read_at) VALUES (?, ?, ?, ?) USING TTL ?`

type NotificationRepository struct {
	session     *gocql.Session
	ttl         int
	maxInFlight int
}

func NewNotificationRepository(session *gocql.Session, maxInFlight int) *NotificationRepository {
	return &NotificationRepository{
		session: session, ttl: int(notificationTTL / time.Second), maxInFlight: maxInFlight,
	}
}

func (r *NotificationRepository) SaveAcceptedBatch(ctx context.Context, notifications []domain.Notification) error {
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(r.maxInFlight)
	for _, notification := range notifications {
		notification := notification
		group.Go(func() error { return r.saveAccepted(groupCtx, notification) })
	}
	return group.Wait()
}

func (r *NotificationRepository) saveAccepted(ctx context.Context, notification domain.Notification) error {
	payload := notification.Payload()
	data, err := json.Marshal(payload.Data())
	if err != nil {
		return fmt.Errorf("encode notification data: %w", err)
	}
	return r.session.Query(insertNotification,
		notification.TenantID().String(), string(notification.UserID()), notification.CreatedAt(), notification.ID().String(),
		notification.CampaignID().String(), payload.Title(), payload.Body(), payload.ImageURL(), string(data), r.ttl,
	).ExecContext(ctx)
}

func (r *NotificationRepository) MarkRead(ctx context.Context, tenantID domain.TenantID, userID domain.UserID, notificationID domain.NotificationID, readAt time.Time) error {
	return r.session.Query(insertReadState,
		tenantID.String(), string(userID), notificationID.String(), readAt, r.ttl,
	).ExecContext(ctx)
}

var _ ports.NotificationRepository = (*NotificationRepository)(nil)
