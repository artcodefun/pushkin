package queries

import (
	"context"
	"fmt"
	"strings"

	"github.com/superman/pushkin/internal/application"
	"github.com/superman/pushkin/internal/application/ports"
	"github.com/superman/pushkin/internal/application/queries/readmodels"
	"github.com/superman/pushkin/internal/domain"
)

var _ application.NotificationQueries = (*NotificationQueries)(nil)

const defaultNotificationPageSize = 50
const maximumNotificationPageSize = 100

type NotificationQueries struct {
	notifications ports.NotificationReadRepository
}

func NewNotificationQueries(notifications ports.NotificationReadRepository) *NotificationQueries {
	return &NotificationQueries{notifications: notifications}
}

func (q *NotificationQueries) ListNotifications(ctx context.Context, tenantID domain.TenantID, userID domain.UserID, cursor string, limit int) (readmodels.NotificationPage, error) {
	if strings.TrimSpace(string(userID)) == "" || limit < 0 || limit > maximumNotificationPageSize {
		return readmodels.NotificationPage{}, fmt.Errorf("list notifications: %w", application.ErrValidation)
	}
	if limit == 0 {
		limit = defaultNotificationPageSize
	}
	return q.notifications.List(ctx, tenantID, userID, cursor, limit)
}
