//go:build integration

package cassandra_test

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"uuid"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
	"github.com/testcontainers/testcontainers-go"
	testcassandra "github.com/testcontainers/testcontainers-go/modules/cassandra"

	"github.com/superman/pushkin/internal/domain"
	cassandrareadrepo "github.com/superman/pushkin/internal/infrastructure/cassandra/readrepo"
	cassandrarepo "github.com/superman/pushkin/internal/infrastructure/cassandra/repo"
)

func TestNotificationRepositoriesPersistNotificationsAndReadState(t *testing.T) {
	ctx := context.Background()
	session := newIntegrationSession(t, ctx)
	writer := cassandrarepo.NewNotificationRepository(session, 2)
	reader := cassandrareadrepo.NewNotificationRepository(session)

	payload, err := domain.NewPushPayload("A delivery", "has completed", "", map[string]string{"kind": "delivery"})
	if err != nil {
		t.Fatalf("new payload: %v", err)
	}
	tenantID := uuid.NewV7()
	notification, err := domain.NewNotification(domain.NewNotificationParams{
		CampaignID: uuid.NewV7(),
		TenantID:   tenantID,
		UserID:     "user-1",
		CreatedAt:  time.Now().UTC(),
		Payload:    payload,
	})
	if err != nil {
		t.Fatalf("new notification: %v", err)
	}

	if err := writer.SaveAcceptedBatch(ctx, []domain.Notification{notification}); err != nil {
		t.Fatalf("save accepted notification: %v", err)
	}
	if err := writer.SaveAcceptedBatch(ctx, []domain.Notification{notification}); err != nil {
		t.Fatalf("save accepted notification idempotently: %v", err)
	}

	page, err := reader.List(ctx, tenantID, notification.UserID(), "", 10)
	if err != nil {
		t.Fatalf("list notifications: %v", err)
	}
	if len(page.Notifications) != 1 {
		t.Fatalf("notification count = %d, want 1", len(page.Notifications))
	}
	stored := page.Notifications[0]
	if stored.ID != notification.ID() || stored.CampaignID != notification.CampaignID() || stored.Title != "A delivery" || stored.Read {
		t.Fatalf("stored notification = %#v", stored)
	}

	readAt := time.Now().UTC()
	if err := writer.MarkRead(ctx, tenantID, notification.UserID(), notification.ID(), readAt); err != nil {
		t.Fatalf("mark notification read: %v", err)
	}
	page, err = reader.List(ctx, tenantID, notification.UserID(), "", 10)
	if err != nil {
		t.Fatalf("list read notification: %v", err)
	}
	if len(page.Notifications) != 1 || !page.Notifications[0].Read {
		t.Fatalf("read notifications = %#v, want one read notification", page.Notifications)
	}
}

func newIntegrationSession(t *testing.T, ctx context.Context) *gocql.Session {
	t.Helper()
	container, err := testcassandra.Run(ctx, "cassandra:5.0")
	if err != nil {
		t.Fatalf("start Cassandra test container: %v", err)
	}
	testcontainers.CleanupContainer(t, container)
	endpoint, err := container.ConnectionHost(ctx)
	if err != nil {
		t.Fatalf("get Cassandra test endpoint: %v", err)
	}
	host, port, err := net.SplitHostPort(endpoint)
	if err != nil {
		t.Fatalf("split Cassandra test endpoint: %v", err)
	}

	cluster := gocql.NewCluster(host)
	cluster.Port, err = strconv.Atoi(port)
	if err != nil {
		t.Fatalf("parse Cassandra CQL port: %v", err)
	}
	cluster.Timeout = 10 * time.Second
	cluster.ConnectTimeout = 10 * time.Second
	setupSession, err := cluster.CreateSession()
	if err != nil {
		t.Fatalf("connect to Cassandra: %v", err)
	}
	if err := setupSession.Query("CREATE KEYSPACE pushkin_test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}").ExecContext(ctx); err != nil {
		setupSession.Close()
		t.Fatalf("create Cassandra keyspace: %v", err)
	}
	setupSession.Close()

	cluster.Keyspace = "pushkin_test"
	session, err := cluster.CreateSession()
	if err != nil {
		t.Fatalf("connect to Cassandra keyspace: %v", err)
	}
	t.Cleanup(session.Close)
	applyMigrations(t, ctx, session)
	return session
}

func applyMigrations(t *testing.T, ctx context.Context, session *gocql.Session) {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate Cassandra integration test source")
	}
	migration, err := os.ReadFile(filepath.Join(filepath.Dir(file), "migrations", "000001_notification_inbox.up.cql"))
	if err != nil {
		t.Fatalf("read Cassandra migration: %v", err)
	}
	for _, statement := range strings.Split(string(migration), ";") {
		statement = strings.TrimSpace(statement)
		if statement == "" {
			continue
		}
		if err := session.Query(statement).ExecContext(ctx); err != nil {
			t.Fatalf("apply Cassandra migration: %v", err)
		}
	}
}
