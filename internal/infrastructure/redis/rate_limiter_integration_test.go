//go:build integration

package redis

import (
	"context"
	"sync"
	"testing"
	"time"
	"uuid"

	redisclient "github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	testredis "github.com/testcontainers/testcontainers-go/modules/redis"

	"github.com/superman/pushkin/internal/domain"
)

func TestRateLimiterReservesBothBucketsAtomically(t *testing.T) {
	ctx := context.Background()
	limiter := newIntegrationRateLimiter(t, ctx)
	tenant, provider := rateLimitConfiguration(t, 2, 2, 2)

	first, err := limiter.Reserve(ctx, tenant, provider, 2)
	if err != nil || !first.Granted {
		t.Fatalf("first reservation = %#v, %v; want granted", first, err)
	}
	second, err := limiter.Reserve(ctx, tenant, provider, 1)
	if err != nil {
		t.Fatalf("second reservation: %v", err)
	}
	if second.Granted || !second.AvailableAt.After(time.Now().UTC()) {
		t.Fatalf("second reservation = %#v; want future denial", second)
	}
}

func TestRateLimiterDoesNotOverReserveConcurrently(t *testing.T) {
	ctx := context.Background()
	limiter := newIntegrationRateLimiter(t, ctx)
	tenant, provider := rateLimitConfiguration(t, 3, 3, 3)

	var group sync.WaitGroup
	granted := make(chan bool, 10)
	for range 10 {
		group.Add(1)
		go func() {
			defer group.Done()
			reservation, err := limiter.Reserve(ctx, tenant, provider, 1)
			if err != nil {
				t.Errorf("reserve permit: %v", err)
				return
			}
			granted <- reservation.Granted
		}()
	}
	group.Wait()
	close(granted)

	count := 0
	for wasGranted := range granted {
		if wasGranted {
			count++
		}
	}
	if count != 3 {
		t.Fatalf("granted reservations = %d, want 3", count)
	}
}

func newIntegrationRateLimiter(t *testing.T, ctx context.Context) *RateLimiter {
	t.Helper()
	container, err := testredis.Run(ctx, "redis:7.4-alpine")
	if err != nil {
		t.Fatalf("start Redis test container: %v", err)
	}
	testcontainers.CleanupContainer(t, container)
	endpoint, err := container.Endpoint(ctx, "")
	if err != nil {
		t.Fatalf("get Redis endpoint: %v", err)
	}
	client := redisclient.NewClient(&redisclient.Options{Addr: endpoint})
	t.Cleanup(func() { _ = client.Close() })
	limiter, err := NewRateLimiter(RateLimiterParams{Client: client, KeyPrefix: "pushkin-test:" + uuid.NewV7().String()})
	if err != nil {
		t.Fatalf("new Redis rate limiter: %v", err)
	}
	return limiter
}

func rateLimitConfiguration(t *testing.T, perMinute, qps, burst int) (*domain.Tenant, *domain.Provider) {
	t.Helper()
	tenantID := uuid.NewV7()
	tenant, err := domain.HydrateTenant(domain.HydrateTenantParams{ID: tenantID, Name: "tenant", RateLimitPerMinute: perMinute, Status: domain.ConfigurationStatusActive})
	if err != nil {
		t.Fatalf("hydrate tenant: %v", err)
	}
	provider, err := domain.HydrateProvider(domain.HydrateProviderParams{ID: uuid.NewV7(), TenantID: tenantID, Type: domain.ProviderTypeFCM, EncryptedCredentials: "encrypted-credentials", RateLimitQPS: qps, RateLimitBurst: burst, Status: domain.ConfigurationStatusActive})
	if err != nil {
		t.Fatalf("hydrate provider: %v", err)
	}
	return tenant, provider
}
