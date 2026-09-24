package redis

import (
	"context"
	"fmt"
	"math"
	"strings"
	"time"

	redis "github.com/redis/go-redis/v9"

	"github.com/superman/pushkin/internal/application/ports"
	"github.com/superman/pushkin/internal/domain"
)

var _ ports.DeliveryRateLimiter = (*RateLimiter)(nil)

const defaultKeyPrefix = "pushkin:rate-limit"

var reserveScript = redis.NewScript(`
local now = redis.call('TIME')
local now_ms = now[1] * 1000 + math.floor(now[2] / 1000)

local function refill(key, rate_per_second, capacity)
  local values = redis.call('HMGET', key, 'tokens', 'updated_at_ms')
  local tokens = tonumber(values[1])
  local updated_at_ms = tonumber(values[2])
  if tokens == nil or updated_at_ms == nil then
    return capacity
  end
  return math.min(capacity, tokens + (now_ms - updated_at_ms) * rate_per_second / 1000)
end

local provider_tokens = refill(KEYS[1], tonumber(ARGV[1]), tonumber(ARGV[2]))
local tenant_tokens = refill(KEYS[2], tonumber(ARGV[3]), tonumber(ARGV[4]))
local permits = tonumber(ARGV[5])

if provider_tokens >= permits and tenant_tokens >= permits then
  redis.call('HSET', KEYS[1], 'tokens', provider_tokens - permits, 'updated_at_ms', now_ms)
  redis.call('PEXPIRE', KEYS[1], tonumber(ARGV[6]))
  redis.call('HSET', KEYS[2], 'tokens', tenant_tokens - permits, 'updated_at_ms', now_ms)
  redis.call('PEXPIRE', KEYS[2], tonumber(ARGV[7]))
  return {1, now_ms}
end

local provider_wait_ms = math.max(0, (permits - provider_tokens) * 1000 / tonumber(ARGV[1]))
local tenant_wait_ms = math.max(0, (permits - tenant_tokens) * 1000 / tonumber(ARGV[3]))
return {0, now_ms + math.ceil(math.max(provider_wait_ms, tenant_wait_ms))}
`)

// RateLimiter atomically reserves provider and tenant permits from Redis token
// buckets. Bucket configuration comes from PostgreSQL-backed domain models;
// Redis stores only short-lived token state.
type RateLimiter struct {
	client    redis.UniversalClient
	keyPrefix string
}

type RateLimiterParams struct {
	Client    redis.UniversalClient
	KeyPrefix string
}

func NewRateLimiter(params RateLimiterParams) (*RateLimiter, error) {
	if params.Client == nil {
		return nil, fmt.Errorf("Redis rate limiter client must not be nil")
	}
	keyPrefix := strings.TrimSpace(params.KeyPrefix)
	if keyPrefix == "" {
		keyPrefix = defaultKeyPrefix
	}
	return &RateLimiter{client: params.Client, keyPrefix: keyPrefix}, nil
}

func (l *RateLimiter) Reserve(
	ctx context.Context,
	tenant *domain.Tenant,
	provider *domain.Provider,
	permits int,
) (ports.DeliveryRateLimitReservation, error) {
	if tenant == nil || provider == nil {
		return ports.DeliveryRateLimitReservation{}, fmt.Errorf("Redis rate limiter tenant and provider must not be nil")
	}
	if permits <= 0 {
		return ports.DeliveryRateLimitReservation{}, fmt.Errorf("Redis rate limiter permits must be positive")
	}
	if provider.TenantID() != tenant.ID() {
		return ports.DeliveryRateLimitReservation{}, fmt.Errorf("Redis rate limiter provider belongs to another tenant")
	}

	providerBurst := provider.RateLimitBurst()
	tenantBurst := tenant.RateLimitPerMinute()
	if permits > providerBurst || permits > tenantBurst {
		return ports.DeliveryRateLimitReservation{}, fmt.Errorf("Redis rate limiter permits exceed bucket capacity")
	}
	providerRate := provider.RateLimitQPS()
	tenantRate := float64(tenant.RateLimitPerMinute()) / 60
	result, err := reserveScript.Run(
		ctx,
		l.client,
		[]string{l.providerKey(provider.ID()), l.tenantKey(tenant.ID())},
		providerRate,
		providerBurst,
		tenantRate,
		tenantBurst,
		permits,
		bucketTTLMillis(float64(providerBurst), float64(providerRate)),
		bucketTTLMillis(float64(tenantBurst), tenantRate),
	).Slice()
	if err != nil {
		return ports.DeliveryRateLimitReservation{}, fmt.Errorf("reserve Redis rate limit permits: %w", err)
	}
	if len(result) != 2 {
		return ports.DeliveryRateLimitReservation{}, fmt.Errorf("unexpected Redis rate limiter response")
	}
	granted, ok := result[0].(int64)
	if !ok {
		return ports.DeliveryRateLimitReservation{}, fmt.Errorf("unexpected Redis rate limiter grant response")
	}
	availableAtMillis, ok := result[1].(int64)
	if !ok {
		return ports.DeliveryRateLimitReservation{}, fmt.Errorf("unexpected Redis rate limiter availability response")
	}
	return ports.DeliveryRateLimitReservation{
		Granted:     granted == 1,
		AvailableAt: time.UnixMilli(availableAtMillis).UTC(),
	}, nil
}

func (l *RateLimiter) providerKey(id domain.ProviderID) string {
	return l.keyPrefix + ":provider:" + id.String()
}

func (l *RateLimiter) tenantKey(id domain.TenantID) string {
	return l.keyPrefix + ":tenant:" + id.String()
}

func bucketTTLMillis(capacity, ratePerSecond float64) int64 {
	return int64(math.Ceil(capacity/ratePerSecond*2000)) + 1
}
