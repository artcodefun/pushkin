package domain

import (
	"errors"
	"testing"
	"time"
	"uuid"
)

func TestChannelMobileApplicationServiceAcceptsApplicationOfChannelProvider(t *testing.T) {
	t.Parallel()

	tenantID := uuid.NewV7()
	now := time.Time{}
	provider := newTestProvider(t, tenantID, now)
	application := newTestApplication(t, tenantID, provider, MobilePlatformIOS, "com.example.app", now)
	channel := newTestChannel(t, tenantID, provider, now)

	service := NewChannelMobileApplicationService()
	if err := service.ValidateLink(channel, application); err != nil {
		t.Fatalf("validate application link: %v", err)
	}
}

func TestChannelMobileApplicationServiceRejectsApplicationOfAnotherProvider(t *testing.T) {
	t.Parallel()

	tenantID := uuid.NewV7()
	now := time.Time{}
	channelProvider := newTestProvider(t, tenantID, now)
	applicationProvider := newTestProvider(t, tenantID, now)
	application := newTestApplication(t, tenantID, applicationProvider, MobilePlatformIOS, "com.example.app", now)
	channel := newTestChannel(t, tenantID, channelProvider, now)

	service := NewChannelMobileApplicationService()
	if err := service.ValidateLink(channel, application); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("expected provider mismatch, got %v", err)
	}
}

func TestChannelMobileApplicationServiceRejectsCrossTenantApplication(t *testing.T) {
	t.Parallel()

	channelTenantID := uuid.NewV7()
	applicationTenantID := uuid.NewV7()
	now := time.Time{}
	channelProvider := newTestProvider(t, channelTenantID, now)
	applicationProvider := newTestProvider(t, applicationTenantID, now)
	application := newTestApplication(t, applicationTenantID, applicationProvider, MobilePlatformAndroid, "com.example.app", now)
	channel := newTestChannel(t, channelTenantID, channelProvider, now)

	service := NewChannelMobileApplicationService()
	if err := service.ValidateLink(channel, application); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("expected tenant mismatch, got %v", err)
	}
}

func TestNewChannelRejectsProviderOfAnotherTenant(t *testing.T) {
	t.Parallel()

	tenantID := uuid.NewV7()
	provider := newTestProvider(t, uuid.NewV7(), time.Time{})
	if _, err := NewChannel(NewChannelParams{
		TenantID: tenantID,
		Provider: provider,
		Type:     ChannelTypeMobilePush,
		Key:      "customer-app",
	}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("expected tenant mismatch, got %v", err)
	}
}

func TestHydrateChannelRejectsInvalidStatus(t *testing.T) {
	t.Parallel()

	_, err := HydrateChannel(HydrateChannelParams{
		ID:         uuid.NewV7(),
		TenantID:   uuid.NewV7(),
		ProviderID: uuid.NewV7(),
		Type:       ChannelTypeMobilePush,
		Key:        "customer-app",
		Status:     "unknown",
	})
	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("expected invalid argument, got %v", err)
	}
}

func newTestProvider(t *testing.T, tenantID TenantID, _ time.Time) *Provider {
	t.Helper()
	provider, err := NewProvider(NewProviderParams{
		TenantID:             tenantID,
		Type:                 ProviderTypeFCM,
		EncryptedCredentials: "encrypted-credentials",
		RateLimitQPS:         100,
		RateLimitBurst:       500,
	})
	if err != nil {
		t.Fatalf("new provider: %v", err)
	}
	return provider
}

func newTestApplication(
	t *testing.T,
	tenantID TenantID,
	provider *Provider,
	platform MobilePlatform,
	packageName string,
	_ time.Time,
) *MobileApplication {
	t.Helper()
	application, err := NewMobileApplication(NewMobileApplicationParams{
		TenantID:    tenantID,
		Provider:    provider,
		Platform:    platform,
		PackageName: packageName,
	})
	if err != nil {
		t.Fatalf("new mobile application: %v", err)
	}
	return application
}

func newTestChannel(t *testing.T, tenantID TenantID, provider *Provider, _ time.Time) *Channel {
	t.Helper()
	channel, err := NewChannel(NewChannelParams{
		TenantID: tenantID,
		Provider: provider,
		Type:     ChannelTypeMobilePush,
		Key:      "customer-app",
	})
	if err != nil {
		t.Fatalf("new channel: %v", err)
	}
	if err := channel.Activate(); err != nil {
		t.Fatalf("activate channel: %v", err)
	}
	return channel
}
