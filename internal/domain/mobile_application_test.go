package domain

import (
	"errors"
	"testing"
	"time"
	"uuid"
)

func TestMobileApplicationDisconnectsAndReconnectsProvider(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.September, 5, 12, 0, 0, 0, time.UTC)
	tenantID := uuid.NewV7()
	provider := newTestProvider(t, tenantID, now)
	application := newTestApplication(t, tenantID, provider, MobilePlatformIOS, "com.example.app", now)

	if err := application.DisconnectProvider(); err != nil {
		t.Fatalf("disconnect provider: %v", err)
	}
	if application.ProviderID() != nil || application.Status() != ConfigurationStatusDisabled {
		t.Fatal("disconnected application must be disabled and have no provider")
	}

	if err := application.ConnectProvider(provider); err != nil {
		t.Fatalf("connect provider: %v", err)
	}
	if application.ProviderID() == nil || *application.ProviderID() != provider.ID() {
		t.Fatal("reconnected application has unexpected provider")
	}
	if application.Status() != ConfigurationStatusActive {
		t.Fatal("reconnected application must be active")
	}
}

func TestMobileApplicationRejectsConnectingAnotherProviderWithoutDisconnecting(t *testing.T) {
	t.Parallel()

	tenantID := uuid.NewV7()
	provider := newTestProvider(t, tenantID, time.Time{})
	otherProvider := newTestProvider(t, tenantID, time.Time{})
	application := newTestApplication(t, tenantID, provider, MobilePlatformIOS, "com.example.app", time.Time{})

	err := application.ConnectProvider(otherProvider)
	if !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("expected invalid transition, got %v", err)
	}
	if application.ProviderID() == nil || *application.ProviderID() != provider.ID() {
		t.Fatal("failed connection must preserve the original provider")
	}
}

func TestHydrateMobileApplicationRejectsActiveApplicationWithoutProvider(t *testing.T) {
	t.Parallel()

	_, err := HydrateMobileApplication(HydrateMobileApplicationParams{
		ID:          uuid.NewV7(),
		TenantID:    uuid.NewV7(),
		Platform:    MobilePlatformIOS,
		PackageName: "com.example.app",
		Status:      ConfigurationStatusActive,
	})
	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("expected invalid argument, got %v", err)
	}
}
