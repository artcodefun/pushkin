package domain

import (
	"testing"
	"time"
	"uuid"
)

func TestPushInstallationRefreshReactivatesToken(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.September, 5, 12, 0, 0, 0, time.UTC)
	tenantID := uuid.NewV7()
	provider := newTestProvider(t, tenantID, now)
	application := newTestApplication(t, tenantID, provider, MobilePlatformAndroid, "com.example.app", now)
	installation, err := NewPushInstallation(NewPushInstallationParams{
		TenantID:          tenantID,
		UserID:            "subscriber-1",
		MobileApplication: application,
		InstallationID:    "installation-1",
		Token:             "old-token",
	})
	if err != nil {
		t.Fatalf("new push installation: %v", err)
	}

	if err := installation.Deactivate(); err != nil {
		t.Fatalf("deactivate: %v", err)
	}
	if installation.Status() != PushInstallationStatusInactive {
		t.Fatal("installation must be inactive")
	}

	if err := installation.RefreshToken("new-token"); err != nil {
		t.Fatalf("refresh token: %v", err)
	}
	if installation.Token() != "new-token" || installation.Status() != PushInstallationStatusActive {
		t.Fatal("token refresh must update and reactivate installation")
	}
}
