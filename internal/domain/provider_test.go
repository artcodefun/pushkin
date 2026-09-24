package domain

import (
	"errors"
	"testing"

	"uuid"
)

func TestHydrateProvider(t *testing.T) {
	t.Parallel()

	providerID := uuid.NewV7()
	tenantID := uuid.NewV7()
	provider, err := HydrateProvider(HydrateProviderParams{
		ID:             providerID,
		TenantID:       tenantID,
		Type:           ProviderTypeFCM,
		EncryptedCredentials: "encrypted-credentials",
		RateLimitQPS:   100,
		RateLimitBurst: 500,
		Status:         ConfigurationStatusDisabled,
	})
	if err != nil {
		t.Fatalf("hydrate provider: %v", err)
	}
	if provider.ID() != providerID || provider.TenantID() != tenantID {
		t.Fatalf("provider IDs were not preserved: provider=%s tenant=%s", provider.ID(), provider.TenantID())
	}
	if provider.Status() != ConfigurationStatusDisabled || provider.RateLimitQPS() != 100 || provider.RateLimitBurst() != 500 {
		t.Fatalf("unexpected hydrated provider: %#v", provider)
	}
}

func TestHydrateProviderRejectsInvalidState(t *testing.T) {
	t.Parallel()

	_, err := HydrateProvider(HydrateProviderParams{
		ID:             uuid.NewV7(),
		TenantID:       uuid.NewV7(),
		Type:           ProviderTypeFCM,
		EncryptedCredentials: "encrypted-credentials",
		RateLimitQPS:   100,
		RateLimitBurst: 500,
		Status:         "unknown",
	})
	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("hydrate invalid provider error = %v, want invalid argument", err)
	}
}
