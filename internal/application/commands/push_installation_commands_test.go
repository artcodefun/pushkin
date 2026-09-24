package commands

import (
	"context"
	"errors"
	"testing"
	"uuid"

	"github.com/superman/pushkin/internal/application"
	"github.com/superman/pushkin/internal/domain"
)

func TestPushInstallationCommandsRegistersInstallationForResolvedApplication(t *testing.T) {
	t.Parallel()

	tenantID := uuid.NewV7()
	mobileApplication := newCommandTestMobileApplication(t, tenantID)
	applications := &pushInstallationMobileApplicationRepository{application: mobileApplication}
	installations := &pushInstallationRepository{}
	commands := NewPushInstallationCommands(PushInstallationCommandsParams{
		MobileApplicationRepository: applications,
		PushInstallationRepository:  installations,
	})

	err := commands.RegisterPushInstallation(context.Background(), application.RegisterPushInstallationCommand{
		TenantID:       tenantID,
		UserID:         "subscriber-42",
		Platform:       domain.MobilePlatformAndroid,
		PackageName:    "com.example.customer",
		InstallationID: "installation-42",
		Token:          "fcm-token",
	})
	if err != nil {
		t.Fatalf("register push installation: %v", err)
	}

	if applications.tenantID != tenantID ||
		applications.platform != domain.MobilePlatformAndroid ||
		applications.packageName != "com.example.customer" {
		t.Fatalf("unexpected application lookup: %+v", applications)
	}
	if installations.installation == nil {
		t.Fatal("installation was not persisted")
	}
	if installations.installation.TenantID() != tenantID ||
		installations.installation.UserID() != "subscriber-42" ||
		installations.installation.MobileApplicationID() != mobileApplication.ID() ||
		installations.installation.InstallationID() != "installation-42" ||
		installations.installation.Token() != "fcm-token" ||
		installations.installation.Status() != domain.PushInstallationStatusActive {
		t.Fatalf("unexpected installation: %+v", installations.installation)
	}
}

func TestPushInstallationCommandsRejectsApplicationFromAnotherTenant(t *testing.T) {
	t.Parallel()

	mobileApplication := newCommandTestMobileApplication(t, uuid.NewV7())
	commands := NewPushInstallationCommands(PushInstallationCommandsParams{
		MobileApplicationRepository: &pushInstallationMobileApplicationRepository{application: mobileApplication},
		PushInstallationRepository:  &pushInstallationRepository{},
	})

	err := commands.RegisterPushInstallation(context.Background(), application.RegisterPushInstallationCommand{
		TenantID:       uuid.NewV7(),
		UserID:         "subscriber-42",
		Platform:       domain.MobilePlatformAndroid,
		PackageName:    "com.example.customer",
		InstallationID: "installation-42",
		Token:          "fcm-token",
	})
	if !errors.Is(err, domain.ErrInvalidArgument) {
		t.Fatalf("expected invalid argument, got %v", err)
	}
}

type pushInstallationMobileApplicationRepository struct {
	application *domain.MobileApplication
	tenantID    domain.TenantID
	platform    domain.MobilePlatform
	packageName string
}

func (r *pushInstallationMobileApplicationRepository) ListIDsByChannel(
	context.Context,
	domain.ChannelID,
) ([]domain.MobileApplicationID, error) {
	return nil, nil
}

func (r *pushInstallationMobileApplicationRepository) ListIDsByChannels(
	context.Context,
	[]domain.ChannelID,
) (map[domain.ChannelID][]domain.MobileApplicationID, error) {
	return nil, nil
}

func (r *pushInstallationMobileApplicationRepository) Create(
	context.Context,
	*domain.MobileApplication,
) error {
	return nil
}

func (r *pushInstallationMobileApplicationRepository) FindByID(
	context.Context,
	domain.MobileApplicationID,
) (*domain.MobileApplication, error) {
	return r.application, nil
}

func (r *pushInstallationMobileApplicationRepository) FindByPlatformAndPackageName(
	_ context.Context,
	tenantID domain.TenantID,
	platform domain.MobilePlatform,
	packageName string,
) (*domain.MobileApplication, error) {
	r.tenantID = tenantID
	r.platform = platform
	r.packageName = packageName
	return r.application, nil
}

func (r *pushInstallationMobileApplicationRepository) Update(
	context.Context,
	*domain.MobileApplication,
) error {
	return nil
}

func (r *pushInstallationMobileApplicationRepository) ListChannelIDs(
	context.Context,
	domain.MobileApplicationID,
) ([]domain.ChannelID, error) {
	return nil, nil
}

type pushInstallationRepository struct {
	installation *domain.PushInstallation
}

func (r *pushInstallationRepository) Deactivate(
	context.Context,
	domain.TenantID,
	domain.PushInstallationID,
) error {
	return nil
}

func (r *pushInstallationRepository) Upsert(
	_ context.Context,
	installation *domain.PushInstallation,
) error {
	r.installation = installation
	return nil
}

func (r *pushInstallationRepository) ListActiveTokensByIDs(
	context.Context,
	domain.TenantID,
	[]domain.PushInstallationID,
) (map[domain.PushInstallationID]string, error) {
	return nil, nil
}

func (r *pushInstallationRepository) ListActiveIDs(
	context.Context,
	domain.TenantID,
	[]domain.UserID,
	[]domain.MobileApplicationID,
) ([]domain.PushInstallationID, error) {
	return nil, nil
}

func (r *pushInstallationRepository) ListActiveIDsByUsers(
	context.Context,
	domain.TenantID,
	[]domain.UserID,
	[]domain.MobileApplicationID,
) (map[domain.UserID][]domain.PushInstallationID, error) {
	return nil, nil
}

func newCommandTestMobileApplication(t *testing.T, tenantID domain.TenantID) *domain.MobileApplication {
	t.Helper()

	provider, err := domain.NewProvider(domain.NewProviderParams{
		TenantID:             tenantID,
		Type:                 domain.ProviderTypeFCM,
		EncryptedCredentials: "encrypted-credentials",
		RateLimitQPS:         100,
		RateLimitBurst:       500,
	})
	if err != nil {
		t.Fatalf("new provider: %v", err)
	}

	mobileApplication, err := domain.NewMobileApplication(domain.NewMobileApplicationParams{
		TenantID:    tenantID,
		Provider:    provider,
		Platform:    domain.MobilePlatformAndroid,
		PackageName: "com.example.customer",
	})
	if err != nil {
		t.Fatalf("new mobile application: %v", err)
	}

	return mobileApplication
}
