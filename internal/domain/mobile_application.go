package domain

import (
	"fmt"
	"strings"
	"uuid"
)

type MobilePlatform string

const (
	MobilePlatformAndroid MobilePlatform = "android"
	MobilePlatformIOS     MobilePlatform = "ios"
)

func (p MobilePlatform) IsValid() bool {
	return p == MobilePlatformAndroid || p == MobilePlatformIOS
}

type NewMobileApplicationParams struct {
	TenantID    TenantID
	Provider    *Provider
	Platform    MobilePlatform
	PackageName string
}

type MobileApplication struct {
	id          MobileApplicationID
	tenantID    TenantID
	providerID  *ProviderID
	platform    MobilePlatform
	packageName string
	status      ConfigurationStatus
}

func NewMobileApplication(params NewMobileApplicationParams) (*MobileApplication, error) {
	if err := requireUUID("tenant_id", params.TenantID); err != nil {
		return nil, err
	}
	if params.Provider == nil {
		return nil, fmt.Errorf("%w: provider must not be nil", ErrInvalidArgument)
	}
	if params.Provider.TenantID() != params.TenantID {
		return nil, fmt.Errorf("%w: application and provider belong to different tenants", ErrInvalidArgument)
	}
	if params.Provider.Status() != ConfigurationStatusActive {
		return nil, fmt.Errorf("%w: provider is disabled", ErrInvalidArgument)
	}
	if !params.Platform.IsValid() {
		return nil, fmt.Errorf("%w: unsupported mobile platform %q", ErrInvalidArgument, params.Platform)
	}
	if strings.TrimSpace(params.PackageName) == "" {
		return nil, fmt.Errorf("%w: package_name must not be empty", ErrInvalidArgument)
	}
	providerID := params.Provider.ID()
	return &MobileApplication{
		id:          uuid.NewV7(),
		tenantID:    params.TenantID,
		providerID:  &providerID,
		platform:    params.Platform,
		packageName: params.PackageName,
		status:      ConfigurationStatusActive,
	}, nil
}

func (a *MobileApplication) ConnectProvider(provider *Provider) error {
	if provider == nil {
		return fmt.Errorf("%w: provider must not be nil", ErrInvalidArgument)
	}
	if provider.TenantID() != a.tenantID {
		return fmt.Errorf("%w: application and provider belong to different tenants", ErrInvalidArgument)
	}
	if provider.Status() != ConfigurationStatusActive {
		return fmt.Errorf("%w: provider is disabled", ErrInvalidArgument)
	}
	providerID := provider.ID()
	if a.providerID != nil {
		if *a.providerID == providerID {
			return nil
		}
		return fmt.Errorf("%w: mobile application is already connected to another provider", ErrInvalidTransition)
	}
	a.providerID = &providerID
	a.status = ConfigurationStatusActive
	return nil
}

// DisconnectProvider disables fan-out to this platform-specific application.
func (a *MobileApplication) DisconnectProvider() error {
	if a.providerID == nil && a.status == ConfigurationStatusDisabled {
		return nil
	}

	a.providerID = nil
	a.status = ConfigurationStatusDisabled
	return nil
}

func (a *MobileApplication) ID() MobileApplicationID     { return a.id }
func (a *MobileApplication) TenantID() TenantID          { return a.tenantID }
func (a *MobileApplication) Platform() MobilePlatform    { return a.platform }
func (a *MobileApplication) PackageName() string         { return a.packageName }
func (a *MobileApplication) Status() ConfigurationStatus { return a.status }

func (a *MobileApplication) ProviderID() *ProviderID {
	if a.providerID == nil {
		return nil
	}
	result := *a.providerID
	return &result
}

type HydrateMobileApplicationParams struct {
	ID          MobileApplicationID
	TenantID    TenantID
	ProviderID  *ProviderID
	Platform    MobilePlatform
	PackageName string
	Status      ConfigurationStatus
}

func HydrateMobileApplication(params HydrateMobileApplicationParams) (*MobileApplication, error) {
	if err := requireUUID("mobile_application_id", params.ID); err != nil {
		return nil, err
	}
	if err := requireUUID("tenant_id", params.TenantID); err != nil {
		return nil, err
	}
	if params.ProviderID != nil {
		if err := requireUUID("provider_id", *params.ProviderID); err != nil {
			return nil, err
		}
	}
	if !params.Platform.IsValid() {
		return nil, fmt.Errorf("%w: unsupported mobile platform %q", ErrInvalidArgument, params.Platform)
	}
	if strings.TrimSpace(params.PackageName) == "" {
		return nil, fmt.Errorf("%w: package_name must not be empty", ErrInvalidArgument)
	}
	if !params.Status.isValid() {
		return nil, fmt.Errorf("%w: invalid mobile application status", ErrInvalidArgument)
	}
	if params.ProviderID == nil && params.Status != ConfigurationStatusDisabled {
		return nil, fmt.Errorf("%w: disconnected mobile application must be disabled", ErrInvalidArgument)
	}
	providerID := params.ProviderID
	if providerID != nil {
		value := *providerID
		providerID = &value
	}
	return &MobileApplication{
		id:          params.ID,
		tenantID:    params.TenantID,
		providerID:  providerID,
		platform:    params.Platform,
		packageName: params.PackageName,
		status:      params.Status,
	}, nil
}
