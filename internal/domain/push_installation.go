package domain

import (
	"fmt"
	"strings"
	"uuid"
)

type PushInstallationStatus string

const (
	PushInstallationStatusActive   PushInstallationStatus = "active"
	PushInstallationStatusInactive PushInstallationStatus = "inactive"
)

type NewPushInstallationParams struct {
	TenantID          TenantID
	UserID            UserID
	MobileApplication *MobileApplication
	InstallationID    string
	Token             string
}

type PushInstallation struct {
	id                  PushInstallationID
	tenantID            TenantID
	userID              UserID
	mobileApplicationID MobileApplicationID
	installationID      string
	token               string
	status              PushInstallationStatus
}

func NewPushInstallation(params NewPushInstallationParams) (*PushInstallation, error) {
	if err := requireUUID("tenant_id", params.TenantID); err != nil {
		return nil, err
	}
	if err := requireIdentifier("user_id", string(params.UserID)); err != nil {
		return nil, err
	}
	if params.MobileApplication == nil {
		return nil, fmt.Errorf("%w: mobile application must not be nil", ErrInvalidArgument)
	}
	if params.MobileApplication.TenantID() != params.TenantID {
		return nil, fmt.Errorf("%w: installation and application belong to different tenants", ErrInvalidArgument)
	}
	if params.MobileApplication.Status() != ConfigurationStatusActive || params.MobileApplication.ProviderID() == nil {
		return nil, fmt.Errorf("%w: mobile application is disabled", ErrInvalidArgument)
	}
	if strings.TrimSpace(params.InstallationID) == "" {
		return nil, fmt.Errorf("%w: installation_id must not be empty", ErrInvalidArgument)
	}
	if strings.TrimSpace(params.Token) == "" {
		return nil, fmt.Errorf("%w: token must not be empty", ErrInvalidArgument)
	}
	return &PushInstallation{
		id:                  uuid.NewV7(),
		tenantID:            params.TenantID,
		userID:              params.UserID,
		mobileApplicationID: params.MobileApplication.ID(),
		installationID:      params.InstallationID,
		token:               params.Token,
		status:              PushInstallationStatusActive,
	}, nil
}

// RefreshToken models registration/upsert of the same stable installation.
// A fresh token reactivates an installation previously rejected by FCM.
func (i *PushInstallation) RefreshToken(token string) error {
	if strings.TrimSpace(token) == "" {
		return fmt.Errorf("%w: token must not be empty", ErrInvalidArgument)
	}
	i.token = token
	i.status = PushInstallationStatusActive
	return nil
}

func (i *PushInstallation) Deactivate() error {
	if i.status == PushInstallationStatusInactive {
		return nil
	}

	i.status = PushInstallationStatusInactive
	return nil
}

func (i *PushInstallation) ID() PushInstallationID                   { return i.id }
func (i *PushInstallation) TenantID() TenantID                       { return i.tenantID }
func (i *PushInstallation) UserID() UserID                           { return i.userID }
func (i *PushInstallation) MobileApplicationID() MobileApplicationID { return i.mobileApplicationID }
func (i *PushInstallation) InstallationID() string                   { return i.installationID }
func (i *PushInstallation) Token() string                            { return i.token }
func (i *PushInstallation) Status() PushInstallationStatus           { return i.status }
