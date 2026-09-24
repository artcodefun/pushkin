package domain

import "fmt"

// ChannelMobileApplicationService contains domain rules for the association
// between a Channel and MobileApplications. It deliberately has no state or
// infrastructure dependencies.
type ChannelMobileApplicationService struct{}

func NewChannelMobileApplicationService() *ChannelMobileApplicationService {
	return &ChannelMobileApplicationService{}
}

// ValidateLink validates the v1 routing invariant for a Channel and a
// MobileApplication. Persisting the link itself is an application/repository
// responsibility.
func (s ChannelMobileApplicationService) ValidateLink(
	channel *Channel,
	application *MobileApplication,
) error {
	if channel == nil {
		return fmt.Errorf("%w: channel must not be nil", ErrInvalidArgument)
	}
	if application == nil {
		return fmt.Errorf("%w: mobile application must not be nil", ErrInvalidArgument)
	}
	if channel.Type() != ChannelTypeMobilePush {
		return fmt.Errorf("%w: mobile application requires a mobile_push channel", ErrInvalidArgument)
	}
	if application.TenantID() != channel.TenantID() {
		return fmt.Errorf("%w: channel and application belong to different tenants", ErrInvalidArgument)
	}

	providerID := application.ProviderID()
	if application.Status() != ConfigurationStatusActive || providerID == nil {
		return fmt.Errorf("%w: mobile application is disabled or disconnected", ErrInvalidArgument)
	}
	if *providerID != channel.ProviderID() {
		return fmt.Errorf("%w: channel and mobile application use different providers", ErrInvalidArgument)
	}

	return nil
}
