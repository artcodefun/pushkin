package domain

import (
	"fmt"
	"strings"
	"uuid"
)

type ChannelType string

const ChannelTypeMobilePush ChannelType = "mobile_push"

// ChannelStatus describes whether a Channel can accept Campaigns. A newly
// created Channel remains provisioning until its Kafka delivery topics exist.
type ChannelStatus string

const (
	ChannelStatusProvisioning ChannelStatus = "provisioning"
	ChannelStatusActive       ChannelStatus = "active"
	ChannelStatusDisabled     ChannelStatus = "disabled"
)

func (s ChannelStatus) isValid() bool {
	return s == ChannelStatusProvisioning || s == ChannelStatusActive || s == ChannelStatusDisabled
}

func (t ChannelType) IsValid() bool {
	return t == ChannelTypeMobilePush
}

type NewChannelParams struct {
	TenantID TenantID
	Provider *Provider
	Type     ChannelType
	Key      string
}

type Channel struct {
	id          ChannelID
	tenantID    TenantID
	providerID  ProviderID
	channelType ChannelType
	key         string
	status      ChannelStatus
}

func NewChannel(params NewChannelParams) (*Channel, error) {
	if err := requireUUID("tenant_id", params.TenantID); err != nil {
		return nil, err
	}
	if params.Provider == nil {
		return nil, fmt.Errorf("%w: provider must not be nil", ErrInvalidArgument)
	}
	if params.Provider.TenantID() != params.TenantID {
		return nil, fmt.Errorf("%w: channel and provider belong to different tenants", ErrInvalidArgument)
	}
	if params.Provider.Status() != ConfigurationStatusActive {
		return nil, fmt.Errorf("%w: provider is disabled", ErrInvalidArgument)
	}
	if !params.Type.IsValid() {
		return nil, fmt.Errorf("%w: unsupported channel type %q", ErrInvalidArgument, params.Type)
	}
	if strings.TrimSpace(params.Key) == "" {
		return nil, fmt.Errorf("%w: channel key must not be empty", ErrInvalidArgument)
	}
	return &Channel{
		id:          uuid.NewV7(),
		tenantID:    params.TenantID,
		providerID:  params.Provider.ID(),
		channelType: params.Type,
		key:         params.Key,
		status:      ChannelStatusProvisioning,
	}, nil
}

func (c *Channel) Disable() error {
	if c.status == ChannelStatusDisabled {
		return nil
	}
	c.status = ChannelStatusDisabled
	return nil
}

func (c *Channel) Enable() error {
	if c.status == ChannelStatusActive {
		return nil
	}
	c.status = ChannelStatusProvisioning
	return nil
}

// Activate marks a successfully provisioned Channel ready to accept Campaigns.
func (c *Channel) Activate() error {
	if c.status == ChannelStatusActive {
		return nil
	}
	if c.status != ChannelStatusProvisioning {
		return fmt.Errorf("activate channel: %w", ErrInvalidTransition)
	}
	c.status = ChannelStatusActive
	return nil
}

func (c *Channel) ID() ChannelID          { return c.id }
func (c *Channel) TenantID() TenantID     { return c.tenantID }
func (c *Channel) ProviderID() ProviderID { return c.providerID }
func (c *Channel) Type() ChannelType      { return c.channelType }
func (c *Channel) Key() string            { return c.key }
func (c *Channel) Status() ChannelStatus  { return c.status }

type HydrateChannelParams struct {
	ID         ChannelID
	TenantID   TenantID
	ProviderID ProviderID
	Type       ChannelType
	Key        string
	Status     ChannelStatus
}

func HydrateChannel(params HydrateChannelParams) (*Channel, error) {
	if err := requireUUID("channel_id", params.ID); err != nil {
		return nil, err
	}
	if err := requireUUID("tenant_id", params.TenantID); err != nil {
		return nil, err
	}
	if err := requireUUID("provider_id", params.ProviderID); err != nil {
		return nil, err
	}
	if !params.Type.IsValid() {
		return nil, fmt.Errorf("%w: unsupported channel type %q", ErrInvalidArgument, params.Type)
	}
	if strings.TrimSpace(params.Key) == "" {
		return nil, fmt.Errorf("%w: channel key must not be empty", ErrInvalidArgument)
	}
	if !params.Status.isValid() {
		return nil, fmt.Errorf("%w: invalid channel status", ErrInvalidArgument)
	}
	return &Channel{
		id:          params.ID,
		tenantID:    params.TenantID,
		providerID:  params.ProviderID,
		channelType: params.Type,
		key:         params.Key,
		status:      params.Status,
	}, nil
}
