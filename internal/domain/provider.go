package domain

import (
	"fmt"
	"strings"
	"uuid"
)

type ProviderType string

const ProviderTypeFCM ProviderType = "fcm"

func (t ProviderType) IsValid() bool {
	return t == ProviderTypeFCM
}

type NewProviderParams struct {
	TenantID             TenantID
	Type                 ProviderType
	EncryptedCredentials string
	RateLimitQPS         int
	RateLimitBurst       int
}

type Provider struct {
	id                   ProviderID
	tenantID             TenantID
	providerType         ProviderType
	encryptedCredentials string
	rateLimitQPS         int
	rateLimitBurst       int
	status               ConfigurationStatus
}

func NewProvider(params NewProviderParams) (*Provider, error) {
	if err := requireUUID("tenant_id", params.TenantID); err != nil {
		return nil, err
	}
	if !params.Type.IsValid() {
		return nil, fmt.Errorf("%w: unsupported provider type %q", ErrInvalidArgument, params.Type)
	}
	if strings.TrimSpace(params.EncryptedCredentials) == "" {
		return nil, fmt.Errorf("%w: encrypted credentials must not be empty", ErrInvalidArgument)
	}
	if params.RateLimitQPS <= 0 {
		return nil, fmt.Errorf("%w: rate_limit_qps must be positive", ErrInvalidArgument)
	}
	if params.RateLimitBurst <= 0 {
		return nil, fmt.Errorf("%w: rate_limit_burst must be positive", ErrInvalidArgument)
	}
	return &Provider{
		id:                   uuid.NewV7(),
		tenantID:             params.TenantID,
		providerType:         params.Type,
		encryptedCredentials: params.EncryptedCredentials,
		rateLimitQPS:         params.RateLimitQPS,
		rateLimitBurst:       params.RateLimitBurst,
		status:               ConfigurationStatusActive,
	}, nil
}

func (p *Provider) ChangeRateLimit(qps, burst int) error {
	if qps <= 0 || burst <= 0 {
		return fmt.Errorf("%w: provider rate limit values must be positive", ErrInvalidArgument)
	}
	p.rateLimitQPS = qps
	p.rateLimitBurst = burst
	return nil
}

func (p *Provider) ChangeEncryptedCredentials(encryptedCredentials string) error {
	if strings.TrimSpace(encryptedCredentials) == "" {
		return fmt.Errorf("%w: encrypted credentials must not be empty", ErrInvalidArgument)
	}
	p.encryptedCredentials = encryptedCredentials
	return nil
}

func (p *Provider) Disable() error {
	if p.status == ConfigurationStatusDisabled {
		return nil
	}
	p.status = ConfigurationStatusDisabled
	return nil
}

func (p *Provider) Enable() error {
	if p.status == ConfigurationStatusActive {
		return nil
	}
	p.status = ConfigurationStatusActive
	return nil
}

func (p *Provider) ID() ProviderID               { return p.id }
func (p *Provider) TenantID() TenantID           { return p.tenantID }
func (p *Provider) Type() ProviderType           { return p.providerType }
func (p *Provider) EncryptedCredentials() string { return p.encryptedCredentials }
func (p *Provider) RateLimitQPS() int            { return p.rateLimitQPS }
func (p *Provider) RateLimitBurst() int          { return p.rateLimitBurst }
func (p *Provider) Status() ConfigurationStatus  { return p.status }

type HydrateProviderParams struct {
	ID                   ProviderID
	TenantID             TenantID
	Type                 ProviderType
	EncryptedCredentials string
	RateLimitQPS         int
	RateLimitBurst       int
	Status               ConfigurationStatus
}

func HydrateProvider(params HydrateProviderParams) (*Provider, error) {
	if err := requireUUID("provider_id", params.ID); err != nil {
		return nil, err
	}
	if err := requireUUID("tenant_id", params.TenantID); err != nil {
		return nil, err
	}
	if !params.Type.IsValid() {
		return nil, fmt.Errorf("%w: unsupported provider type %q", ErrInvalidArgument, params.Type)
	}
	if strings.TrimSpace(params.EncryptedCredentials) == "" {
		return nil, fmt.Errorf("%w: encrypted credentials must not be empty", ErrInvalidArgument)
	}
	if params.RateLimitQPS <= 0 || params.RateLimitBurst <= 0 {
		return nil, fmt.Errorf("%w: provider rate limit values must be positive", ErrInvalidArgument)
	}
	if !params.Status.isValid() {
		return nil, fmt.Errorf("%w: invalid provider status", ErrInvalidArgument)
	}
	return &Provider{
		id:                   params.ID,
		tenantID:             params.TenantID,
		providerType:         params.Type,
		encryptedCredentials: params.EncryptedCredentials,
		rateLimitQPS:         params.RateLimitQPS,
		rateLimitBurst:       params.RateLimitBurst,
		status:               params.Status,
	}, nil
}
