package domain

import (
	"fmt"
	"strings"
	"uuid"
)

type NewTenantParams struct {
	Name               string
	RateLimitPerMinute int
}

type Tenant struct {
	id                 TenantID
	name               string
	rateLimitPerMinute int
	status             ConfigurationStatus
}

func NewTenant(params NewTenantParams) (*Tenant, error) {
	if strings.TrimSpace(params.Name) == "" {
		return nil, fmt.Errorf("%w: tenant name must not be empty", ErrInvalidArgument)
	}
	if params.RateLimitPerMinute <= 0 {
		return nil, fmt.Errorf("%w: tenant rate limit must be positive", ErrInvalidArgument)
	}
	return &Tenant{
		id:                 uuid.NewV7(),
		name:               params.Name,
		rateLimitPerMinute: params.RateLimitPerMinute,
		status:             ConfigurationStatusActive,
	}, nil
}

func (t *Tenant) ChangeRateLimit(limitPerMinute int) error {
	if limitPerMinute <= 0 {
		return fmt.Errorf("%w: tenant rate limit must be positive", ErrInvalidArgument)
	}
	t.rateLimitPerMinute = limitPerMinute
	return nil
}

func (t *Tenant) Disable() error {
	if t.status == ConfigurationStatusDisabled {
		return nil
	}
	t.status = ConfigurationStatusDisabled
	return nil
}

func (t *Tenant) Enable() error {
	if t.status == ConfigurationStatusActive {
		return nil
	}
	t.status = ConfigurationStatusActive
	return nil
}

func (t *Tenant) ID() TenantID                { return t.id }
func (t *Tenant) Name() string                { return t.name }
func (t *Tenant) RateLimitPerMinute() int     { return t.rateLimitPerMinute }
func (t *Tenant) Status() ConfigurationStatus { return t.status }

type HydrateTenantParams struct {
	ID                 TenantID
	Name               string
	RateLimitPerMinute int
	Status             ConfigurationStatus
}

func HydrateTenant(params HydrateTenantParams) (*Tenant, error) {
	if err := requireUUID("tenant_id", params.ID); err != nil {
		return nil, err
	}
	if strings.TrimSpace(params.Name) == "" {
		return nil, fmt.Errorf("%w: tenant name must not be empty", ErrInvalidArgument)
	}
	if params.RateLimitPerMinute <= 0 {
		return nil, fmt.Errorf("%w: tenant rate limit must be positive", ErrInvalidArgument)
	}
	if params.Status != ConfigurationStatusActive && params.Status != ConfigurationStatusDisabled {
		return nil, fmt.Errorf("%w: invalid tenant status %q", ErrInvalidArgument, params.Status)
	}
	return &Tenant{
		id:                 params.ID,
		name:               params.Name,
		rateLimitPerMinute: params.RateLimitPerMinute,
		status:             params.Status,
	}, nil
}
