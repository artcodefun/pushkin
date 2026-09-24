package dtos

import "github.com/superman/pushkin/internal/application/queries/readmodels"

type CreateTenantRequest struct {
	Name               string `json:"name" binding:"required"`
	RateLimitPerMinute int    `json:"rate_limit_per_minute" binding:"required,gt=0"`
}

type CreateTenantResponse struct {
	TenantID string `json:"tenant_id"`
}

type TenantResponse struct {
	TenantID           string `json:"tenant_id"`
	Name               string `json:"name"`
	RateLimitPerMinute int    `json:"rate_limit_per_minute"`
	Status             string `json:"status"`
}

func NewTenantResponse(tenant *readmodels.Tenant) TenantResponse {
	return TenantResponse{
		TenantID:           tenant.ID.String(),
		Name:               tenant.Name,
		RateLimitPerMinute: tenant.RateLimitPerMinute,
		Status:             string(tenant.Status),
	}
}

type ListTenantsResponse struct {
	Tenants []TenantResponse `json:"tenants"`
}

func NewListTenantsResponse(tenants []readmodels.Tenant) ListTenantsResponse {
	responses := make([]TenantResponse, len(tenants))
	for index := range tenants {
		responses[index] = NewTenantResponse(&tenants[index])
	}
	return ListTenantsResponse{Tenants: responses}
}
