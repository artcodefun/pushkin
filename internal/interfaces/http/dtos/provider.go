package dtos

import "github.com/superman/pushkin/internal/application/queries/readmodels"

type CreateProviderRequest struct {
	Type           string `json:"type" binding:"required"`
	Credentials    string `json:"credentials" binding:"required"`
	RateLimitQPS   int    `json:"rate_limit_qps" binding:"required,gt=0"`
	RateLimitBurst int    `json:"rate_limit_burst" binding:"required,gt=0"`
}

type ProviderResponse struct {
	ProviderID     string `json:"provider_id"`
	Type           string `json:"type"`
	Status         string `json:"status"`
	RateLimitQPS   int    `json:"rate_limit_qps"`
	RateLimitBurst int    `json:"rate_limit_burst"`
}

type CreateProviderResponse struct {
	ProviderID string `json:"provider_id"`
}

func NewProviderResponse(provider *readmodels.Provider) ProviderResponse {
	return ProviderResponse{
		ProviderID:     provider.ID.String(),
		Type:           string(provider.Type),
		Status:         string(provider.Status),
		RateLimitQPS:   provider.RateLimitQPS,
		RateLimitBurst: provider.RateLimitBurst,
	}
}

type ListProvidersResponse struct {
	Providers []ProviderResponse `json:"providers"`
}

func NewListProvidersResponse(providers []readmodels.Provider) ListProvidersResponse {
	responses := make([]ProviderResponse, len(providers))
	for index := range providers {
		responses[index] = NewProviderResponse(&providers[index])
	}
	return ListProvidersResponse{Providers: responses}
}
