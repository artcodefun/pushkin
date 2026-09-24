package dtos

type IssueTenantAPIKeyRequest struct {
	TenantID string `json:"tenant_id" binding:"required,uuid"`
	Name     string `json:"name" binding:"required"`
}

type IssueTenantAPIKeyResponse struct {
	APIKeyID string `json:"api_key_id"`
	APIKey   string `json:"api_key"`
}
