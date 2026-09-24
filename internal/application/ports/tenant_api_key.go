package ports

import (
	"context"

	"github.com/superman/pushkin/internal/domain"
)

// TenantAPIKeyHasher derives the durable secret hash from a high-entropy API
// key secret. Its implementation owns the configured server-side pepper.
type TenantAPIKeyHasher interface {
	Hash(ctx context.Context, secret []byte) ([]byte, error)
}

// TenantAPIKeyValidator validates a presented API key and returns its tenant.
// It is consumed by the HTTP authentication middleware.
type TenantAPIKeyValidator interface {
	Validate(ctx context.Context, rawKey string) (tenantID domain.TenantID, err error)
}
