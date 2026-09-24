-- name: CreateTenantAPIKey :exec
INSERT INTO tenant_api_keys (id, tenant_id, name, secret_hash)
VALUES ($1, $2, $3, $4);

-- name: FindActiveTenantAPIKeyByID :one
SELECT id, tenant_id, name, secret_hash
FROM tenant_api_keys
WHERE id = $1 AND revoked_at IS NULL;

-- name: RevokeTenantAPIKey :execrows
UPDATE tenant_api_keys
SET revoked_at = now()
WHERE id = $1 AND revoked_at IS NULL;
