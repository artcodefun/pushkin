-- name: GetProviderReadModelByID :one
SELECT id, tenant_id, provider_type, status, rate_limit_qps, rate_limit_burst
FROM providers
WHERE tenant_id = $1
  AND id = $2;

-- name: ListProviderReadModels :many
SELECT id, tenant_id, provider_type, status, rate_limit_qps, rate_limit_burst
FROM providers
WHERE tenant_id = $1
ORDER BY provider_type, id;
