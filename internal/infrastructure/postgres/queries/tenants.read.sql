-- name: ListTenantReadModels :many
SELECT id, name, rate_limit_per_minute, status
FROM tenants
ORDER BY name, id;
