-- name: CreateTenant :exec
INSERT INTO tenants (
    id,
    name,
    rate_limit_per_minute,
    status
) VALUES (
    $1, $2, $3, $4
);

-- name: FindTenantByID :one
SELECT id, name, rate_limit_per_minute, status
FROM tenants
WHERE id = $1;
