-- name: CreateProvider :exec
INSERT INTO providers (
    id,
    tenant_id,
    provider_type,
    encrypted_credentials,
    rate_limit_qps,
    rate_limit_burst,
    status
) VALUES (
    $1, $2, $3, $4, $5, $6, $7
);

-- name: GetProviderByID :one
SELECT *
FROM providers
WHERE id = $1;
