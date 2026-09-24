-- name: SaveUser :exec
INSERT INTO users (
    tenant_id,
    user_id,
    attributes,
    status
) VALUES (
    $1, $2, $3, $4
)
ON CONFLICT (tenant_id, user_id) DO UPDATE
SET attributes = EXCLUDED.attributes,
    status = EXCLUDED.status,
    updated_at = now();
