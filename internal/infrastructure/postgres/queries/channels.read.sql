-- name: GetChannelReadModelByID :one
SELECT id, tenant_id, provider_id, channel_type, channel_key, status
FROM channels
WHERE tenant_id = $1
  AND id = $2;

-- name: ListChannelReadModels :many
SELECT id, tenant_id, provider_id, channel_type, channel_key, status
FROM channels
WHERE tenant_id = $1
ORDER BY channel_key, id;
