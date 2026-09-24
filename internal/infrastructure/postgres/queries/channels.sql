-- name: CreateChannel :exec
INSERT INTO channels (
    id,
    tenant_id,
    provider_id,
    channel_type,
    channel_key,
    status
) VALUES (
    $1, $2, $3, $4, $5, $6
);

-- name: GetChannelByID :one
SELECT *
FROM channels
WHERE id = $1;

-- name: GetChannelByTenantIDAndKey :one
SELECT *
FROM channels
WHERE tenant_id = $1
  AND channel_key = $2;

-- name: ListActiveChannelIDs :many
SELECT channels.id
FROM channels
JOIN tenants ON tenants.id = channels.tenant_id
JOIN providers ON providers.id = channels.provider_id
WHERE channels.status = 'active'
  AND tenants.status = 'active'
  AND providers.status = 'active'
ORDER BY channels.id;

-- name: GetProvisioningChannelForUpdate :one
SELECT *
FROM channels
WHERE status = 'provisioning'
ORDER BY id
FOR UPDATE SKIP LOCKED
LIMIT 1;

-- name: UpdateChannel :exec
UPDATE channels
SET tenant_id = $2,
    provider_id = $3,
    channel_type = $4,
    channel_key = $5,
    status = $6
WHERE id = $1;

-- name: LinkChannelMobileApplication :exec
INSERT INTO channel_mobile_applications (
    channel_id,
    mobile_application_id
) VALUES (
    $1, $2
)
ON CONFLICT DO NOTHING;

-- name: UnlinkChannelMobileApplication :exec
DELETE FROM channel_mobile_applications
WHERE channel_id = $1
  AND mobile_application_id = $2;
