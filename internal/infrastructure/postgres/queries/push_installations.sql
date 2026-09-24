-- name: UpsertPushInstallation :exec
INSERT INTO push_installations (
    id,
    tenant_id,
    user_id,
    mobile_application_id,
    installation_id,
    token,
    status
) VALUES (
    $1, $2, $3, $4, $5, $6, $7
)
ON CONFLICT (tenant_id, mobile_application_id, installation_id) DO UPDATE
SET user_id = EXCLUDED.user_id,
    token = EXCLUDED.token,
    status = EXCLUDED.status,
    updated_at = now();

-- name: DeactivatePushInstallation :exec
UPDATE push_installations
SET status = 'inactive',
    updated_at = now()
WHERE tenant_id = $1
  AND id = $2;

-- name: ListActivePushInstallationTokensByIDs :many
SELECT id, token
FROM push_installations
WHERE tenant_id = $1
  AND id = ANY(sqlc.arg(ids)::uuid[])
  AND status = 'active';

-- name: ListActivePushInstallationIDs :many
SELECT id
FROM push_installations
WHERE tenant_id = $1
  AND user_id = ANY(sqlc.arg(user_ids)::text[])
  AND mobile_application_id = ANY(sqlc.arg(mobile_application_ids)::uuid[])
  AND status = 'active'
ORDER BY id;

-- name: ListActivePushInstallationIDsByUsers :many
SELECT id, user_id
FROM push_installations
WHERE tenant_id = $1
  AND user_id = ANY(sqlc.arg(user_ids)::text[])
  AND mobile_application_id = ANY(sqlc.arg(mobile_application_ids)::uuid[])
  AND status = 'active'
ORDER BY user_id, id;
