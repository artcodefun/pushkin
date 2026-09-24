-- name: CreateMobileApplication :exec
INSERT INTO mobile_applications (
    id,
    tenant_id,
    provider_id,
    platform,
    package_name,
    status
) VALUES (
    $1, $2, $3, $4, $5, $6
);

-- name: GetMobileApplicationByID :one
SELECT *
FROM mobile_applications
WHERE id = $1;

-- name: GetMobileApplicationByPlatformAndPackageName :one
SELECT *
FROM mobile_applications
WHERE tenant_id = $1
  AND platform = $2
  AND package_name = $3;

-- name: UpdateMobileApplication :exec
UPDATE mobile_applications
SET provider_id = $2,
    status = $3,
    updated_at = now()
WHERE id = $1;

-- name: ListMobileApplicationIDsByChannel :many
SELECT mobile_application_id
FROM channel_mobile_applications
WHERE channel_id = $1
ORDER BY mobile_application_id;

-- name: ListMobileApplicationIDsByChannels :many
SELECT channel_id, mobile_application_id
FROM channel_mobile_applications
WHERE channel_id = ANY(sqlc.arg(channel_ids)::uuid[])
ORDER BY channel_id, mobile_application_id;

-- name: ListChannelIDsByMobileApplication :many
SELECT channel_id
FROM channel_mobile_applications
WHERE mobile_application_id = $1
ORDER BY channel_id;
