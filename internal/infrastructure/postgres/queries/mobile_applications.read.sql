-- name: GetMobileApplicationReadModelByID :one
SELECT id, tenant_id, provider_id, platform, package_name, status
FROM mobile_applications
WHERE tenant_id = $1
  AND id = $2;

-- name: ListMobileApplicationReadModels :many
SELECT id, tenant_id, provider_id, platform, package_name, status
FROM mobile_applications
WHERE tenant_id = $1
ORDER BY platform, package_name, id;
