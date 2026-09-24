-- name: GetCampaignReadModelByID :one
SELECT id,
       tenant_id,
       channel_id,
       priority,
       status,
       title,
       body,
       image_url,
       data,
       scheduled_at,
       started_at,
       delivery_total,
       delivery_processed,
       delivery_accepted_count,
       delivery_failed_count
FROM campaigns
WHERE tenant_id = $1
  AND id = $2;
