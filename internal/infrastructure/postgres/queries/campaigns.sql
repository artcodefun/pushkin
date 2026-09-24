-- name: CreateCampaign :exec
INSERT INTO campaigns (
    id,
    tenant_id,
    channel_id,
    title,
    body,
    image_url,
    data,
    priority,
    recipient_mode,
    inline_recipients,
    scheduled_at,
    status,
    run_id,
    run_attempted_at
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14
);

-- name: GetCampaignByID :one
SELECT *
FROM campaigns
WHERE id = $1;

-- name: GetCampaignByIDs :many
SELECT *
FROM campaigns
WHERE id = ANY(sqlc.arg(ids)::uuid[]);

-- name: GetCampaignByIDsForUpdate :many
SELECT *
FROM campaigns
WHERE id = ANY(sqlc.arg(ids)::uuid[])
ORDER BY id
FOR UPDATE;

-- name: GetCampaignByIDForUpdate :one
SELECT *
FROM campaigns
WHERE id = $1
FOR UPDATE;

-- name: ListStartCandidatesForUpdate :many
SELECT *
FROM campaigns
WHERE (status = 'scheduled' AND scheduled_at <= sqlc.arg(due_before))
   OR (
       status = 'starting'
       AND (run_attempted_at IS NULL OR run_attempted_at <= sqlc.arg(run_attempted_before))
   )
ORDER BY scheduled_at NULLS FIRST, run_attempted_at NULLS FIRST, id
FOR UPDATE SKIP LOCKED
LIMIT sqlc.arg(candidate_limit);

-- name: UpdateCampaign :exec
UPDATE campaigns
SET status = $2,
    scheduled_at = $3,
    run_id = $4,
    run_attempted_at = $5,
    started_at = $6,
    completed_at = $7,
    failed_at = $8,
    failure_reason = $9,
    source_batches_total = $10,
    source_batches_fanned_out = $11,
    delivery_total = $12,
    delivery_processed = $13,
    delivery_accepted_count = $14,
    delivery_failed_count = $15,
    updated_at = now()
WHERE id = $1;

-- name: UpdateCampaignBatch :exec
UPDATE campaigns AS campaign
SET status = updates.status,
    scheduled_at = updates.scheduled_at,
    run_id = updates.run_id,
    run_attempted_at = updates.run_attempted_at,
    started_at = updates.started_at,
    completed_at = updates.completed_at,
    failed_at = updates.failed_at,
    failure_reason = updates.failure_reason,
    source_batches_total = updates.source_batches_total,
    source_batches_fanned_out = updates.source_batches_fanned_out,
    delivery_total = updates.delivery_total,
    delivery_processed = updates.delivery_processed,
    delivery_accepted_count = updates.delivery_accepted_count,
    delivery_failed_count = updates.delivery_failed_count,
    updated_at = now()
FROM jsonb_to_recordset(sqlc.arg(updates)::jsonb) AS updates(
    id uuid,
    status text,
    scheduled_at timestamptz,
    run_id uuid,
    run_attempted_at timestamptz,
    started_at timestamptz,
    completed_at timestamptz,
    failed_at timestamptz,
    failure_reason text,
    source_batches_total bigint,
    source_batches_fanned_out bigint,
    delivery_total bigint,
    delivery_processed bigint,
    delivery_accepted_count bigint,
    delivery_failed_count bigint
)
WHERE campaign.id = updates.id;
