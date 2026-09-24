-- name: CreateSourceBatch :exec
INSERT INTO campaign_recipient_batches (
    id,
    campaign_id,
    user_ids,
    recipient_count
) VALUES (
    $1, $2, $3, $4
);

-- name: GetSourceBatchByID :one
SELECT *
FROM campaign_recipient_batches
WHERE id = $1;

-- name: ListSourceBatchIDsByCampaign :many
SELECT id
FROM campaign_recipient_batches
WHERE campaign_id = $1
ORDER BY id;
