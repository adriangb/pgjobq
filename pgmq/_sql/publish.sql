WITH queue_info AS (
    SELECT
        id AS queue_id,
        max_delivery_attempts,
        retention_period
    FROM pgmq.queues
    WHERE name = $1
)
INSERT INTO pgmq.messages(
    queue_id,
    id,
    expires_at,
    delivery_attempts_remaining,
    available_at,
    body
)
SELECT
    queue_id,
    unnest($3::uuid[]),
    COALESCE($2, now()::timestamp) + retention_period,
    max_delivery_attempts,
    COALESCE($2, now()::timestamp),
    unnest($4::bytea[])
FROM queue_info
RETURNING queue_id;