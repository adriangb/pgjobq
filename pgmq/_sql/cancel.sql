WITH queue_info AS (
    SELECT
        id,
        name,
        max_delivery_attempts,
        retention_period
    FROM pgmq.queues
    WHERE name = $1
)
DELETE FROM pgmq.messages
    WHERE (
        queue_id = (SELECT id FROM queue_info)
        AND
        id = any($2::uuid[])
    )
    RETURNING
        id,
        queue_id,
        body,
        (SELECT pg_notify('pgmq.message_completed_' || (SELECT name FROM queue_info), id::text)) AS notified
;