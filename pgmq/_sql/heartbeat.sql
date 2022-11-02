WITH queue_info AS (
    SELECT
        id,
        ack_deadline
    FROM pgmq.queues
    WHERE name = $1
), selected_messages AS (
    SELECT
        id
    FROM pgmq.messages
    WHERE (
        queue_id = (SELECT id FROM queue_info)
        AND
        id = any($2::uuid[])
        AND
        -- skip any messages that already expired
        -- this avoids race conditions between
        -- extending deadlines and nacking
        available_at > now()
    )
    ORDER BY id
    FOR UPDATE
)
UPDATE pgmq.messages
SET available_at = (
    now() + (
        SELECT ack_deadline
        FROM queue_info
    )
)
FROM selected_messages
WHERE (
    pgmq.messages.queue_id = (SELECT id FROM queue_info)
    AND
    pgmq.messages.id = selected_messages.id
)
RETURNING available_at AS next_ack_deadline;