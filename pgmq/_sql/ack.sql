WITH queue_info AS (
    SELECT
        name,
        id
    FROM pgmq.queues
    WHERE name = $1
)
DELETE
FROM pgmq.messages
WHERE (
    queue_id = (SELECT id FROM queue_info)
    AND
    pgmq.messages.id = $2
)
RETURNING
    pg_notify('pgmq.message_completed_' || (SELECT name FROM queue_info), (SELECT id)::text) AS notified;
